#!/usr/bin/env python3
"""Read from Snowflake, write to MySQL / SQL Server. On-prem, batch, Airflow-friendly.

The job runs ON-PREM and makes only OUTBOUND connections: out to Snowflake (and,
for the unload transport, to the stage storage) to read, and to the local RDBMS
to write. Nothing has to reach INTO the on-prem data center, which is the
connectivity constraint that matters on locked-down enterprise networks.

Each run combines a change-capture mode (WHICH rows; change_capture.py) with a
transport (HOW they move; transports.py):

  --change-capture none | hwm | stream
  --transport      pull   (Transport A: connector reads, batched DML)
                   unload (Transport B: COPY INTO stage, GET, native bulk load)

Write modes (--mode):
  truncate  empty the target, then load (full refresh)
  upsert    MERGE / ON DUPLICATE KEY on --key-cols
  append    plain inserts

Every run is one target transaction (the pull transport may commit every
--commit-rows rows for none/hwm). Watermark or outbox state advances only after
the target commit, so a failed run is retried from the same point.

Examples
--------
  # Full refresh of a small dimension
  python sync.py --source ANALYTICS.DENTAL.PROVIDERS --target PROVIDERS \
      --change-capture none --mode truncate

  # Timestamp-delta upsert
  python sync.py --source ANALYTICS.DENTAL.CLAIMS --target CLAIMS \
      --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID

  # Same, via unload and bulk load
  python sync.py --transport unload --source ANALYTICS.DENTAL.CLAIMS --target CLAIMS \
      --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID \
      --stage @EXPORT_STAGE --local-dir /var/lib/simple-reverse-etl/unload

  # Stream-based CDC (no reliable modified-at column; captures deletes).
  # One-time setup: sql/01_snowflake_setup.sql
  python sync.py --source ANALYTICS.DENTAL.CLAIMS --target CLAIMS \
      --change-capture stream --stream ANALYTICS.DENTAL.CLAIMS_STREAM \
      --outbox ANALYTICS.DENTAL.CLAIMS_OUTBOX --mode upsert --key-cols CLAIM_ID
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import snowflake_source as sf
from change_capture import PLANNERS
from config import SnowflakeConfig, TargetConfig
from targets import make_target
from transports import TRANSPORTS

log = logging.getLogger("sync")

DEFAULT_COMMIT_ROWS = 100_000
DEFAULT_STAGE = "@~/simple_reverse_etl"
DEFAULT_LOCAL_DIR = "_unload_tmp"


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Snowflake -> MySQL / SQL Server sync")
    p.add_argument("--source", required=True,
                   help="Fully-qualified Snowflake source (DB.SCHEMA.OBJECT)")
    p.add_argument("--target", required=True,
                   help="Target table name in the RDBMS")
    p.add_argument("--transport", choices=tuple(TRANSPORTS), default="pull",
                   help="pull: connector reads + batched DML (default); "
                        "unload: COPY INTO stage + GET + native bulk load")
    p.add_argument("--change-capture", choices=tuple(PLANNERS), default="none")
    p.add_argument("--mode", choices=("truncate", "upsert", "append"),
                   default="truncate")
    p.add_argument("--key-cols", nargs="*", default=None,
                   help="Primary key column(s); required for upsert and stream")
    p.add_argument("--lower-cols", action="store_true",
                   help="Lowercase Snowflake column names before writing")

    g = p.add_argument_group("hwm change capture")
    g.add_argument("--hwm-col", help="Monotonic column, e.g. UPDATED_AT")
    g.add_argument("--hwm-start", default=None,
                   help="Initial watermark when no state is saved yet "
                        "(default: load all rows)")
    g.add_argument("--state-file", default="sync_state.json",
                   help="JSON file holding the watermark per source and target "
                        "(default: ./sync_state.json)")

    g = p.add_argument_group("stream change capture")
    g.add_argument("--stream", help="Snowflake stream on the source table")
    g.add_argument("--outbox", help="Outbox table (see sql/01_snowflake_setup.sql)")
    g.add_argument("--outbox-retention-days", type=int, default=0,
                   help="Keep delivered outbox rows this many days "
                        "(default 0: delete them after each successful run)")

    g = p.add_argument_group("pull transport")
    g.add_argument("--commit-rows", type=int, default=None,
                   help=f"Approx rows per target commit for none/hwm "
                        f"(default {DEFAULT_COMMIT_ROWS:,})")

    g = p.add_argument_group("unload transport")
    g.add_argument("--stage", default=None,
                   help=f"Stage prefix to unload into (default {DEFAULT_STAGE})")
    g.add_argument("--local-dir", default=None,
                   help=f"Local folder for downloaded files (default {DEFAULT_LOCAL_DIR}); "
                        "for SQL Server BULK INSERT, a folder SQL Server can also read")
    return p.parse_args(argv)


def validate(args) -> str | None:
    """Returns an error message, or None. Also fills transport defaults."""
    if args.mode == "upsert" and not args.key_cols:
        return "--mode upsert requires --key-cols"
    if args.change_capture == "hwm" and not args.hwm_col:
        return "--change-capture hwm requires --hwm-col"
    if args.change_capture == "stream":
        if not (args.stream and args.outbox):
            return "--change-capture stream requires --stream and --outbox"
        if args.mode != "upsert" or not args.key_cols:
            return "--change-capture stream requires --mode upsert and --key-cols"
    if args.outbox_retention_days < 0:
        return "--outbox-retention-days must be 0 or more"
    if args.transport == "pull":
        if args.stage is not None or args.local_dir is not None:
            return "--stage and --local-dir apply only to --transport unload"
        args.commit_rows = args.commit_rows or DEFAULT_COMMIT_ROWS
    else:
        if args.commit_rows is not None:
            return "--commit-rows applies only to --transport pull"
        args.stage = args.stage or DEFAULT_STAGE
        args.local_dir = args.local_dir or DEFAULT_LOCAL_DIR
    return None


def main(argv=None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    error = validate(args)
    if error:
        log.error(error)
        return 2

    sf_conn = sf.connect(SnowflakeConfig.from_env())
    target = make_target(TargetConfig.from_env())
    try:
        # 1. Change capture decides which rows to send (None: nothing to do).
        plan = PLANNERS[args.change_capture](sf_conn, args)
        if plan is None:
            return 0
        # 2. The transport moves them into the target.
        written, deleted = TRANSPORTS[args.transport](sf_conn, target, plan, args)
        # 3. Commit the target, then advance watermark / acknowledge the outbox.
        target.commit()
    except Exception:
        target.rollback()
        log.exception("Sync failed; target transaction rolled back")
        target.close()
        sf_conn.close()
        return 1
    try:
        plan.on_commit()
        log.info(plan.summary(written, deleted))
        return 0
    except Exception:
        # The target is committed; only the state update failed. The next run
        # re-sends the same rows, which the key-based upsert makes harmless.
        log.exception("Target committed, but saving watermark/outbox state failed")
        return 1
    finally:
        target.close()
        sf_conn.close()


if __name__ == "__main__":
    sys.exit(main())
