#!/usr/bin/env python3
"""Read from Snowflake, write to MySQL / SQL Server. On-prem, batch, Airflow-friendly.

The job runs ON-PREM and makes only OUTBOUND connections: out to Snowflake
to read, and to the local RDBMS to write. Nothing has to reach INTO the on-prem
data center, which is the connectivity constraint that matters on locked-down
enterprise networks.

Change-capture modes:
  none    full table each run           (pair with --mode truncate)
  hwm     rows where <hwm-col> advanced  (needs a monotonic column; state file)
  stream  Snowflake stream -> outbox     (no source column needed; see sql/)

Write modes:
  truncate  wipe target then load        (full refresh)
  upsert    MERGE / ON DUPLICATE KEY      (needs --key-cols)

High-water-mark (hwm) state
---------------------------
The watermark is the largest <hwm-col> value already delivered to the target.
It is kept in a small JSON file on the job host (--state-file, default
./sync_state.json), keyed by source table so one file can track many tables:

  {"ANALYTICS.DENTAL.CLAIMS": {"watermark": "2026-10-08 15:09:23.237000"}}

Each run: (1) read the saved watermark, falling back to --hwm-start, or to
"no lower bound" if neither exists; (2) read MAX(<hwm-col>) from Snowflake as
the ceiling; (3) load rows with watermark < <hwm-col> <= ceiling; (4) write the
ceiling back to the file only after the target commit succeeds. A failed run
therefore leaves the watermark unchanged and the next run retries the same
window. Large loads commit every --commit-rows rows, so a retry can re-send rows
that were already committed; upsert mode makes that harmless.

The file is local to one host. In production, keep it on durable storage, or
replace _load_state/_save_state with a control table or scheduler variable.
Deleting the file (or changing --state-file) causes a reload from --hwm-start.

Examples
--------
  # Full refresh of a small dimension into MySQL
  python sync.py --source ANALYTICS.DENTAL.PROVIDERS --target PROVIDERS \
      --change-capture none --mode truncate

  # Timestamp-delta upsert into SQL Server
  python sync.py --source ANALYTICS.DENTAL.CLAIMS --target CLAIMS \
      --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID

  # Stream-based CDC (no reliable modified-at column); see sql/01_snowflake_setup.sql
  python sync.py --source ANALYTICS.DENTAL.CLAIMS --target CLAIMS \
      --change-capture stream --stream ANALYTICS.DENTAL.CLAIMS_STREAM \
      --outbox ANALYTICS.DENTAL.CLAIMS_OUTBOX --mode upsert --key-cols CLAIM_ID
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd

import snowflake_source as sf
from config import SnowflakeConfig, TargetConfig
from targets import make_target

log = logging.getLogger("sync")

# CDC metadata columns added by the stream/outbox path; not written to the target.
CDC_META = ("_CDC_ACTION", "_CDC_ISUPDATE", "_CDC_LOADED_AT", "_CDC_EXPORTED")


# --- helpers ----------------------------------------------------------------

def _df_to_rows(df: pd.DataFrame):
    """DataFrame -> list of tuples with pandas NaN/NaT coerced to None."""
    obj = df.astype(object).where(pd.notnull(df), None)
    return list(obj.itertuples(index=False, name=None))


def _load_state(path: str) -> dict:
    """Watermark state: {source_table: {"watermark": value}}; {} if no file yet."""
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else {}


def _save_state(path: str, state: dict) -> None:
    """Persist watermark state. Call only after the target commit succeeds."""
    Path(path).write_text(json.dumps(state, indent=2, default=str))


def _write_frames(target, frames, table, columns, mode, key_columns, commit_rows):
    """Stream frames to the target, committing about every `commit_rows` rows."""
    pending = 0
    total = 0
    for df in frames:
        cols = [c for c in df.columns if c not in CDC_META]
        rows = _df_to_rows(df[cols])
        target.write(table, cols, rows, mode=mode, key_columns=key_columns)
        pending += len(rows)
        total += len(rows)
        if pending >= commit_rows:
            target.commit()
            log.info("  committed (%s rows so far)", total)
            pending = 0
    if pending:
        target.commit()
    return total


# --- change-capture strategies ----------------------------------------------

def run_full(sf_conn, target, args):
    if args.mode == "truncate":
        target.truncate(args.target)
    frames = sf.read_query_batches(sf_conn, sf.build_full_query(args.source))
    n = _write_frames(target, frames, args.target, None,
                      args.mode, args.key_cols, args.commit_rows)
    log.info("Full load complete: %s rows -> %s", n, args.target)


def run_hwm(sf_conn, target, args):
    """Incremental load of rows whose <hwm-col> advanced since the last run.

    See "High-water-mark (hwm) state" in the module docstring.
    """
    # 1. Last delivered value: saved state, else --hwm-start, else None (load all).
    state = _load_state(args.state_file)
    last = state.get(args.source, {}).get("watermark", args.hwm_start)
    # 2. Ceiling captured before reading, so rows updated mid-read wait for next run.
    ceiling = sf.scalar(sf_conn, f"SELECT MAX({args.hwm_col}) FROM {args.source}")
    if ceiling is None or (last is not None and str(ceiling) <= str(last)):
        log.info("No new rows above watermark %r (ceiling %r).", last, ceiling)
        return
    log.info("HWM window: %r < %s <= %r", last, args.hwm_col, ceiling)
    # 3. Read the window and write it to the target (_write_frames commits).
    frames = sf.read_query_batches(
        sf_conn,
        sf.build_hwm_query(args.source, args.hwm_col, has_watermark=last is not None),
        params={"watermark": last, "ceiling": ceiling},
    )
    n = _write_frames(target, frames, args.target, None,
                      args.mode, args.key_cols, args.commit_rows)
    # 4. Advance the watermark only now that the target has committed.
    state.setdefault(args.source, {})["watermark"] = ceiling
    _save_state(args.state_file, state)
    log.info("HWM load complete: %s rows, watermark advanced to %r", n, ceiling)


def run_stream(sf_conn, target, args):
    """Stream change capture: stream -> outbox in Snowflake -> target.

    See "Stream-based change capture" in README.md for the full design.
    """
    if not args.key_cols:
        raise SystemExit("--change-capture stream requires --key-cols")

    # 1. Consume the stream into the outbox (this commit advances the offset).
    sf.consume_stream_to_outbox(sf_conn, args.stream, args.outbox)

    # 2. Snapshot: handle only outbox rows consumed up to now.
    cutoff = sf.outbox_cutoff(sf_conn, args.outbox)
    if cutoff is None:
        log.info("Stream CDC: no un-exported changes in %s", args.outbox)
        return

    # 3. Read the net change per key (reduced in Snowflake; see
    #    build_outbox_changes_query) and apply it in one target transaction.
    #    Keys are unique across the result, so batches can be applied in turn.
    frames = sf.read_query_batches(
        sf_conn, sf.build_outbox_changes_query(args.outbox, args.key_cols),
        params={"cutoff": cutoff})
    total = deletes = 0
    for df in frames:
        data_cols = [c for c in df.columns if c not in CDC_META]
        is_del = df["_CDC_ACTION"] == "DELETE"
        upserts = df.loc[~is_del, data_cols]
        removes = df.loc[is_del, list(args.key_cols)]
        if not upserts.empty:
            target.write(args.target, data_cols, _df_to_rows(upserts),
                         mode="upsert", key_columns=args.key_cols)
            total += len(upserts)
        if not removes.empty:
            target.delete(args.target, args.key_cols, _df_to_rows(removes))
            deletes += len(removes)
    target.commit()

    # 4. Only now is the target write durable -> acknowledge up to the cutoff.
    sf.mark_outbox_exported(sf_conn, args.outbox, cutoff)
    log.info("Stream CDC complete: %s upserts, %s deletes -> %s",
             total, deletes, args.target)


# --- entrypoint --------------------------------------------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Snowflake -> MySQL/SQL Server sync")
    p.add_argument("--source", required=True,
                   help="Fully-qualified Snowflake source (DB.SCHEMA.OBJECT)")
    p.add_argument("--target", required=True,
                   help="Target table name in the RDBMS")
    p.add_argument("--change-capture", choices=("none", "hwm", "stream"),
                   default="none")
    p.add_argument("--mode", choices=("truncate", "upsert", "append"),
                   default="truncate")
    p.add_argument("--key-cols", nargs="*", default=None,
                   help="Primary key column(s); required for upsert / deletes")
    p.add_argument("--hwm-col", help="Monotonic column for --change-capture hwm")
    p.add_argument("--hwm-start", default=None,
                   help="Initial watermark when the state file has none")
    p.add_argument("--state-file", default="sync_state.json",
                   help="JSON file holding the hwm watermark per source table "
                        "(default: ./sync_state.json)")
    p.add_argument("--stream", help="Stream name for --change-capture stream")
    p.add_argument("--outbox", help="Outbox table for --change-capture stream")
    p.add_argument("--commit-rows", type=int, default=100_000,
                   help="Approx rows per target commit")
    return p.parse_args(argv)


def main(argv=None) -> int:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)

    if args.mode == "upsert" and not args.key_cols:
        log.error("--mode upsert requires --key-cols")
        return 2
    if args.change_capture == "hwm" and not args.hwm_col:
        log.error("--change-capture hwm requires --hwm-col")
        return 2
    if args.change_capture == "stream" and not (args.stream and args.outbox):
        log.error("--change-capture stream requires --stream and --outbox")
        return 2

    sf_conn = sf.connect(SnowflakeConfig.from_env())
    target = make_target(TargetConfig.from_env())
    try:
        if args.change_capture == "none":
            run_full(sf_conn, target, args)
        elif args.change_capture == "hwm":
            run_hwm(sf_conn, target, args)
        else:
            run_stream(sf_conn, target, args)
        return 0
    except Exception:
        target.rollback()
        log.exception("Sync failed; target transaction rolled back")
        return 1
    finally:
        target.close()
        sf_conn.close()


if __name__ == "__main__":
    sys.exit(main())
