#!/usr/bin/env python3
"""Transport B: unload Snowflake -> stage, pull files on-prem, bulk-load into MySQL / SQL Server.

The connector transport (sync.py) streams rows over a live connection. This one
instead tells Snowflake to write the result to compressed files in a stage, pulls
them down with GET, and bulk-loads them with the target's native fast path
(LOAD DATA LOCAL INFILE for MySQL, BULK INSERT for SQL Server). At large volume this is faster and decoupled:
the Snowflake session is only held for the short unload, and on-prem only needs to
reach the stage/bucket.

DEMO uses the Snowflake user stage (@~) -- zero setup, no cloud credentials.
PRODUCTION would point --stage at an EXTERNAL stage over object storage that
on-prem can also reach (see snowflake_source.unload_to_stage docstring).

Examples
--------
  # Full refresh via unload + bulk load
  python unload_sync.py --source ANALYTICS.DENTAL.DENTAL_CLAIMS --target DENTAL_CLAIMS \
      --change-capture none --mode truncate

  # Timestamp-delta upsert via unload + bulk load
  python unload_sync.py --source ANALYTICS.DENTAL.DENTAL_CLAIMS --target DENTAL_CLAIMS \
      --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID

  # Stream-based change capture, including deletes (see sql/01_snowflake_setup.sql)
  python unload_sync.py --source ANALYTICS.DENTAL.DENTAL_CLAIMS --target DENTAL_CLAIMS \
      --change-capture stream --stream ANALYTICS.DENTAL.CLAIMS_STREAM \
      --outbox ANALYTICS.DENTAL.CLAIMS_OUTBOX --mode upsert --key-cols CLAIM_ID
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import os
import shutil
import sys
from pathlib import Path

import snowflake_source as sf
from config import SnowflakeConfig, TargetConfig
from targets import make_target

log = logging.getLogger("unload_sync")


def _load_state(path: str) -> dict:
    """Watermark state: {source_table: {"watermark": value}}; {} if no file yet.

    Same format and semantics as sync.py; see "High-water-mark (hwm) state" in
    its module docstring. Use a separate --state-file per transport and target.
    """
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else {}


def _save_state(path: str, state: dict) -> None:
    """Persist watermark state. Call only after the target commit succeeds."""
    Path(path).write_text(json.dumps(state, indent=2, default=str))


def _gunzip(gz_path: str) -> str:
    """Decompress a .gz file next to it; return the .csv path (LOAD DATA needs plain text)."""
    csv_path = gz_path[:-3] if gz_path.endswith(".gz") else gz_path + ".csv"
    with gzip.open(gz_path, "rb") as src, open(csv_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    return csv_path


def _header_columns(csv_path: str) -> list[str]:
    with open(csv_path, newline="") as fh:
        return next(csv.reader(fh))


def _clear_dir(path: str) -> None:
    """Empty `path` without removing it. The directory itself is kept because
    with the SQL Server BULK INSERT method it is typically a share or mount
    that SQL Server reads; deleting and recreating it can break that mount."""
    os.makedirs(path, exist_ok=True)
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            shutil.rmtree(entry.path)
        else:
            os.unlink(entry.path)


def _pull(sf_conn, stage_path: str, local_dir: str, sub: str = "") -> list[tuple[str, str]]:
    """GET stage_path+sub into local_dir/sub and decompress.

    Returns (csv_path, rel_path) pairs; rel_path is relative to local_dir and is
    what SQL Server BULK INSERT resolves under TARGET_MSSQL_BULK_DIR.
    """
    target_dir = os.path.join(local_dir, sub) if sub else local_dir
    files = sf.get_files(sf_conn, stage_path + sub, target_dir)
    csv_paths = [_gunzip(f) if f.endswith(".gz") else f for f in files]
    return [(p, os.path.relpath(p, local_dir)) for p in csv_paths]


def _columns(csv_path: str, lower: bool) -> list[str]:
    columns = _header_columns(csv_path)
    return [c.lower() for c in columns] if lower else columns


def run_batch(sf_conn, target, args, stage_path: str, local_dir: str) -> int:
    """--change-capture none | hwm: unload one result set and bulk-load it."""
    # 1. Build the query (full or hwm delta).
    if args.change_capture == "hwm":
        # Last delivered value: saved state, else --hwm-start, else None (load all).
        # The ceiling is read before unloading so rows updated mid-run wait
        # for the next run instead of being skipped.
        state = _load_state(args.state_file)
        last = state.get(args.source, {}).get("watermark", args.hwm_start)
        ceiling = sf.scalar(sf_conn, f"SELECT MAX({args.hwm_col}) FROM {args.source}")
        if ceiling is None or (last is not None and str(ceiling) <= str(last)):
            log.info("No new rows above watermark %r (ceiling %r).", last, ceiling)
            return 0
        log.info("HWM window: %r < %s <= %r", last, args.hwm_col, ceiling)
        query = sf.build_hwm_query(args.source, args.hwm_col,
                                   has_watermark=last is not None)
        params = {"watermark": last, "ceiling": ceiling}
    else:
        query = sf.build_full_query(args.source)
        params = None

    # 2. Clear any stale unload for this target, then unload.
    sf_conn.cursor().execute(f"REMOVE {stage_path}")
    rows = sf.unload_to_stage(sf_conn, query, stage_path, params)
    if rows == 0:
        log.info("Nothing unloaded; target unchanged.")
        return 0

    # 3. Pull the files on-prem, decompress, and bulk-load in one transaction.
    _clear_dir(local_dir)
    files = _pull(sf_conn, stage_path, local_dir)
    if not files:
        log.warning("No files pulled from %s", stage_path)
        return 0
    columns = _columns(files[0][0], args.lower_cols)
    if args.mode == "truncate":
        target.truncate(args.target)
    for csv_path, rel_path in files:
        target.bulk_load(args.target, csv_path, columns, mode=args.mode,
                         key_columns=args.key_cols, rel_path=rel_path)
    target.commit()

    # 4. Advance the watermark only after the load committed.
    if args.change_capture == "hwm":
        state.setdefault(args.source, {})["watermark"] = ceiling
        _save_state(args.state_file, state)

    log.info("Bulk-loaded %s file(s) (%s rows) -> %s", len(files), rows, args.target)
    return 0


def run_stream(sf_conn, target, args, stage_path: str, local_dir: str) -> int:
    """--change-capture stream: stream -> outbox -> two unloads -> one target transaction.

    The outbox is reduced in Snowflake to the net change per key (see
    snowflake_source.build_outbox_changes_query), then unloaded twice: full rows
    to upsert under <stage>/<target>/upsert/, and keys to delete under
    <stage>/<target>/delete/. Both are applied in one target transaction, and
    the outbox is acknowledged only after it commits.
    """
    # 1. Consume the stream into the outbox (this commit advances the offset).
    sf.consume_stream_to_outbox(sf_conn, args.stream, args.outbox)

    # 2. Snapshot: handle only outbox rows consumed up to now.
    cutoff = sf.outbox_cutoff(sf_conn, args.outbox)
    if cutoff is None:
        log.info("Stream CDC: no un-exported changes in %s", args.outbox)
        return 0
    changes = sf.build_outbox_changes_query(args.outbox, args.key_cols)
    cdc = ", ".join(sf.CDC_COLUMNS)
    keys = ", ".join(args.key_cols)
    upsert_q = (f"SELECT * EXCLUDE ({cdc}) FROM ({changes}) "
                f"WHERE _CDC_ACTION = 'INSERT'")
    delete_q = f"SELECT {keys} FROM ({changes}) WHERE _CDC_ACTION = 'DELETE'"
    params = {"cutoff": cutoff}

    # 3. Unload upserts and delete keys to separate stage paths.
    sf_conn.cursor().execute(f"REMOVE {stage_path}")
    n_upsert = sf.unload_to_stage(sf_conn, upsert_q, stage_path + "upsert/", params)
    n_delete = sf.unload_to_stage(sf_conn, delete_q, stage_path + "delete/", params)

    # 4. Pull both sets and apply them in one target transaction.
    _clear_dir(local_dir)
    upserts = _pull(sf_conn, stage_path, local_dir, "upsert/") if n_upsert else []
    deletes = _pull(sf_conn, stage_path, local_dir, "delete/") if n_delete else []
    for csv_path, rel_path in upserts:
        target.bulk_load(args.target, csv_path, _columns(csv_path, args.lower_cols),
                         mode="upsert", key_columns=args.key_cols, rel_path=rel_path)
    for csv_path, rel_path in deletes:
        target.bulk_delete(args.target, csv_path, _columns(csv_path, args.lower_cols),
                           rel_path=rel_path)
    target.commit()

    # 5. Only now is the target write durable -> acknowledge up to the cutoff.
    sf.mark_outbox_exported(sf_conn, args.outbox, cutoff)
    log.info("Stream CDC complete: %s upserts, %s deletes -> %s",
             n_upsert, n_delete, args.target)
    return 0


def run(args) -> int:
    sf_conn = sf.connect(SnowflakeConfig.from_env())
    target = make_target(TargetConfig.from_env())
    local_dir = os.path.abspath(args.local_dir)
    stage_path = f"{args.stage.rstrip('/')}/{args.target.lower()}/"
    try:
        if args.change_capture == "stream":
            return run_stream(sf_conn, target, args, stage_path, local_dir)
        return run_batch(sf_conn, target, args, stage_path, local_dir)
    except Exception:
        target.rollback()
        log.exception("Unload sync failed; target rolled back")
        return 1
    finally:
        target.close()
        sf_conn.close()


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Snowflake unload -> on-prem bulk load")
    p.add_argument("--source", required=True, help="Fully-qualified source (DB.SCHEMA.OBJECT)")
    p.add_argument("--target", required=True, help="Target table name in the RDBMS")
    p.add_argument("--change-capture", choices=("none", "hwm", "stream"), default="none")
    p.add_argument("--mode", choices=("truncate", "upsert"), default="truncate")
    p.add_argument("--key-cols", nargs="*", default=None, help="Primary key column(s) for upsert")
    p.add_argument("--hwm-col", help="Monotonic column for --change-capture hwm")
    p.add_argument("--hwm-start", default=None, help="Initial watermark on first run")
    p.add_argument("--state-file", default="sync_state.json",
                   help="JSON file holding the hwm watermark per source table "
                        "(default: ./sync_state.json; use a different file than sync.py)")
    p.add_argument("--stage", default="@~/simple_reverse_etl",
                   help="Stage prefix to unload into (default: your user stage @~)")
    p.add_argument("--local-dir", default="_unload_tmp", help="Local dir for pulled files")
    p.add_argument("--stream", help="Stream name for --change-capture stream")
    p.add_argument("--outbox", help="Outbox table for --change-capture stream")
    p.add_argument("--lower-cols", action="store_true",
                   help="Lowercase Snowflake column names before loading")
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
    if args.change_capture == "stream" and not (args.stream and args.outbox
                                                and args.key_cols and args.mode == "upsert"):
        log.error("--change-capture stream requires --stream, --outbox, "
                  "--mode upsert, and --key-cols")
        return 2
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
