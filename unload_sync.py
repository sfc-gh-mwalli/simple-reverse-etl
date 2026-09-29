#!/usr/bin/env python3
"""Transport B: unload Snowflake -> stage, pull files on-prem, bulk-load into MySQL.

The connector transport (sync.py) streams rows over a live connection. This one
instead tells Snowflake to write the result to compressed files in a stage, pulls
them down with GET, and bulk-loads them with the target's native fast path
(LOAD DATA LOCAL INFILE for MySQL). At large volume this is faster and decoupled:
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
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else {}


def _save_state(path: str, state: dict) -> None:
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


def run(args) -> int:
    sf_conn = sf.connect(SnowflakeConfig.from_env())
    target = make_target(TargetConfig.from_env())
    local_dir = os.path.abspath(args.local_dir)
    stage_path = f"{args.stage.rstrip('/')}/{args.target.lower()}/"

    try:
        # 1. Build the query (full or hwm delta) and unload it to the stage.
        if args.change_capture == "hwm":
            state = _load_state(args.state_file)
            last = state.get(args.source, {}).get("watermark", args.hwm_start)
            ceiling = sf.scalar(sf_conn, f"SELECT MAX({args.hwm_col}) FROM {args.source}")
            if ceiling is None or (last is not None and str(ceiling) <= str(last)):
                log.info("No new rows above watermark %r (ceiling %r).", last, ceiling)
                return 0
            log.info("HWM window: %r < %s <= %r", last, args.hwm_col, ceiling)
            query = sf.build_hwm_query(args.source, args.hwm_col)
            params = {"watermark": last, "ceiling": ceiling}
        else:
            query = sf.build_full_query(args.source)
            params = None
            ceiling = None

        # Clear any stale unload for this target, then unload.
        sf_conn.cursor().execute(f"REMOVE {stage_path}")
        rows = sf.unload_to_stage(sf_conn, query, stage_path, params)
        if rows == 0:
            log.info("Nothing unloaded; target unchanged.")
            return 0

        # 2. Pull the files on-prem and decompress.
        if os.path.isdir(local_dir):
            shutil.rmtree(local_dir)
        gz_files = sf.get_files(sf_conn, stage_path, local_dir)
        csv_files = [_gunzip(f) if f.endswith(".gz") else f for f in gz_files]
        if not csv_files:
            log.warning("No files pulled from %s", stage_path)
            return 0
        columns = _header_columns(csv_files[0])
        if args.lower_cols:
            columns = [c.lower() for c in columns]

        # 3. Bulk-load into the target.
        if args.mode == "truncate":
            target.truncate(args.target)
        loaded = 0
        for path in csv_files:
            target.bulk_load(args.target, path, columns,
                             mode=args.mode, key_columns=args.key_cols)
            loaded += 1
        target.commit()

        # 4. Advance the watermark only after the load committed.
        if args.change_capture == "hwm":
            state.setdefault(args.source, {})["watermark"] = ceiling
            _save_state(args.state_file, state)

        log.info("Bulk-loaded %s file(s) (%s rows) -> %s", loaded, rows, args.target)
        return 0
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
    p.add_argument("--change-capture", choices=("none", "hwm"), default="none")
    p.add_argument("--mode", choices=("truncate", "upsert"), default="truncate")
    p.add_argument("--key-cols", nargs="*", default=None, help="Primary key column(s) for upsert")
    p.add_argument("--hwm-col", help="Monotonic column for --change-capture hwm")
    p.add_argument("--hwm-start", default=None, help="Initial watermark on first run")
    p.add_argument("--state-file", default="sync_state.json")
    p.add_argument("--stage", default="@~/simple_reverse_etl",
                   help="Stage prefix to unload into (default: your user stage @~)")
    p.add_argument("--local-dir", default="_unload_tmp", help="Local dir for pulled files")
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
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
