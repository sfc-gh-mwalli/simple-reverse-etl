"""Transports: HOW a LoadPlan's rows move from Snowflake into the target.

  pull    Transport A. Read the plan's queries over the connector as Arrow
          batches and write them with batched DML (target.write / target.delete).
  unload  Transport B. COPY INTO a stage as compressed CSV, download the files
          (GET), and load them with the target's native bulk path
          (target.bulk_load / target.bulk_delete).

Both apply the whole plan and leave the final commit to the caller (sync.py),
which then runs plan.on_commit. The pull transport may commit every
--commit-rows rows when plan.single_transaction is False.

The demo unloads to an INTERNAL named stage (self-contained, no cloud
credentials). Production usually uses an EXTERNAL stage over object storage;
GET does not support external stages, so files are then downloaded with the
cloud provider's SDK or CLI instead (see README.md).
"""
from __future__ import annotations

import csv
import gzip
import logging
import os
import shutil

import pandas as pd

import snowflake_source as sf

log = logging.getLogger("transports")


def _df_to_rows(df: pd.DataFrame):
    """DataFrame -> list of tuples with pandas NaN/NaT coerced to None."""
    obj = df.astype(object).where(pd.notnull(df), None)
    return list(obj.itertuples(index=False, name=None))


def _names(columns, lower: bool) -> list[str]:
    return [c.lower() for c in columns] if lower else list(columns)


# --- Transport A: pull ---------------------------------------------------------

def pull_apply(sf_conn, target, plan, args) -> tuple[int, int]:
    """Returns (rows written, rows deleted)."""
    if plan.truncate:
        target.truncate(args.target)
    written = pending = 0
    for df in sf.read_query_batches(sf_conn, plan.upsert_query, plan.params):
        cols = _names(df.columns, args.lower_cols)
        target.write(args.target, cols, _df_to_rows(df), mode=args.mode,
                     key_columns=args.key_cols)
        written += len(df)
        pending += len(df)
        if not plan.single_transaction and pending >= args.commit_rows:
            target.commit()
            log.info("  committed (%s rows so far)", written)
            pending = 0
    deleted = 0
    if plan.delete_query:
        for df in sf.read_query_batches(sf_conn, plan.delete_query, plan.params):
            target.delete(args.target, _names(df.columns, args.lower_cols),
                          _df_to_rows(df))
            deleted += len(df)
    return written, deleted


# --- Transport B: unload -------------------------------------------------------

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


def _gunzip(gz_path: str) -> str:
    """Decompress a .gz file next to it; return the .csv path (bulk loaders need plain text)."""
    csv_path = gz_path[:-3] if gz_path.endswith(".gz") else gz_path + ".csv"
    with gzip.open(gz_path, "rb") as src, open(csv_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    return csv_path


def _header_columns(csv_path: str) -> list[str]:
    with open(csv_path, newline="", encoding="utf-8") as fh:
        return next(csv.reader(fh))


def _pull(sf_conn, stage_path: str, local_dir: str, sub: str) -> list[tuple[str, str]]:
    """GET stage_path+sub into local_dir/sub and decompress.

    Returns (csv_path, rel_path) pairs; rel_path is relative to local_dir and is
    what SQL Server BULK INSERT resolves under TARGET_MSSQL_BULK_DIR.
    """
    files = sf.get_files(sf_conn, stage_path + sub, os.path.join(local_dir, sub))
    csv_paths = [_gunzip(f) if f.endswith(".gz") else f for f in files]
    return [(p, os.path.relpath(p, local_dir)) for p in csv_paths]


def unload_apply(sf_conn, target, plan, args) -> tuple[int, int]:
    """Returns (rows unloaded for writing, rows unloaded for deletion).

    Layout: <stage>/<target>/upsert/ holds the rows to write and, in stream
    mode, <stage>/<target>/delete/ holds the keys to delete. They are retrieved
    into --local-dir/upsert/ and --local-dir/delete/. Stage files are kept until
    the next run for the same target; --local-dir is emptied at the start of
    every run, so use a separate --local-dir per concurrently scheduled target.
    """
    stage_path = f"{args.stage.rstrip('/')}/{args.target.lower()}/"
    local_dir = os.path.abspath(args.local_dir)

    # 1. Clear any previous unload for this target, then unload.
    sf_conn.cursor().execute(f"REMOVE {stage_path}")
    n_rows = sf.unload_to_stage(sf_conn, plan.upsert_query, stage_path + "upsert/",
                                plan.params)
    n_keys = (sf.unload_to_stage(sf_conn, plan.delete_query, stage_path + "delete/",
                                 plan.params) if plan.delete_query else 0)

    # 2. Download and decompress.
    _clear_dir(local_dir)
    rows = _pull(sf_conn, stage_path, local_dir, "upsert/") if n_rows else []
    keys = _pull(sf_conn, stage_path, local_dir, "delete/") if n_keys else []

    # 3. Bulk-load and bulk-delete (the caller commits).
    if plan.truncate:
        target.truncate(args.target)
    for csv_path, rel_path in rows:
        target.bulk_load(args.target, csv_path,
                         _names(_header_columns(csv_path), args.lower_cols),
                         mode=args.mode, key_columns=args.key_cols, rel_path=rel_path)
    for csv_path, rel_path in keys:
        target.bulk_delete(args.target, csv_path,
                           _names(_header_columns(csv_path), args.lower_cols),
                           rel_path=rel_path)
    log.info("Bulk-loaded %s file(s) (%s rows) -> %s",
             len(rows) + len(keys), n_rows + n_keys, args.target)
    return n_rows, n_keys


TRANSPORTS = {"pull": pull_apply, "unload": unload_apply}
