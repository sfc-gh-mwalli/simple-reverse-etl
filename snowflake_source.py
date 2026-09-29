"""Snowflake source: connection + high-throughput batched read + CDC helpers.

Performance notes (matters at 30-60M rows):
  * fetch_pandas_batches() pulls results in the Arrow format the server already
    produces. Arrow is columnar and compressed on the wire, and we get ONE
    pandas DataFrame per result batch instead of materializing the whole result
    set in memory. Memory stays bounded no matter how large the table is.
  * For a distributed/parallel fetch, cur.get_result_batches() hands back
    ResultBatch objects you can farm out to threads/processes. We keep the main
    path single-stream because for this use case the on-prem RDBMS write is the
    bottleneck, not the Snowflake read. See README "Scaling up".
"""
from __future__ import annotations

import logging
import os
from typing import Iterator

import pandas as pd
import snowflake.connector

log = logging.getLogger(__name__)


def _private_key_der(path: str, passphrase: str | None) -> bytes:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    with open(path, "rb") as fh:
        key = serialization.load_pem_private_key(
            fh.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def connect(cfg) -> "snowflake.connector.SnowflakeConnection":
    """Open a Snowflake connection.

    connection_name: reuse a named entry from ~/.snowflake/connections.toml.
    PAT (demo): the token is presented in place of the password.
    Key-pair (prod): an RSA private key is loaded and passed as private_key.
    """
    # Overrides applied on top of whichever auth path we use.
    overrides: dict = {}
    for attr in ("role", "warehouse", "database", "schema"):
        val = getattr(cfg, attr, None)
        if val:
            overrides[attr] = val

    if getattr(cfg, "connection_name", None):
        log.info("Connecting to Snowflake via connections.toml entry %r",
                 cfg.connection_name)
        return snowflake.connector.connect(
            connection_name=cfg.connection_name, **overrides
        )

    kwargs: dict = {"account": cfg.account, "user": cfg.user, **overrides}
    if cfg.auth_method == "keypair":
        kwargs["private_key"] = _private_key_der(
            cfg.private_key_path, cfg.private_key_passphrase
        )
    else:  # pat
        kwargs["password"] = cfg.pat

    log.info("Connecting to Snowflake account=%s user=%s auth=%s",
             cfg.account, cfg.user, cfg.auth_method)
    return snowflake.connector.connect(**kwargs)


def read_query_batches(conn, query: str, params=None) -> Iterator[pd.DataFrame]:
    """Execute `query` and yield one pandas DataFrame per Arrow result batch."""
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        for batch in cur.fetch_pandas_batches():
            yield batch
    finally:
        cur.close()


def scalar(conn, query: str, params=None):
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()


# --- change-capture query builders ------------------------------------------

def build_full_query(source: str) -> str:
    return f"SELECT * FROM {source}"


def build_hwm_query(source: str, hwm_col: str) -> str:
    """Rows strictly above the last watermark, up to a captured ceiling.

    The ceiling (bound with %(ceiling)s at call time) is read first via
    SELECT MAX(hwm_col) so a value that keeps advancing during the read can't
    cause us to skip late rows.
    """
    return (
        f"SELECT * FROM {source} "
        f"WHERE {hwm_col} > %(watermark)s AND {hwm_col} <= %(ceiling)s "
        f"ORDER BY {hwm_col}"
    )


# --- stream / outbox CDC helpers --------------------------------------------

def consume_stream_to_outbox(conn, stream: str, outbox: str) -> int:
    """Consume the stream into the outbox. Committing this INSERT is what
    advances the stream offset. Returns the number of change rows captured."""
    sql = (
        f"INSERT INTO {outbox} "
        f"SELECT s.*, METADATA$ACTION, METADATA$ISUPDATE, CURRENT_TIMESTAMP(), FALSE "
        f"FROM {stream} AS s"
    )
    cur = conn.cursor()
    try:
        conn.cursor().execute("BEGIN")
        cur.execute(sql)
        captured = cur.rowcount or 0
        conn.cursor().execute("COMMIT")
        log.info("Consumed %s change rows from %s into %s", captured, stream, outbox)
        return captured
    except Exception:
        conn.cursor().execute("ROLLBACK")
        raise
    finally:
        cur.close()


def read_outbox_batches(conn, outbox: str, cutoff_col: str = "_CDC_LOADED_AT"
                        ) -> Iterator[pd.DataFrame]:
    """Yield un-exported outbox rows in Arrow batches, oldest first."""
    query = (
        f"SELECT * FROM {outbox} WHERE _CDC_EXPORTED = FALSE ORDER BY {cutoff_col}"
    )
    yield from read_query_batches(conn, query)


def mark_outbox_exported(conn, outbox: str) -> int:
    """Mark currently un-exported rows as exported. Call only AFTER the target
    write for those rows has committed."""
    cur = conn.cursor()
    try:
        cur.execute(f"UPDATE {outbox} SET _CDC_EXPORTED = TRUE WHERE _CDC_EXPORTED = FALSE")
        return cur.rowcount or 0
    finally:
        cur.close()


# --- Transport B: unload to a stage, then pull files on-prem -----------------
#
# Instead of streaming rows over a live connection (Transport A), Transport B
# tells Snowflake to write the query result to files in a *stage*, then the
# on-prem job pulls those files down and bulk-loads them locally. Advantages at
# large volume: the unload is massively parallel and produces compressed files,
# the load uses the target's native bulk path, and on-prem only needs to reach
# the stage/bucket -- not Snowflake directly.
#
# DEMO uses an INTERNAL named stage (self-contained, no cloud credentials).
# PRODUCTION would use an EXTERNAL stage over object storage that on-prem can
# also reach, e.g.:
#
#   CREATE STAGE my_ext_stage
#     URL='s3://my-bucket/exports/'
#     STORAGE_INTEGRATION = my_s3_int
#     FILE_FORMAT = (TYPE=CSV COMPRESSION=GZIP FIELD_OPTIONALLY_ENCLOSED_BY='"');
#
# ...then Snowflake unloads to the bucket and the on-prem side pulls with the
# cloud SDK / CLI instead of GET. Everything else in this module is identical.


def unload_to_stage(conn, query: str, stage_path: str, params=None) -> int:
    """COPY the result of `query` into compressed CSV files at `stage_path`
    (e.g. '@my_stage/claims/'). Returns the number of rows unloaded.

    HEADER=TRUE writes a header row (the bulk loader skips it). OVERWRITE=TRUE
    keeps re-runs idempotent for the demo.
    """
    sql = (
        f"COPY INTO {stage_path} FROM ({query}) "
        "FILE_FORMAT = (TYPE = CSV COMPRESSION = GZIP "
        "FIELD_OPTIONALLY_ENCLOSED_BY = '\"' NULL_IF = ('')) "
        "HEADER = TRUE OVERWRITE = TRUE MAX_FILE_SIZE = 100000000"
    )
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        rows = 0
        # COPY INTO <location> summary row is (rows_unloaded, input_bytes, output_bytes).
        for row in cur:
            try:
                rows += int(row[0])  # rows_unloaded
            except (IndexError, TypeError, ValueError):
                pass
        log.info("Unloaded %s rows to %s", rows, stage_path)
        return rows
    finally:
        cur.close()


def get_files(conn, stage_path: str, local_dir: str) -> list[str]:
    """GET files from `stage_path` down to `local_dir`. Returns local file paths.

    (External-stage production variant pulls with the cloud SDK/CLI instead.)
    """
    os.makedirs(local_dir, exist_ok=True)
    cur = conn.cursor()
    try:
        # file:// URI; Snowflake GET downloads every file under the stage path.
        cur.execute(f"GET {stage_path} 'file://{local_dir}'")
        cur.fetchall()
    finally:
        cur.close()
    return [
        os.path.join(local_dir, f)
        for f in sorted(os.listdir(local_dir))
        if f.endswith(".gz") or f.endswith(".csv")
    ]
