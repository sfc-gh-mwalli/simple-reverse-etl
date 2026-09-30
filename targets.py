"""Pluggable target writers: MySQL and SQL Server.

Each writer exposes the same small contract so sync.py doesn't care which RDBMS
it's talking to:

    w = make_target(cfg)
    w.truncate(table)                                  # full-refresh mode
    w.write(table, columns, rows, mode, key_columns)   # 'append' | 'upsert'
    w.delete(table, key_columns, key_rows)             # CDC deletes
    w.commit(); w.close()

`rows` is a list of tuples aligned to `columns`. NULLs must already be Python
None (sync.py normalizes pandas NaN/NaT -> None before calling).
"""
from __future__ import annotations

import logging
from typing import Sequence

log = logging.getLogger(__name__)

Rows = Sequence[Sequence[object]]


# ============================================================================
# MySQL
# ============================================================================
class MySQLTarget:
    def __init__(self, cfg):
        import pymysql

        self.conn = pymysql.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            password=cfg.password,
            database=cfg.database,
            charset="utf8mb4",
            autocommit=False,
            local_infile=True,   # required for LOAD DATA LOCAL INFILE (Transport B)
        )

    def truncate(self, table: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")

    def write(self, table, columns, rows, mode="append", key_columns=None) -> None:
        if not rows:
            return
        cols = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        if mode == "upsert":
            keys = set(key_columns or [])
            updates = ", ".join(
                f"`{c}`=VALUES(`{c}`)" for c in columns if c not in keys
            )
            if updates:  # if every column is a key there's nothing to update
                sql += f" ON DUPLICATE KEY UPDATE {updates}"
        # pymysql rewrites executemany INSERT ... VALUES (...) [ON DUPLICATE ...]
        # into a single multi-row statement -> fast.
        with self.conn.cursor() as cur:
            cur.executemany(sql, rows)

    def delete(self, table, key_columns, key_rows) -> None:
        if not key_rows:
            return
        where = " AND ".join(f"`{k}`=%s" for k in key_columns)
        with self.conn.cursor() as cur:
            cur.executemany(f"DELETE FROM {table} WHERE {where}", key_rows)

    def bulk_load(self, table, csv_path, columns, mode="upsert", key_columns=None) -> None:
        """Transport B load: native bulk load of a header CSV via
        LOAD DATA LOCAL INFILE. `mode='upsert'` uses REPLACE (row with the same
        primary key is replaced); otherwise plain append (caller truncates first).
        Far faster than row-by-row executemany for large files.

        Each field is read into a user variable, then NULLIF turns the unload
        sentinel '__NULL__' back into a real NULL (empty strings stay ''). The
        sentinel must match NULL_IF in snowflake_source.unload_to_stage.
        """
        verb = "REPLACE" if mode == "upsert" else ""
        variables = [f"@v{i}" for i in range(len(columns))]
        set_clause = ", ".join(
            f"`{c}` = NULLIF({v}, '__NULL__')" for c, v in zip(columns, variables)
        )
        sql = (
            f"LOAD DATA LOCAL INFILE %s {verb} INTO TABLE {table} "
            "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
            "LINES TERMINATED BY '\\n' IGNORE 1 LINES "
            f"({', '.join(variables)}) SET {set_clause}"
        )
        with self.conn.cursor() as cur:
            cur.execute(sql, (csv_path,))

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()


# ============================================================================
# SQL Server
# ============================================================================
class MSSQLTarget:
    def __init__(self, cfg):
        import pyodbc

        conn_str = (
            f"DRIVER={{{cfg.odbc_driver}}};"
            f"SERVER={cfg.host},{cfg.port};"
            f"DATABASE={cfg.database};"
            f"UID={cfg.user};PWD={cfg.password};"
            "Encrypt=yes;TrustServerCertificate=yes"
        )
        self.conn = pyodbc.connect(conn_str, autocommit=False)

    def truncate(self, table: str) -> None:
        cur = self.conn.cursor()
        cur.execute(f"TRUNCATE TABLE {table}")
        cur.close()

    def write(self, table, columns, rows, mode="append", key_columns=None) -> None:
        if not rows:
            return
        cur = self.conn.cursor()
        cur.fast_executemany = True   # batches parameter arrays -> big speedup
        cols = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join(["?"] * len(columns))

        if mode != "upsert" or not key_columns:
            cur.executemany(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", rows)
            cur.close()
            return

        # Upsert = bulk-load into a staging table, then a single set-based MERGE.
        # Requires a table named <table>_stg with the same column shape.
        stg = f"{table}_stg"
        cur.execute(f"TRUNCATE TABLE {stg}")
        cur.executemany(f"INSERT INTO {stg} ({cols}) VALUES ({placeholders})", rows)

        on = " AND ".join(f"t.[{k}] = s.[{k}]" for k in key_columns)
        non_keys = [c for c in columns if c not in set(key_columns)]
        set_clause = ", ".join(f"t.[{c}] = s.[{c}]" for c in non_keys)
        insert_cols = ", ".join(f"[{c}]" for c in columns)
        insert_vals = ", ".join(f"s.[{c}]" for c in columns)
        matched = f"WHEN MATCHED THEN UPDATE SET {set_clause} " if non_keys else ""
        merge = (
            f"MERGE {table} AS t USING {stg} AS s ON {on} "
            f"{matched}"
            f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
        )
        cur.execute(merge)   # trailing ';' is required by SQL Server for MERGE
        cur.close()

    def delete(self, table, key_columns, key_rows) -> None:
        if not key_rows:
            return
        cur = self.conn.cursor()
        cur.fast_executemany = True
        where = " AND ".join(f"[{k}] = ?" for k in key_columns)
        cur.executemany(f"DELETE FROM {table} WHERE {where}", key_rows)
        cur.close()

    def bulk_load(self, table, csv_path, columns, mode="upsert", key_columns=None) -> None:
        """Transport B load for SQL Server. The native bulk paths are BULK INSERT
        (server-side: the file must be reachable by the SQL Server host) or the
        `bcp` command-line utility (client-side). Both are environment-specific,
        so this is intentionally left as a documented stub -- the local demo runs
        the MySQL target. Sketch:

            BULK INSERT {table} FROM '<path>'
              WITH (FORMAT='CSV', FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='0x0a');
            -- then MERGE from a staging table for upsert semantics.
        """
        raise NotImplementedError(
            "SQL Server bulk load uses BULK INSERT / bcp (environment-specific); "
            "use the connector transport (sync.py) or wire up bcp for your host."
        )

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()


def make_target(cfg):
    if cfg.kind == "mysql":
        return MySQLTarget(cfg)
    if cfg.kind == "mssql":
        return MSSQLTarget(cfg)
    raise ValueError(f"Unknown target kind: {cfg.kind}")
