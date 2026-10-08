"""Pluggable target writers: MySQL and SQL Server.

Each writer exposes the same small contract so sync.py and unload_sync.py don't
care which RDBMS they're talking to:

    w = make_target(cfg)
    w.truncate(table)                                    # full-refresh mode
    w.write(table, columns, rows, mode, key_columns)     # Transport A: 'append' | 'upsert'
    w.delete(table, key_columns, key_rows)               # Transport A: CDC deletes
    w.bulk_load(table, csv_path, columns, mode, key_columns, rel_path)
                                                         # Transport B: load a CSV file
    w.bulk_delete(table, csv_path, key_columns, rel_path)
                                                         # Transport B: delete keys in a CSV
    w.commit(); w.rollback(); w.close()

`rows` is a list of tuples aligned to `columns`. NULLs must already be Python
None (sync.py normalizes pandas NaN/NaT -> None before calling).

Transport B CSV files are produced by snowflake_source.unload_to_stage: a header
row, fields optionally enclosed in double quotes, and SQL NULL written as the
sentinel NULL_TOKEN so NULL and empty strings stay distinct. `rel_path` is the
file's path relative to --local-dir; only the SQL Server BULK INSERT method uses
it (to find the same file under TARGET_MSSQL_BULK_DIR).
"""
from __future__ import annotations

import csv
import logging
from typing import Sequence

log = logging.getLogger(__name__)

Rows = Sequence[Sequence[object]]

# Must match NULL_IF in snowflake_source.unload_to_stage.
NULL_TOKEN = "__NULL__"


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
        # Note: MySQL TRUNCATE commits implicitly; a failed load after it leaves
        # the table empty until the next successful run.
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

    def _load_data_sql(self, into: str, columns, verb: str = "") -> str:
        """LOAD DATA LOCAL INFILE for an unload CSV. Each field is read into a
        user variable, then NULLIF turns NULL_TOKEN back into a real NULL (empty
        strings stay '')."""
        variables = [f"@v{i}" for i in range(len(columns))]
        set_clause = ", ".join(
            f"`{c}` = NULLIF({v}, '{NULL_TOKEN}')" for c, v in zip(columns, variables)
        )
        return (
            f"LOAD DATA LOCAL INFILE %s {verb} INTO TABLE {into} "
            "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
            "LINES TERMINATED BY '\\n' IGNORE 1 LINES "
            f"({', '.join(variables)}) SET {set_clause}"
        )

    def bulk_load(self, table, csv_path, columns, mode="upsert", key_columns=None,
                  rel_path=None) -> None:
        """Transport B load: native bulk load via LOAD DATA LOCAL INFILE.
        `mode='upsert'` uses REPLACE (a row with the same primary key is
        replaced); otherwise plain append (caller truncates first)."""
        verb = "REPLACE" if mode == "upsert" else ""
        with self.conn.cursor() as cur:
            cur.execute(self._load_data_sql(table, columns, verb), (csv_path,))

    def bulk_delete(self, table, csv_path, key_columns, rel_path=None) -> None:
        """Transport B deletes: bulk-load the key CSV into a temporary table with
        the target's key types, then delete the matching target rows in one
        statement. CREATE/DROP TEMPORARY TABLE do not commit implicitly."""
        keys = ", ".join(f"`{k}`" for k in key_columns)
        on = " AND ".join(f"t.`{k}` = d.`{k}`" for k in key_columns)
        with self.conn.cursor() as cur:
            cur.execute("DROP TEMPORARY TABLE IF EXISTS _sre_delete_keys")
            cur.execute(f"CREATE TEMPORARY TABLE _sre_delete_keys "
                        f"SELECT {keys} FROM {table} WHERE 1 = 0")
            cur.execute(self._load_data_sql("_sre_delete_keys", key_columns), (csv_path,))
            cur.execute(f"DELETE t FROM {table} t JOIN _sre_delete_keys d ON {on}")
            cur.execute("DROP TEMPORARY TABLE _sre_delete_keys")

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()


# ============================================================================
# SQL Server
# ============================================================================
def _odbc_value(value) -> str:
    """Brace-quote an ODBC connection-string value so ';', '=' or '}' inside it
    (for example in a password) cannot alter the connection string."""
    return "{" + str(value).replace("}", "}}") + "}"


def _q(name: str) -> str:
    """Bracket-quote a SQL Server column identifier."""
    return "[" + name.replace("]", "]]") + "]"


class MSSQLTarget:
    """SQL Server writer (pyodbc + Microsoft ODBC Driver 18).

    Upserts and Transport B loads go through session temp tables (#name), so no
    staging tables need to be created in the target database. Temp tables are
    created with `SELECT TOP 0 ... INTO #t FROM <table> UNION ALL SELECT TOP 0
    ...` to copy the target's column types without its IDENTITY property.
    """

    CLIENT_BATCH_ROWS = 10_000

    def __init__(self, cfg):
        import pyodbc

        conn_str = ";".join([
            f"DRIVER={_odbc_value(cfg.odbc_driver)}",
            f"SERVER={_odbc_value(f'{cfg.host},{cfg.port}')}",
            f"DATABASE={_odbc_value(cfg.database)}",
            f"UID={_odbc_value(cfg.user)}",
            f"PWD={_odbc_value(cfg.password)}",
            f"Encrypt={'yes' if cfg.mssql_encrypt else 'no'}",
            f"TrustServerCertificate={'yes' if cfg.mssql_trust_server_cert else 'no'}",
        ])
        self.conn = pyodbc.connect(conn_str, autocommit=False)
        self.load_method = cfg.mssql_load_method
        self.bulk_dir = cfg.mssql_bulk_dir
        # BULK INSERT needs CODEPAGE='65001' to read UTF-8 files on Windows; SQL
        # Server on Linux rejects the option and reads UTF-8 by default.
        self.on_windows = "on Linux" not in self.conn.execute(
            "SELECT @@VERSION").fetchone()[0]
        if self.load_method == "bulk_insert" and not self.bulk_dir:
            log.info("TARGET_MSSQL_BULK_DIR is not set; Transport B loads will fail "
                     "with load method bulk_insert")

    # --- helpers -------------------------------------------------------------

    def _cursor(self, fast=False):
        cur = self.conn.cursor()
        cur.fast_executemany = fast   # send parameter arrays in one round trip
        return cur

    @staticmethod
    def _bind_types(cur, rows) -> None:
        """Declare timestamp parameters with 7 fractional digits. Without this,
        fast_executemany binds Python datetimes with zero fractional digits and
        SQL Server silently drops milliseconds."""
        import datetime
        import pyodbc

        sizes = []
        for i in range(len(rows[0])):
            sample = next((r[i] for r in rows if r[i] is not None), None)
            is_ts = isinstance(sample, datetime.datetime)
            sizes.append((pyodbc.SQL_TYPE_TIMESTAMP, 0, 7) if is_ts else None)
        if any(sizes):
            cur.setinputsizes(sizes)

    def _typed_temp(self, cur, temp: str, table: str, columns) -> None:
        """Create #temp with the same types as `columns` in `table` (no IDENTITY)."""
        cols = ", ".join(_q(c) for c in columns)
        cur.execute(f"DROP TABLE IF EXISTS {temp}")
        cur.execute(f"SELECT TOP 0 {cols} INTO {temp} FROM {table} "
                    f"UNION ALL SELECT TOP 0 {cols} FROM {table}")

    def _text_temp(self, cur, temp: str, columns) -> None:
        """Create #temp with an NVARCHAR(MAX) column per CSV column."""
        cur.execute(f"DROP TABLE IF EXISTS {temp}")
        cur.execute(f"CREATE TABLE {temp} ("
                    + ", ".join(f"{_q(c)} NVARCHAR(MAX) NULL" for c in columns) + ")")

    def _server_path(self, csv_path: str, rel_path: str | None) -> str:
        """Path of the CSV as SQL Server sees it: TARGET_MSSQL_BULK_DIR + rel_path."""
        if not self.bulk_dir:
            raise RuntimeError(
                "TARGET_MSSQL_BULK_DIR must be set for TARGET_MSSQL_LOAD_METHOD=bulk_insert")
        sep = "\\" if "\\" in self.bulk_dir else "/"
        rel = (rel_path or csv_path.rsplit("/", 1)[-1]).replace("/", sep)
        return self.bulk_dir.rstrip("/\\") + sep + rel

    def _fill_from_csv(self, cur, temp: str, columns, csv_path, rel_path) -> int:
        """Load a header CSV into #temp with BULK INSERT or client-side batches."""
        if self.load_method == "bulk_insert":
            path = self._server_path(csv_path, rel_path).replace("'", "''")
            codepage = ", CODEPAGE = '65001'" if self.on_windows else ""
            cur.execute(
                f"BULK INSERT {temp} FROM '{path}' WITH ("
                "FORMAT = 'CSV', FIELDQUOTE = '\"', FIRSTROW = 2, "
                f"FIELDTERMINATOR = ',', ROWTERMINATOR = '0x0a'{codepage}, TABLOCK)"
            )
            return cur.rowcount
        cols = ", ".join(_q(c) for c in columns)
        sql = f"INSERT INTO {temp} ({cols}) VALUES ({', '.join('?' * len(columns))})"
        total = 0
        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            next(reader)  # header
            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= self.CLIENT_BATCH_ROWS:
                    cur.executemany(sql, batch)
                    total += len(batch)
                    batch = []
            if batch:
                cur.executemany(sql, batch)
                total += len(batch)
        return total

    def _merge_sql(self, table, source, columns, key_columns) -> str:
        on = " AND ".join(f"t.{_q(k)} = s.{_q(k)}" for k in key_columns)
        non_keys = [c for c in columns if c not in set(key_columns)]
        matched = ("WHEN MATCHED THEN UPDATE SET "
                   + ", ".join(f"t.{_q(c)} = s.{_q(c)}" for c in non_keys) + " "
                   if non_keys else "")
        cols = ", ".join(_q(c) for c in columns)
        vals = ", ".join(f"s.{_q(c)}" for c in columns)
        # HOLDLOCK makes the MERGE upsert safe against concurrent writers.
        return (f"MERGE {table} WITH (HOLDLOCK) AS t USING {source} AS s ON {on} "
                f"{matched}WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals});")

    # --- contract --------------------------------------------------------------

    def truncate(self, table: str) -> None:
        cur = self._cursor()
        cur.execute(f"TRUNCATE TABLE {table}")   # transactional on SQL Server
        cur.close()

    def write(self, table, columns, rows, mode="append", key_columns=None) -> None:
        if not rows:
            return
        cur = self._cursor(fast=True)
        cols = ", ".join(_q(c) for c in columns)
        placeholders = ", ".join(["?"] * len(columns))
        if mode != "upsert" or not key_columns:
            self._bind_types(cur, rows)
            cur.executemany(f"INSERT INTO {table} ({cols}) VALUES ({placeholders})", rows)
            cur.close()
            return
        # Upsert: batch-insert into a typed session temp table, then one MERGE.
        self._typed_temp(cur, "#sre_stage", table, columns)
        self._bind_types(cur, rows)
        cur.executemany(f"INSERT INTO #sre_stage ({cols}) VALUES ({placeholders})", rows)
        cur.execute(self._merge_sql(table, "#sre_stage", columns, key_columns))
        cur.execute("DROP TABLE #sre_stage")
        cur.close()

    def delete(self, table, key_columns, key_rows) -> None:
        if not key_rows:
            return
        cur = self._cursor(fast=True)
        where = " AND ".join(f"{_q(k)} = ?" for k in key_columns)
        self._bind_types(cur, key_rows)
        cur.executemany(f"DELETE FROM {table} WHERE {where}", key_rows)
        cur.close()

    def bulk_load(self, table, csv_path, columns, mode="upsert", key_columns=None,
                  rel_path=None) -> None:
        """Transport B load. The CSV is loaded as text into #sre_load (BULK INSERT,
        or client-side batches with TARGET_MSSQL_LOAD_METHOD=client), then
        NULL_TOKEN is converted to NULL and SQL Server converts the text to the
        target column types during the MERGE (upsert) or INSERT ... SELECT."""
        cur = self._cursor(fast=True)
        self._text_temp(cur, "#sre_load", columns)
        n = self._fill_from_csv(cur, "#sre_load", columns, csv_path, rel_path)
        select = ", ".join(f"NULLIF({_q(c)}, N'{NULL_TOKEN}') AS {_q(c)}" for c in columns)
        source = f"(SELECT {select} FROM #sre_load)"
        if mode == "upsert":
            cur.execute(self._merge_sql(table, source, columns, key_columns))
        else:
            cols = ", ".join(_q(c) for c in columns)
            cur.execute(f"INSERT INTO {table} ({cols}) SELECT {select} FROM #sre_load")
        cur.execute("DROP TABLE #sre_load")
        cur.close()
        log.info("  %s: %s rows from %s (%s)", table, n, rel_path or csv_path,
                 self.load_method)

    def bulk_delete(self, table, csv_path, key_columns, rel_path=None) -> None:
        """Transport B deletes: load the key CSV into a typed #sre_delete_keys,
        then delete the matching target rows with one join."""
        cur = self._cursor(fast=True)
        self._typed_temp(cur, "#sre_delete_keys", table, key_columns)
        self._fill_from_csv(cur, "#sre_delete_keys", key_columns, csv_path, rel_path)
        on = " AND ".join(f"t.{_q(k)} = d.{_q(k)}" for k in key_columns)
        cur.execute(f"DELETE t FROM {table} AS t JOIN #sre_delete_keys AS d ON {on}")
        cur.execute("DROP TABLE #sre_delete_keys")
        cur.close()

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
