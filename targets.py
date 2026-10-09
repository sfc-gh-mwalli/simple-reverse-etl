"""Pluggable target writers: MySQL and SQL Server.

Each writer exposes the same small contract so the transports (transports.py)
don't care which RDBMS they're talking to:

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
None (transports.py normalizes pandas NaN/NaT -> None before calling).

Transport B CSV files are produced by snowflake_source.unload_to_stage: a header
row, fields optionally enclosed in double quotes, and SQL NULL written as the
sentinel NULL_TOKEN so NULL and empty strings stay distinct. `rel_path` is the
file's path relative to --local-dir; only the SQL Server BULK INSERT method uses
it (to find the same file under TARGET_MSSQL_BULK_DIR).
"""
from __future__ import annotations

import csv
import logging
import os
import shutil
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

        # TLS: PyMySQL negotiates TLS when the server offers it, but checks the
        # server certificate only when a CA file is given.
        tls = {}
        if cfg.mysql_ssl_ca:
            tls = {"ssl_ca": cfg.mysql_ssl_ca, "ssl_verify_cert": True,
                   "ssl_verify_identity": cfg.mysql_ssl_verify_identity}
        self.conn = pymysql.connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            password=cfg.password,
            database=cfg.database,
            charset="utf8mb4",
            autocommit=False,
            local_infile=True,   # required for LOAD DATA LOCAL INFILE (Transport B)
            # Seconds to wait for the server to answer a statement (None: forever).
            read_timeout=cfg.statement_timeout,
            write_timeout=cfg.statement_timeout,
            **tls,
        )

    def acquire_lock(self, name: str) -> bool:
        """Take a server-wide named lock without waiting. It is held until the
        connection closes, across commits (MySQL GET_LOCK)."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT GET_LOCK(%s, 0)", (_lock_name(name, 64),))
            self._lock = _lock_name(name, 64) if cur.fetchone()[0] == 1 else None
        return self._lock is not None

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

    def _load_data_sql(self, into: str, columns, verb: str = "", types_of: str | None = None) -> str:
        """LOAD DATA LOCAL INFILE for an unload CSV. Each field is read into a
        user variable, then NULLIF turns NULL_TOKEN back into a real NULL (empty
        strings stay ''). Two Snowflake text forms need converting: BOOLEAN is
        unloaded as true/false, which MySQL rejects for TINYINT(1)/BIT columns,
        so those map to 1/0; BINARY is unloaded as hex, so binary columns are
        decoded with UNHEX. Column types come from `types_of` (default `into`)."""
        types = self._column_types(types_of or into)
        variables = [f"@v{i}" for i in range(len(columns))]

        def value(c, v):
            plain = f"NULLIF({v}, '{NULL_TOKEN}')"
            kind = types.get(c.lower())
            if kind in ("tinyint", "bit"):
                return f"CASE {v} WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE {plain} END"
            if kind in ("binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob"):
                return f"UNHEX({plain})"
            return plain

        set_clause = ", ".join(f"`{c}` = {value(c, v)}" for c, v in zip(columns, variables))
        return (
            f"LOAD DATA LOCAL INFILE %s {verb} INTO TABLE {into} "
            "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' "
            "LINES TERMINATED BY '\\n' IGNORE 1 LINES "
            f"({', '.join(variables)}) SET {set_clause}"
        )

    def _column_types(self, table: str) -> dict[str, str]:
        """{lower-case column name: DATA_TYPE} for `table` in the current database."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s", (table,))
            return {r[0].lower(): r[1].lower() for r in cur.fetchall()}

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
            cur.execute(self._load_data_sql("_sre_delete_keys", key_columns, types_of=table),
                        (csv_path,))
            cur.execute(f"DELETE t FROM {table} t JOIN _sre_delete_keys d ON {on}")
            cur.execute("DROP TEMPORARY TABLE _sre_delete_keys")

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        if getattr(self, "_lock", None):
            try:
                with self.conn.cursor() as cur:
                    cur.execute("DO RELEASE_LOCK(%s)", (self._lock,))
            except Exception:  # noqa: BLE001 - closing the session releases it anyway
                pass
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
        if cfg.statement_timeout:
            self.conn.timeout = cfg.statement_timeout   # per statement, in seconds
        self.load_method = cfg.mssql_load_method
        self.bulk_dir = cfg.mssql_bulk_dir
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
        """Declare parameter types that fast_executemany would otherwise get wrong:

        - timestamps: 7 fractional digits. Without this, Python datetimes are
          bound with zero fractional digits and SQL Server drops milliseconds.
        - strings: Unicode (SQL_WVARCHAR). Without this, strings bound to
          NVARCHAR(MAX) columns are sent as non-Unicode and characters outside
          the server code page (for example CJK) become '?'.

        Decimals are converted to text beforehand (see _exact_decimals).
        """
        import datetime
        import pyodbc

        sizes = []
        for i in range(len(rows[0])):
            values = [r[i] for r in rows if r[i] is not None]
            sample = values[0] if values else None
            if isinstance(sample, datetime.datetime):
                sizes.append((pyodbc.SQL_TYPE_TIMESTAMP, 0, 7))
            elif isinstance(sample, str):
                longest = max(len(v) for v in values if isinstance(v, str))
                # 0 means NVARCHAR(MAX); a bounded size keeps the fast path.
                sizes.append((pyodbc.SQL_WVARCHAR, max(longest, 1) if longest <= 4000 else 0, 0))
            else:
                sizes.append(None)
        if any(sizes):
            cur.setinputsizes(sizes)

    @staticmethod
    def _exact_decimals(rows):
        """Send Decimal values as text, which SQL Server converts exactly. With
        fast_executemany, pyodbc rounds Decimals beyond about 15 significant
        digits, even when the parameter precision is declared."""
        import decimal
        if not any(isinstance(v, decimal.Decimal) for r in rows for v in r):
            return rows
        return [tuple(format(v, "f") if isinstance(v, decimal.Decimal) else v for v in r)
                for r in rows]

    def _column_types(self, cur, table: str) -> dict[str, str]:
        """{lower-case column name: DATA_TYPE} for `table`."""
        cur.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_NAME = PARSENAME(?, 1) "
                    "AND TABLE_SCHEMA = COALESCE(PARSENAME(?, 2), SCHEMA_NAME())", table, table)
        return {r[0].lower(): r[1].lower() for r in cur.fetchall()}

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
            # BULK INSERT does not reliably read UTF-8 (SQL Server on Linux rejects
            # CODEPAGE and assumes a legacy code page), but reads UTF-16 on every
            # platform with DATAFILETYPE='widechar'. Convert next to the original,
            # in the same folder SQL Server reads.
            wide_path = csv_path + ".utf16"
            with open(csv_path, encoding="utf-8", newline="") as src, \
                 open(wide_path, "w", encoding="utf-16", newline="") as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
            wide_rel = (rel_path or os.path.basename(csv_path)) + ".utf16"
            path = self._server_path(wide_path, wide_rel).replace("'", "''")
            cur.execute(
                f"BULK INSERT {temp} FROM '{path}' WITH ("
                "FORMAT = 'CSV', FIELDQUOTE = '\"', FIRSTROW = 2, "
                "DATAFILETYPE = 'widechar', FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n', TABLOCK)"
            )
            return cur.rowcount
        cols = ", ".join(_q(c) for c in columns)
        sql = f"INSERT INTO {temp} ({cols}) VALUES ({', '.join('?' * len(columns))})"
        total = 0

        def send(batch):
            self._bind_types(cur, batch)
            cur.executemany(sql, batch)

        with open(csv_path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            next(reader)  # header
            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= self.CLIENT_BATCH_ROWS:
                    send(batch)
                    total += len(batch)
                    batch = []
            if batch:
                send(batch)
                total += len(batch)
        return total

    def _text_to_value(self, col: str, data_type: str | None = None) -> str:
        """SQL expression turning a #sre_load text column back into its value.

        SQL NULL arrives as NULL_TOKEN. BULK INSERT also loads a quoted empty
        string ("") as NULL, so with that method a NULL in #sre_load can only be
        an empty string; the client method reads "" as ''. Snowflake unloads
        BINARY as hex, which binary target columns decode with CONVERT style 2.
        """
        c = _q(col)
        if self.load_method == "bulk_insert":
            value = (f"CASE WHEN {c} IS NULL THEN N'' "
                     f"WHEN {c} = N'{NULL_TOKEN}' THEN NULL ELSE {c} END")
        else:
            value = f"NULLIF({c}, N'{NULL_TOKEN}')"
        if data_type in ("binary", "varbinary", "image"):
            return f"CONVERT(VARBINARY(MAX), NULLIF({c}, N'{NULL_TOKEN}'), 2)"
        return value

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

    def acquire_lock(self, name: str) -> bool:
        """Take an exclusive application lock without waiting. It is owned by
        the session, so it is held across commits until the connection closes
        (SQL Server sp_getapplock)."""
        cur = self._cursor()
        cur.execute("SET NOCOUNT ON; DECLARE @rc INT; "
                    "EXEC @rc = sp_getapplock @Resource = ?, @LockMode = 'Exclusive', "
                    "@LockOwner = 'Session', @LockTimeout = 0; SELECT @rc",
                    _lock_name(name, 255))
        rc = cur.fetchone()[0]
        cur.close()
        self._lock = _lock_name(name, 255) if rc >= 0 else None
        return self._lock is not None

    def truncate(self, table: str) -> None:
        cur = self._cursor()
        cur.execute(f"TRUNCATE TABLE {table}")   # transactional on SQL Server
        cur.close()

    def write(self, table, columns, rows, mode="append", key_columns=None) -> None:
        if not rows:
            return
        rows = self._exact_decimals(rows)
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
        key_rows = self._exact_decimals(key_rows)
        cur = self._cursor(fast=True)
        where = " AND ".join(f"{_q(k)} = ?" for k in key_columns)
        self._bind_types(cur, key_rows)
        cur.executemany(f"DELETE FROM {table} WHERE {where}", key_rows)
        cur.close()

    def bulk_load(self, table, csv_path, columns, mode="upsert", key_columns=None,
                  rel_path=None) -> None:
        """Transport B load. The CSV is loaded as text into #sre_load (BULK INSERT,
        or client-side batches with TARGET_MSSQL_LOAD_METHOD=client), then
        NULL_TOKEN is converted to NULL (see _text_to_value) and SQL Server
        converts the text to the target column types.

        Upserts convert into #sre_typed first, a temp table with the target's
        column types and a clustered index on the key, and MERGE from there:
        joining typed, ordered keys is about three times faster than merging
        straight from the text columns."""
        cur = self._cursor(fast=True)
        types = self._column_types(cur, table)
        self._text_temp(cur, "#sre_load", columns)
        n = self._fill_from_csv(cur, "#sre_load", columns, csv_path, rel_path)
        select = ", ".join(f"{self._text_to_value(c, types.get(c.lower()))} AS {_q(c)}"
                           for c in columns)
        cols = ", ".join(_q(c) for c in columns)
        if mode == "upsert":
            self._typed_temp(cur, "#sre_typed", table, columns)
            cur.execute("CREATE CLUSTERED INDEX sre_key ON #sre_typed ("
                        + ", ".join(_q(k) for k in key_columns) + ")")
            cur.execute(f"INSERT INTO #sre_typed ({cols}) SELECT {select} FROM #sre_load")
            cur.execute(self._merge_sql(table, "#sre_typed", columns, key_columns))
            cur.execute("DROP TABLE #sre_typed")
        else:
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
        # Release explicitly: with ODBC connection pooling, close() can return
        # the session to the pool instead of logging out, which would keep a
        # session-owned lock (tested: the lock outlived close()).
        if getattr(self, "_lock", None):
            try:
                cur = self._cursor()
                cur.execute("EXEC sp_releaseapplock @Resource = ?, @LockOwner = 'Session'",
                            self._lock)
                cur.close()
            except Exception:  # noqa: BLE001 - logging out releases it anyway
                pass
        self.conn.close()


def _lock_name(name: str, limit: int) -> str:
    """Lock name within the database's length limit (hashed if too long)."""
    import hashlib
    full = f"simple_reverse_etl:{name}"
    if len(full) <= limit:
        return full
    return "simple_reverse_etl:" + hashlib.sha256(name.encode()).hexdigest()[: limit - 19]


def make_target(cfg):
    if cfg.kind == "mysql":
        return MySQLTarget(cfg)
    if cfg.kind == "mssql":
        return MSSQLTarget(cfg)
    raise ValueError(f"Unknown target kind: {cfg.kind}")
