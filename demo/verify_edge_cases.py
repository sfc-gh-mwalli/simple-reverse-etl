#!/usr/bin/env python3
"""Edge-case probes: record what actually happens, case by case.

Each probe sets up a small Snowflake source and target table, runs the job,
and prints what it observed. These are characterization tests: they record
behavior so the README can state it accurately. A probe FAILs only when the
observation differs from the behavior the README documents (EXPECT below).

  DEMO_TARGET=mysql ./demo/verify_edge_cases.sh
  DEMO_TARGET=mssql ./demo/verify_edge_cases.sh
  DEMO_TARGET=mssql TARGET_MSSQL_LOAD_METHOD=client ./demo/verify_edge_cases.sh

Options: --only <substring> runs matching probes.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import snowflake_source as sf                      # noqa: E402
import sync                                        # noqa: E402
import targets                                     # noqa: E402
from config import SnowflakeConfig, TargetConfig   # noqa: E402

DB = "SIMPLE_REVERSE_ETL_DEMO.DENTAL"
SRC, VIEW = f"{DB}.EDGE_SRC", f"{DB}.EDGE_VIEW"
STAGE = f"@{DB}.UNLOAD_STAGE"
TABLE = "EDGE_T"
NOW = "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"


class Probe:
    def __init__(self, tcfg, sf_conn, transport):
        self.tcfg, self.kind, self.sf, self.transport = tcfg, tcfg.kind, sf_conn, transport
        self.state = os.path.join(tempfile.mkdtemp(), "state.json")

    # Snowflake
    def sql(self, *stmts):
        cur = self.sf.cursor()
        for s in stmts:
            cur.execute(s)
        return cur

    # target
    def target_exec(self, *stmts):
        t = targets.make_target(self.tcfg)
        try:
            cur = t.conn.cursor()
            for s in stmts:
                cur.execute(s)
            t.commit()
        finally:
            t.close()

    def target_rows(self, cols="*", order="1"):
        t = targets.make_target(self.tcfg)
        try:
            cur = t.conn.cursor()
            cur.execute(f"SELECT {cols} FROM {TABLE} ORDER BY {order}")
            return [tuple(r) for r in cur.fetchall()]
        finally:
            t.close()

    def ddl(self, mysql, mssql):
        self.target_exec(f"DROP TABLE IF EXISTS {TABLE}",
                         (mysql if self.kind == "mysql" else mssql).format(t=TABLE))

    def run(self, *extra, source=SRC):
        argv = ["--source", source, "--target", TABLE, "--transport", self.transport, *extra]
        if self.transport == "unload":
            argv += ["--stage", STAGE, "--local-dir", "_unload_tmp"]
        if "hwm" in extra:
            argv += ["--state-file", self.state]
        return sync.main(argv)


# --- probes -----------------------------------------------------------------------
# Each returns (ok, observation). ok compares with what the README documents.

def probe_null_hwm(p):
    """Rows whose --hwm-col is NULL: README says they are never selected."""
    p.sql(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, UPDATED_AT TIMESTAMP_NTZ)",
          f"INSERT INTO {SRC} VALUES (1, {NOW}), (2, NULL)")
    p.ddl("CREATE TABLE {t} (ID BIGINT PRIMARY KEY, UPDATED_AT DATETIME(6))",
          "CREATE TABLE {t} (ID BIGINT NOT NULL PRIMARY KEY, UPDATED_AT DATETIME2(6))")
    rc = p.run("--change-capture", "hwm", "--hwm-col", "UPDATED_AT", "--mode", "upsert",
               "--key-cols", "ID")
    ids = [r[0] for r in p.target_rows("ID")]
    return rc == 0 and ids == [1], f"exit {rc}; target IDs {ids} (row 2 has NULL UPDATED_AT)"


def probe_late_commit(p):
    """A row stamped before the watermark but committed after it is missed.
    Session W (a writer) inserts with UPDATED_AT = CURRENT_TIMESTAMP inside an
    open transaction; another row commits later; the job runs; then W commits."""
    p.sql(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, UPDATED_AT TIMESTAMP_NTZ)",
          f"INSERT INTO {SRC} VALUES (1, {NOW})")
    p.ddl("CREATE TABLE {t} (ID BIGINT PRIMARY KEY, UPDATED_AT DATETIME(6))",
          "CREATE TABLE {t} (ID BIGINT NOT NULL PRIMARY KEY, UPDATED_AT DATETIME2(6))")
    hwm = ("--change-capture", "hwm", "--hwm-col", "UPDATED_AT", "--mode", "upsert",
           "--key-cols", "ID")
    p.run(*hwm)
    writer = sf.connect(SnowflakeConfig.from_env())
    try:
        w = writer.cursor()
        w.execute("BEGIN")
        w.execute(f"INSERT INTO {SRC} VALUES (2, {NOW})")          # stamped now, not committed
        p.sql("CALL SYSTEM$WAIT(1)", f"INSERT INTO {SRC} VALUES (3, {NOW})")   # committed, later stamp
        p.run(*hwm)                                                # watermark -> row 3's stamp
        w.execute("COMMIT")
    finally:
        writer.close()
    rc = p.run(*hwm)
    ids = [r[0] for r in p.target_rows("ID")]
    return ids == [1, 3], f"exit {rc}; target IDs {ids}; source IDs [1, 2, 3]"


def probe_duplicate_keys(p):
    """Two source rows share a --key-cols value (Snowflake does not enforce it)."""
    p.sql(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, V VARCHAR)",
          f"INSERT INTO {SRC} VALUES (1, 'first'), (1, 'second'), (2, 'other')")
    p.ddl("CREATE TABLE {t} (ID BIGINT PRIMARY KEY, V VARCHAR(20))",
          "CREATE TABLE {t} (ID BIGINT NOT NULL PRIMARY KEY, V NVARCHAR(20))")
    out = []
    for mode in ("truncate", "upsert"):
        rc = p.run("--change-capture", "none", "--mode", mode, "--key-cols", "ID")
        out.append(f"{mode}: exit {rc}, target {p.target_rows('ID, V')}")
    return True, "; ".join(out)


def probe_float(p):
    """FLOAT values: docs say CSV unload truncates to about (15,9)."""
    p.sql(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, F FLOAT)",
          f"INSERT INTO {SRC} VALUES (1, 0.1234567890123456789), (2, 1234567.123456789012), "
          f"(3, 1.5e-12), (4, 3.141592653589793)")
    p.ddl("CREATE TABLE {t} (ID BIGINT PRIMARY KEY, F DOUBLE)",
          "CREATE TABLE {t} (ID BIGINT NOT NULL PRIMARY KEY, F FLOAT(53))")
    rc = p.run("--change-capture", "none", "--mode", "truncate")
    src = {int(i): f for i, f in p.sql(f"SELECT ID, F FROM {SRC}").fetchall()}
    got = {int(i): f for i, f in p.target_rows("ID, F")}
    diffs = {i: (src[i], got.get(i)) for i in src if src[i] != got.get(i)}
    return rc == 0, f"exit {rc}; differing (source, target): {diffs or 'none'}"


def probe_view_source(p):
    """--source can be a view (the job runs SELECT * FROM <source>)."""
    p.sql(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, V VARCHAR, UPDATED_AT TIMESTAMP_NTZ)",
          f"INSERT INTO {SRC} VALUES (1, 'a', {NOW}), (2, 'b', {NOW})",
          f"CREATE OR REPLACE VIEW {VIEW} AS SELECT ID, UPPER(V) AS V, UPDATED_AT FROM {SRC}")
    p.ddl("CREATE TABLE {t} (ID BIGINT PRIMARY KEY, V VARCHAR(20), UPDATED_AT DATETIME(6))",
          "CREATE TABLE {t} (ID BIGINT NOT NULL PRIMARY KEY, V NVARCHAR(20), UPDATED_AT DATETIME2(6))")
    rc1 = p.run("--change-capture", "none", "--mode", "truncate", source=VIEW)
    p.sql(f"INSERT INTO {SRC} VALUES (3, 'c', {NOW})")
    rc2 = p.run("--change-capture", "hwm", "--hwm-col", "UPDATED_AT", "--mode", "upsert",
                "--key-cols", "ID", source=VIEW)
    rows = p.target_rows("ID, V")
    ok = rc1 == 0 and rc2 == 0 and rows == [(1, "A"), (2, "B"), (3, "C")]
    return ok, f"exit {rc1}/{rc2}; target {rows}"


def probe_target_extra_columns(p):
    """Target has an IDENTITY/AUTO_INCREMENT surrogate and a defaulted column
    that the source does not have."""
    p.sql(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, V VARCHAR)",
          f"INSERT INTO {SRC} VALUES (1, 'a'), (2, 'b')")
    p.ddl("CREATE TABLE {t} (SK BIGINT AUTO_INCREMENT PRIMARY KEY, ID BIGINT UNIQUE, V VARCHAR(20), "
          "LOADED_AT DATETIME DEFAULT CURRENT_TIMESTAMP)",
          "CREATE TABLE {t} (SK BIGINT IDENTITY PRIMARY KEY, ID BIGINT UNIQUE, V NVARCHAR(20), "
          "LOADED_AT DATETIME2 DEFAULT SYSDATETIME())")
    out = []
    for mode in ("truncate", "upsert"):
        rc = p.run("--change-capture", "none", "--mode", mode, "--key-cols", "ID")
        rows = p.target_rows("ID, V, CASE WHEN SK IS NULL OR LOADED_AT IS NULL THEN 0 ELSE 1 END", "ID")
        out.append(f"{mode}: exit {rc}, rows {rows}")
    return True, "; ".join(out)


def probe_identity_key(p):
    """The target key itself is an IDENTITY / AUTO_INCREMENT column."""
    p.sql(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, V VARCHAR)",
          f"INSERT INTO {SRC} VALUES (10, 'a'), (20, 'b')")
    p.ddl("CREATE TABLE {t} (ID BIGINT AUTO_INCREMENT PRIMARY KEY, V VARCHAR(20))",
          "CREATE TABLE {t} (ID BIGINT IDENTITY PRIMARY KEY, V NVARCHAR(20))")
    out = []
    for mode in ("truncate", "upsert"):
        rc = p.run("--change-capture", "none", "--mode", mode, "--key-cols", "ID")
        out.append(f"{mode}: exit {rc}, rows {p.target_rows('ID, V')}")
    return True, "; ".join(out)


def probe_case_keys(p):
    """Keys that differ only by letter case, with the target's default collation
    and with a case-sensitive one on the key column."""
    p.sql(f"CREATE OR REPLACE TABLE {SRC} (K VARCHAR, V VARCHAR)",
          f"INSERT INTO {SRC} VALUES ('abc', 'lower'), ('ABC', 'upper')")
    out = []
    for label, mysql_k, mssql_k in (("default collation", "", ""),
                                    ("case-sensitive key", " COLLATE utf8mb4_bin",
                                     " COLLATE Latin1_General_100_BIN2")):
        p.ddl(f"CREATE TABLE {{t}} (K VARCHAR(20){mysql_k} PRIMARY KEY, V VARCHAR(20))",
              f"CREATE TABLE {{t}} (K NVARCHAR(20){mssql_k} NOT NULL PRIMARY KEY, V NVARCHAR(20))")
        for mode in ("truncate", "upsert"):
            rc = p.run("--change-capture", "none", "--mode", mode, "--key-cols", "K")
            out.append(f"{label} {mode}: exit {rc}, rows {p.target_rows('K, V', 'V')}")
    return True, "; ".join(out)


def probe_late_commit_stream(p):
    """The late-commit case of probe_late_commit, in stream mode."""
    stream, outbox = f"{DB}.EDGE_STREAM", f"{DB}.EDGE_OUTBOX"
    p.sql(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, UPDATED_AT TIMESTAMP_NTZ)",
          f"INSERT INTO {SRC} VALUES (1, {NOW})",
          f"CREATE OR REPLACE STREAM {stream} ON TABLE {SRC}",
          f"CREATE OR REPLACE TRANSIENT TABLE {outbox} (ID NUMBER, UPDATED_AT TIMESTAMP_NTZ, "
          "_CDC_ACTION STRING, _CDC_ISUPDATE BOOLEAN, _CDC_LOADED_AT TIMESTAMP_LTZ, "
          "_CDC_EXPORTED BOOLEAN DEFAULT FALSE)")
    p.ddl("CREATE TABLE {t} (ID BIGINT PRIMARY KEY, UPDATED_AT DATETIME(6))",
          "CREATE TABLE {t} (ID BIGINT NOT NULL PRIMARY KEY, UPDATED_AT DATETIME2(6))")
    cdc = ("--change-capture", "stream", "--stream", stream, "--outbox", outbox,
           "--mode", "upsert", "--key-cols", "ID")
    try:
        p.run("--change-capture", "none", "--mode", "truncate")
        writer = sf.connect(SnowflakeConfig.from_env())
        try:
            w = writer.cursor()
            w.execute("BEGIN")
            w.execute(f"INSERT INTO {SRC} VALUES (2, {NOW})")
            p.sql("CALL SYSTEM$WAIT(1)", f"INSERT INTO {SRC} VALUES (3, {NOW})")
            p.run(*cdc)
            w.execute("COMMIT")
        finally:
            writer.close()
        rc = p.run(*cdc)
    finally:
        p.sql(f"DROP STREAM IF EXISTS {stream}", f"DROP TABLE IF EXISTS {outbox}")
    ids = [r[0] for r in p.target_rows("ID")]
    return ids == [1, 2, 3], f"exit {rc}; target IDs {ids}; source IDs [1, 2, 3]"


# Data types: (name, Snowflake type, literal, MySQL type, SQL Server type).
# SOURCE_TEXT shows the Snowflake value as text for the report.
SOURCE_TEXT = {"BINARY": "HEX_ENCODE(X)", "GEOGRAPHY": "ST_ASWKT(X)"}
TYPES = [
    ("NUMBER(38,0)",  "NUMBER(38,0)",  "12345678901234567890123456789012345678",
                      "DECIMAL(38,0)", "DECIMAL(38,0)"),
    ("NUMBER(18,6)",  "NUMBER(18,6)",  "-123456789012.123456", "DECIMAL(18,6)", "DECIMAL(18,6)"),
    ("TIME",          "TIME(6)",       "'13:14:15.123456'", "TIME(6)", "TIME(6)"),
    ("DATE",          "DATE",          "'0001-01-01'", "DATE", "DATE"),
    ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ(6)", "'2026-03-08 02:30:00.123456'", "DATETIME(6)", "DATETIME2(6)"),
    ("TIMESTAMP_LTZ", "TIMESTAMP_LTZ(6)", "'2026-07-01 12:00:00.123456 -0700'", "DATETIME(6)", "DATETIME2(6)"),
    ("TIMESTAMP_TZ",  "TIMESTAMP_TZ(6)",  "'2026-07-01 12:00:00.123456 +0530'", "DATETIME(6)", "DATETIMEOFFSET(6)"),
    ("BINARY",        "BINARY",        "TO_BINARY('DEADBEEF00', 'HEX')", "VARBINARY(20)", "VARBINARY(20)"),
    ("VARIANT",       "VARIANT",       "PARSE_JSON('{\"a\": [1, 2.5, \"x\"], \"b\": null}')",
                      "JSON", "NVARCHAR(MAX)"),
    ("ARRAY",         "ARRAY",         "ARRAY_CONSTRUCT(1, 'two', NULL)", "JSON", "NVARCHAR(MAX)"),
    ("OBJECT",        "OBJECT",        "OBJECT_CONSTRUCT('k', 'v')", "JSON", "NVARCHAR(MAX)"),
    ("GEOGRAPHY",     "GEOGRAPHY",     "TO_GEOGRAPHY('POINT(-122.35 37.55)')", "TEXT", "NVARCHAR(MAX)"),
    ("VARCHAR 5000",  "VARCHAR",       "REPEAT('x', 5000)", "TEXT", "NVARCHAR(MAX)"),
]


def probe_types(p):
    lines, ok = [], True
    for name, sf_type, literal, my_type, ms_type in TYPES:
        p.sql(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, X {sf_type})",
              f"INSERT INTO {SRC} SELECT 1, {literal}")
        p.ddl(f"CREATE TABLE {{t}} (ID BIGINT PRIMARY KEY, X {my_type})",
              f"CREATE TABLE {{t}} (ID BIGINT NOT NULL PRIMARY KEY, X {ms_type})")
        src = p.sql(f"SELECT {SOURCE_TEXT.get(name, 'X::VARCHAR')} FROM {SRC}").fetchone()[0]
        try:
            rc = p.run("--change-capture", "none", "--mode", "truncate")
            # pyodbc cannot read DATETIMEOFFSET; read it as ISO text instead.
            col = ("CONVERT(NVARCHAR(40), X, 127)" if p.kind == "mssql" and ms_type.startswith("DATETIMEOFFSET")
                   else "X")
            got = p.target_rows(col)
            got = got[0][0] if got else "<no row>"
        except Exception as exc:          # noqa: BLE001 - record any failure
            rc, got = "exception", f"{type(exc).__name__}: {exc}"
        shown = got.hex() if isinstance(got, (bytes, bytearray)) else got
        shown = (str(shown)[:70] + "...") if len(str(shown)) > 70 else shown
        src_shown = (src[:70] + "...") if src and len(src) > 70 else src
        lines.append(f"    {name:14} exit {rc}  source={src_shown!r}  target={shown!r}")
        ok = ok and rc in (0, 1)
    return ok, "\n" + "\n".join(lines)


PROBES = [probe_null_hwm, probe_late_commit, probe_late_commit_stream, probe_duplicate_keys,
          probe_float, probe_view_source, probe_target_extra_columns, probe_identity_key,
          probe_case_keys, probe_types]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    opts = ap.parse_args()
    logging.basicConfig(level="CRITICAL")
    tcfg = TargetConfig.from_env()
    method = tcfg.mssql_load_method if tcfg.kind == "mssql" else "load data"
    print(f"Edge-case probes: target={tcfg.kind} (Transport B load: {method})")
    conn = sf.connect(SnowflakeConfig.from_env())
    failed = 0
    try:
        for probe in PROBES:
            for transport in ("pull", "unload"):
                name = f"{probe.__name__[6:]}/{transport}"
                if opts.only not in name:
                    continue
                try:
                    ok, obs = probe(Probe(tcfg, conn, transport))
                except Exception as exc:  # noqa: BLE001
                    ok, obs = False, f"probe error {type(exc).__name__}: {exc}"
                failed += not ok
                print(f"{'OK  ' if ok else 'FAIL'}  {name:26} {obs}", flush=True)
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP VIEW IF EXISTS {VIEW}")
        cur.execute(f"DROP TABLE IF EXISTS {SRC}")
        cur.execute(f"REMOVE {STAGE}/{TABLE.lower()}/")
        conn.close()
        t = targets.make_target(tcfg)
        t.conn.cursor().execute(f"DROP TABLE IF EXISTS {TABLE}")
        t.commit()
        t.close()
    print("RESULT:", "all observations as documented" if not failed else f"{failed} unexpected")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
