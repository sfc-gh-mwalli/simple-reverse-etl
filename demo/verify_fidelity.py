#!/usr/bin/env python3
"""Round-trip data-fidelity check for both transports against the demo target.

Creates a small Snowflake table of awkward values, loads it with
`--transport pull` and `--transport unload` into two target tables, reads both
back, and compares every value with Snowflake. Cleans up afterwards.

  DEMO_TARGET=mysql ./demo/verify_fidelity.sh     (wrapper; or source
  DEMO_TARGET=mssql ./demo/verify_fidelity.sh      demo/target_env.sh and run
                                                    python demo/verify_fidelity.py)

Exit status 0 when every value matches.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake_source as sf                      # noqa: E402
import sync                                        # noqa: E402
from config import SnowflakeConfig, TargetConfig   # noqa: E402
from targets import make_target                    # noqa: E402

SRC = "SIMPLE_REVERSE_ETL_DEMO.DENTAL.FIDELITY_TEST"
STAGE = "@SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE"
TARGETS = {"pull": "FIDELITY_PULL", "unload": "FIDELITY_UNLOAD"}

# (case name, TXT, AMT, TS) -- ID is assigned in order. TXT is also copied into
# NOTE, an unbounded text column (NVARCHAR(MAX) / TEXT), which drivers bind
# differently from a bounded one. FLAG is a BOOLEAN, alternating by ID.
CASES = [
    ("all NULL",            None, None, None),
    ("empty string, zero",  "''", "0", "'2026-10-08 16:43:51.247'"),
    ("NULL sentinel text",  "'__NULL__x'", "1.5", "'2026-06-15 12:00:00.000'"),
    ("non-ASCII",           "'Zoë Müller — São Paulo 東京 ✓'", "-1234.5678",
                            "'2026-01-01 00:00:00.001'"),
    ("quotes and commas",   "'has \"quotes\", commas, and ''apostrophes'''",
                            "99999999.9999", "'2026-12-31 23:59:59.999'"),
    ("embedded newline",    "'line one\nline two'", "0.0001", "'2026-06-15 12:00:00.500'"),
    ("leading/trailing ws", "'  padded  '", "-0.0001", "'2000-02-29 00:00:00.000'"),
]

TARGET_DDL = {
    "mysql": ("CREATE TABLE {t} (ID BIGINT PRIMARY KEY, TXT VARCHAR(100), "
              "AMT DECIMAL(12,4), TS DATETIME(3), NOTE TEXT, FLAG TINYINT(1)) DEFAULT CHARSET=utf8mb4"),
    "mssql": ("CREATE TABLE {t} (ID BIGINT NOT NULL PRIMARY KEY, TXT NVARCHAR(100), "
              "AMT DECIMAL(12,4), TS DATETIME2(3), NOTE NVARCHAR(MAX), FLAG BIT)"),
}


def _sql(v):
    return "NULL" if v is None else v


def main() -> int:
    tcfg = TargetConfig.from_env()
    method = tcfg.mssql_load_method if tcfg.kind == "mssql" else "load data"
    print(f"Fidelity check: target={tcfg.kind} (Transport B load: {method})")

    conn = sf.connect(SnowflakeConfig.from_env())
    cur = conn.cursor()
    cur.execute(f"CREATE OR REPLACE TABLE {SRC} "
                "(ID NUMBER NOT NULL, TXT VARCHAR(100), AMT NUMBER(12,4), TS TIMESTAMP_NTZ(3), NOTE VARCHAR, FLAG BOOLEAN)")
    values = ", ".join(f"({i}, {_sql(t)}, {_sql(a)}, {_sql(ts)})"
                       for i, (_, t, a, ts) in enumerate(CASES, 1))
    cur.execute(f"INSERT INTO {SRC} (ID, TXT, AMT, TS) VALUES {values}")
    # FLAG: a BOOLEAN (unloaded as true/false), NULL in the all-NULL case.
    cur.execute(f"UPDATE {SRC} SET NOTE = TXT, FLAG = IFF(ID = 1, NULL, MOD(ID, 2) = 0)")
    expected = {r[0]: r[1:] for r in cur.execute(
        f"SELECT ID, TXT, AMT, TS, NOTE, FLAG FROM {SRC} ORDER BY ID").fetchall()}

    target = make_target(tcfg)
    tc = target.conn.cursor()
    for t in TARGETS.values():
        tc.execute(f"DROP TABLE IF EXISTS {t}")
        tc.execute(TARGET_DDL[tcfg.kind].format(t=t))
    target.commit()

    failures = 0
    try:
        for transport, table in TARGETS.items():
            argv = ["--transport", transport, "--source", SRC, "--target", table,
                    "--change-capture", "none", "--mode", "truncate"]
            if transport == "unload":
                argv += ["--stage", STAGE, "--local-dir", "_unload_tmp"]
            if sync.main(argv) != 0:
                print(f"FAIL  {transport}: sync exited non-zero")
                failures += 1
                continue
            tc.execute(f"SELECT ID, TXT, AMT, TS, NOTE, FLAG FROM {table} ORDER BY ID")
            actual = {int(r[0]): tuple(r[1:]) for r in tc.fetchall()}
            target.commit()
            for i, (name, *_rest) in enumerate(CASES, 1):
                exp, got = tuple(expected[i]), actual.get(i)
                ok = got is not None and all(
                    (e is None and g is None) or (e is not None and g is not None and
                                                  (e == g if not isinstance(e, str) else e == str(g)))
                    for e, g in zip(exp, got))
                if not ok:
                    failures += 1
                print(f"{'PASS' if ok else 'FAIL'}  {transport:6} {name:22}"
                      + ("" if ok else f"  expected={exp!r} got={got!r}"))
    finally:
        for t in TARGETS.values():
            tc.execute(f"DROP TABLE IF EXISTS {t}")
        target.commit()
        target.close()
        cur.execute(f"DROP TABLE IF EXISTS {SRC}")
        cur.execute(f"REMOVE {STAGE}/fidelity_unload/")
        conn.close()

    print("RESULT:", "all values match" if failures == 0 else f"{failures} mismatch(es)")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
