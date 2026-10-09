#!/usr/bin/env python3
"""Value check for the Part 7 volume test.

Compares aggregates of SIMPLE_REVERSE_ETL_DEMO.DENTAL.VOLUME_CLAIMS in Snowflake
with each named target table: row count, key range, the four amount sums, rows
per status, NULL and text-length totals, the date range, and the emergency
count. A missed update changes the status counts, sums, or text length; lost
or truncated values change the NULL counts or text length.

  python demo/volume_check.py VOLUME_CLAIMS VOLUME_CLAIMS_STAGED

Run with demo/target_env.sh sourced. Exit status 0 when every table matches.
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake_source as sf                      # noqa: E402
from config import SnowflakeConfig, TargetConfig   # noqa: E402
from targets import make_target                    # noqa: E402

SRC = "SIMPLE_REVERSE_ETL_DEMO.DENTAL.VOLUME_CLAIMS"

# (label, Snowflake expression, MySQL expression, SQL Server expression)
METRICS = [
    ("rows", "COUNT(*)", "COUNT(*)", "COUNT_BIG(*)"),
    ("max CLAIM_ID", "MAX(CLAIM_ID)", "MAX(CLAIM_ID)", "MAX(CLAIM_ID)"),
    ("sum BILLED_AMOUNT", "SUM(BILLED_AMOUNT)", "SUM(BILLED_AMOUNT)", "SUM(BILLED_AMOUNT)"),
    ("sum ALLOWED_AMOUNT", "SUM(ALLOWED_AMOUNT)", "SUM(ALLOWED_AMOUNT)", "SUM(ALLOWED_AMOUNT)"),
    ("sum PAID_AMOUNT", "SUM(PAID_AMOUNT)", "SUM(PAID_AMOUNT)", "SUM(PAID_AMOUNT)"),
    ("sum MEMBER_RESP_AMOUNT", "SUM(MEMBER_RESP_AMOUNT)", "SUM(MEMBER_RESP_AMOUNT)",
     "SUM(MEMBER_RESP_AMOUNT)"),
    ("non-NULL DENIAL_REASON", "COUNT(DENIAL_REASON)", "COUNT(DENIAL_REASON)",
     "COUNT_BIG(DENIAL_REASON)"),
    ("non-NULL PAYER_NOTE", "COUNT(PAYER_NOTE)", "COUNT(PAYER_NOTE)", "COUNT_BIG(PAYER_NOTE)"),
    ("empty PAYER_NOTE", "COUNT_IF(PAYER_NOTE = '')", "SUM(PAYER_NOTE = '')",
     "SUM(CASE WHEN PAYER_NOTE = N'' THEN 1 ELSE 0 END)"),
    # DATALENGTH, not LEN: LEN ignores trailing spaces.
    ("total PAYER_NOTE length", "SUM(LENGTH(PAYER_NOTE))", "SUM(CHAR_LENGTH(PAYER_NOTE))",
     "SUM(CAST(DATALENGTH(PAYER_NOTE) AS BIGINT)) / 2"),
    ("NULL TOOTH_NUMBER", "COUNT_IF(TOOTH_NUMBER IS NULL)", "SUM(TOOTH_NUMBER IS NULL)",
     "SUM(CASE WHEN TOOTH_NUMBER IS NULL THEN 1 ELSE 0 END)"),
    ("emergencies", "COUNT_IF(IS_EMERGENCY)", "SUM(IS_EMERGENCY)",
     "SUM(CAST(IS_EMERGENCY AS INT))"),
    ("min SERVICE_DATE", "MIN(SERVICE_DATE)", "MIN(SERVICE_DATE)", "MIN(SERVICE_DATE)"),
    ("max SERVICE_DATE", "MAX(SERVICE_DATE)", "MAX(SERVICE_DATE)", "MAX(SERVICE_DATE)"),
]


def _norm(v):
    if v is None:
        return None
    if isinstance(v, (int, Decimal, float)):
        return Decimal(str(v)).normalize()
    return str(v)


def _aggregates(cur, table: str, column: int) -> dict:
    exprs = ", ".join(m[column] for m in METRICS)
    cur.execute(f"SELECT {exprs} FROM {table}")
    values = dict(zip((m[0] for m in METRICS), cur.fetchone()))
    cur.execute(f"SELECT CLAIM_STATUS, COUNT(*) FROM {table} GROUP BY CLAIM_STATUS")
    for status, n in cur.fetchall():
        values[f"status {status}"] = n
    return {k: _norm(v) for k, v in values.items()}


def main(tables: list[str]) -> int:
    tcfg = TargetConfig.from_env()
    column = 2 if tcfg.kind == "mysql" else 3
    conn = sf.connect(SnowflakeConfig.from_env())
    try:
        expected = _aggregates(conn.cursor(), SRC, 1)
    finally:
        conn.close()

    target = make_target(tcfg)
    ok = True
    try:
        for table in tables:
            actual = _aggregates(target.conn.cursor(), table, column)
            bad = [k for k in expected if actual.get(k) != expected[k]]
            bad += [k for k in actual if k not in expected]
            print(f"{'PASS' if not bad else 'FAIL'}  {table}: {int(expected['rows']):,} rows, "
                  f"{len(expected)} values compared with Snowflake")
            for k in bad:
                print(f"      {k}: Snowflake={expected.get(k)!r} target={actual.get(k)!r}")
            ok = ok and not bad
    finally:
        target.close()
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("usage: volume_check.py TABLE [TABLE ...]")
    sys.exit(main(sys.argv[1:]))
