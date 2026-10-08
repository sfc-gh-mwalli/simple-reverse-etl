#!/usr/bin/env python3
"""Tiny Snowflake SQL runner for the demo (reuses the job's own connect()).

  python sf_exec.py --file  path/to/script.sql   # run all statements
  python sf_exec.py --section S6                  # run one [Sn] section of
                                                  # snowsight_demo.sql
  python sf_exec.py --scalar "SELECT MAX(...)"    # print one value

--section lets run_demo.sh execute exactly what the presenter runs in Snowsight,
so the worksheet is the only copy of the demo's Snowflake steps.
"""
import argparse
import os
import re
import sys

DEMO = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(DEMO))

import snowflake_source as sf          # noqa: E402
from config import SnowflakeConfig     # noqa: E402

WORKSHEET = os.path.join(DEMO, "snowsight_demo.sql")


def worksheet_section(name: str) -> str:
    """The worksheet's USE statements plus the text of section [name]."""
    text = open(WORKSHEET).read()
    heads = list(re.finditer(r"^-- \[(S\d+)\]", text, re.M))
    for i, m in enumerate(heads):
        if m.group(1) == name:
            end = heads[i + 1].start() if i + 1 < len(heads) else len(text)
            preamble = "".join(re.findall(r"^USE [^;]+;\n", text[:heads[0].start()], re.M))
            return preamble + text[m.start():end]
    raise SystemExit(f"section [{name}] not found in {WORKSHEET}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file")
    ap.add_argument("--section")
    ap.add_argument("--scalar")
    args = ap.parse_args()

    conn = sf.connect(SnowflakeConfig.from_env())
    try:
        if args.scalar:
            cur = conn.cursor()
            cur.execute(args.scalar)
            row = cur.fetchone()
            cur.close()
            print("" if not row or row[0] is None else row[0])
        if args.file:
            with open(args.file) as fh:
                for _ in conn.execute_string(fh.read()):
                    pass
        if args.section:
            for _ in conn.execute_string(worksheet_section(args.section)):
                pass
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
