#!/usr/bin/env python3
"""Tiny Snowflake SQL runner for the demo (reuses the job's own connect()).

  python sf_exec.py --file  path/to/script.sql   # run all statements
  python sf_exec.py --scalar "SELECT MAX(...)"    # print one value
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import snowflake_source as sf          # noqa: E402
from config import SnowflakeConfig     # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file")
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
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
