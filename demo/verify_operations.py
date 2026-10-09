#!/usr/bin/env python3
"""Checks for the job's operational features against the demo target:

  lock       a second run for the same target exits 3 while the first holds the lock
  query_tag  the job's Snowflake queries carry QUERY_TAG simple-reverse-etl:<target>
  timeout    SF_STATEMENT_TIMEOUT_SECONDS cancels a long Snowflake statement
  exit_codes invalid options exit 2 before connecting
  mysql_tls  (MySQL only) with TARGET_MYSQL_SSL_CA the server certificate is
             verified: the demo CA is accepted, a wrong CA is rejected

  DEMO_TARGET=mysql ./demo/verify_operations.sh
  DEMO_TARGET=mssql ./demo/verify_operations.sh
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import snowflake_source as sf                      # noqa: E402
import sync                                        # noqa: E402
import targets                                     # noqa: E402
from config import SnowflakeConfig, TargetConfig   # noqa: E402

DB = "SIMPLE_REVERSE_ETL_DEMO.DENTAL"
SRC, TABLE = f"{DB}.OPS_SRC", "OPS_T"
FULL = ["--source", SRC, "--target", TABLE, "--change-capture", "none", "--mode", "truncate"]


def check(name, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {name:12} {detail}", flush=True)
    return ok


def main() -> int:
    logging.basicConfig(level="CRITICAL")
    tcfg = TargetConfig.from_env()
    print(f"Operations checks: target={tcfg.kind}")
    conn = sf.connect(SnowflakeConfig.from_env())
    cur = conn.cursor()
    cur.execute(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, V VARCHAR)")
    cur.execute(f"INSERT INTO {SRC} VALUES (1, 'a')")
    admin = targets.make_target(tcfg)
    c = admin.conn.cursor()
    c.execute(f"DROP TABLE IF EXISTS {TABLE}")
    c.execute(f"CREATE TABLE {TABLE} (ID BIGINT NOT NULL PRIMARY KEY, V VARCHAR(20))")
    admin.commit()
    results = []
    try:
        # lock: hold it from another connection, then run the job.
        holder = targets.make_target(tcfg)
        held = holder.acquire_lock(TABLE)
        rc_locked = sync.main(FULL)
        holder.close()
        rc_free = sync.main(FULL)
        results.append(check("lock", held and rc_locked == sync.EXIT_LOCKED and rc_free == 0,
                             f"held={held} exit while held={rc_locked} after release={rc_free}"))

        # query_tag: the run above tagged its queries.
        cur.execute("SELECT COUNT(*) FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER("
                    "RESULT_LIMIT => 200)) WHERE QUERY_TAG = %s", (f"simple-reverse-etl:{TABLE}",))
        tagged = cur.fetchone()[0]
        results.append(check("query_tag", tagged > 0, f"{tagged} tagged queries in recent history"))

        # timeout: a session opened with a 2-second statement timeout.
        os.environ["SF_STATEMENT_TIMEOUT_SECONDS"] = "2"
        try:
            t = sf.connect(SnowflakeConfig.from_env())
            try:
                t.cursor().execute("CALL SYSTEM$WAIT(5)")
                msg = "not cancelled"
            except Exception as exc:  # noqa: BLE001
                msg = str(exc).splitlines()[-1]
            t.close()
        finally:
            del os.environ["SF_STATEMENT_TIMEOUT_SECONDS"]
        results.append(check("timeout", "timeout of 2 second" in msg, msg[:90]))

        rc = sync.main(["--source", "X", "--target", "T; DROP TABLE T"])
        results.append(check("exit_codes", rc == sync.EXIT_USAGE, f"invalid name exit {rc}"))

        if tcfg.kind == "mysql":
            results.append(check_mysql_tls(tcfg))
    finally:
        c = admin.conn.cursor()
        c.execute(f"DROP TABLE IF EXISTS {TABLE}")
        admin.commit()
        admin.close()
        cur.execute(f"DROP TABLE IF EXISTS {SRC}")
        conn.close()
    print("RESULT:", "all passed" if all(results) else "failures")
    return 0 if all(results) else 1


def check_mysql_tls(tcfg) -> bool:
    """The demo MySQL container generates its own CA (ca.pem in the data dir);
    its server certificate is not issued for 127.0.0.1, so identity checking is
    turned off and only the certificate chain is verified."""
    tmp = tempfile.mkdtemp()
    good = os.path.join(tmp, "ca.pem")
    subprocess.run(["docker", "cp", "simple-reverse-etl-demo-mysql:/var/lib/mysql/ca.pem", good],
                   check=True, capture_output=True)
    wrong = os.path.join(tmp, "wrong.pem")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout",
                    os.path.join(tmp, "k.pem"), "-out", wrong, "-days", "1", "-subj", "/CN=wrong"],
                   check=True, capture_output=True)
    outcome = {}
    for label, ca in (("demo CA", good), ("wrong CA", wrong)):
        tcfg.mysql_ssl_ca, tcfg.mysql_ssl_verify_identity = ca, False
        try:
            t = targets.make_target(tcfg)
            with t.conn.cursor() as cur:
                cur.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
                outcome[label] = f"connected ({cur.fetchone()[1]})"
            t.close()
        except Exception as exc:  # noqa: BLE001
            outcome[label] = f"rejected ({type(exc).__name__})"
    tcfg.mysql_ssl_ca = None
    ok = outcome["demo CA"].startswith("connected") and outcome["wrong CA"].startswith("rejected")
    return check("mysql_tls", ok, f"demo CA: {outcome['demo CA']}; wrong CA: {outcome['wrong CA']}")


if __name__ == "__main__":
    sys.exit(main())
