#!/usr/bin/env python3
"""Failure-and-recovery check for every change-capture mode and transport.

For each scenario (change capture x write mode x transport) and each injected
failure, the check:

  1. builds a small Snowflake source (plus stream and outbox) and loads it
     into a fresh target table with a clean run (the baseline);
  2. changes the source: inserts, an update and, where the mode can see them,
     a delete and an UPDATE that changes the key value;
  3. runs the job with one failure injected (see FAILURES) and records the
     exit code;
  4. runs the job again with no failure, then a third time;
  5. compares the target with the source.

Pass means: the failed run exits non-zero, both later runs exit 0, and the
target matches the source. (--change-capture hwm --mode append is rejected by
the job, because a retry of such a run inserted rows twice.)

  DEMO_TARGET=mysql ./demo/verify_recovery.sh
  DEMO_TARGET=mssql ./demo/verify_recovery.sh
  DEMO_TARGET=mssql TARGET_MSSQL_LOAD_METHOD=client ./demo/verify_recovery.sh

Options: --only <substring> runs matching scenarios; --keep leaves objects.
"""
from __future__ import annotations

import argparse
import contextlib
import os
import subprocess
import sys
import tempfile
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import change_capture                              # noqa: E402
import snowflake_source as sf                      # noqa: E402
import sync                                        # noqa: E402
import targets                                     # noqa: E402
from config import SnowflakeConfig, TargetConfig   # noqa: E402

DB = "SIMPLE_REVERSE_ETL_DEMO.DENTAL"
SRC, STREAM, OUTBOX = f"{DB}.RECOVERY_SRC", f"{DB}.RECOVERY_STREAM", f"{DB}.RECOVERY_OUTBOX"
STAGE = f"@{DB}.UNLOAD_STAGE"
TABLE = "RECOVERY_T"

# (change capture, mode). hwm cannot see deletes or key changes, so its
# scenarios change the source only with inserts and updates.
SCENARIOS = [("none", "truncate"), ("hwm", "upsert"), ("stream", "upsert")]
TRANSPORTS = ("pull", "unload")
FAILURES = ("read", "mid_write", "commit", "state", "kill")

TARGET_DDL = {
    "mysql": "CREATE TABLE {t} (ID BIGINT{pk}, V VARCHAR(50), UPDATED_AT DATETIME(6))",
    "mssql": "CREATE TABLE {t} (ID BIGINT NOT NULL{pk}, V NVARCHAR(50), UPDATED_AT DATETIME2(6))",
}


class Injected(RuntimeError):
    """The failure raised on purpose by this check."""


# --- Snowflake side -----------------------------------------------------------

def reset_source(cur) -> None:
    cur.execute(f"CREATE OR REPLACE TABLE {SRC} (ID NUMBER, V VARCHAR(50), UPDATED_AT TIMESTAMP_NTZ(6))")
    cur.execute(f"INSERT INTO {SRC} SELECT SEQ4() + 1, 'v1', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ "
                "FROM TABLE(GENERATOR(ROWCOUNT => 6))")
    cur.execute(f"CREATE OR REPLACE STREAM {STREAM} ON TABLE {SRC}")
    cur.execute(f"CREATE OR REPLACE TRANSIENT TABLE {OUTBOX} (ID NUMBER, V VARCHAR(50), "
                "UPDATED_AT TIMESTAMP_NTZ(6), _CDC_ACTION STRING, _CDC_ISUPDATE BOOLEAN, "
                "_CDC_LOADED_AT TIMESTAMP_LTZ, _CDC_EXPORTED BOOLEAN DEFAULT FALSE) "
                "DATA_RETENTION_TIME_IN_DAYS = 1")


def change_source(cur, cc: str) -> None:
    now = "CURRENT_TIMESTAMP()::TIMESTAMP_NTZ"
    cur.execute(f"INSERT INTO {SRC} VALUES (7, 'new', {now}), (8, 'new', {now})")
    cur.execute(f"UPDATE {SRC} SET V = 'updated', UPDATED_AT = {now} WHERE ID = 2")
    if cc != "hwm":
        cur.execute(f"DELETE FROM {SRC} WHERE ID = 3")
        cur.execute(f"UPDATE {SRC} SET ID = 40, V = 'rekeyed', UPDATED_AT = {now} WHERE ID = 4")


def source_rows(cur) -> list[tuple]:
    return sorted((int(i), v) for i, v in cur.execute(f"SELECT ID, V FROM {SRC}").fetchall())


# --- job invocation -------------------------------------------------------------

def job_argv(cc, mode, transport, state_file) -> list[str]:
    argv = ["--source", SRC, "--target", TABLE, "--transport", transport,
            "--change-capture", cc, "--mode", mode]
    if mode == "upsert" or cc == "stream":
        argv += ["--key-cols", "ID"]
    if cc == "hwm":
        argv += ["--hwm-col", "UPDATED_AT", "--state-file", state_file]
    if cc == "stream":
        argv += ["--stream", STREAM, "--outbox", OUTBOX]
    if transport == "unload":
        argv += ["--stage", STAGE, "--local-dir", "_unload_tmp"]
    elif cc != "stream":
        argv += ["--commit-rows", "2"]   # exercise intermediate commits
    return argv


def _small_batches(original):
    """Split each result batch into 2-row pieces, so a small test table behaves
    like a large one (several batches, intermediate commits)."""
    def wrapper(conn, query, params=None):
        for df in original(conn, query, params):
            for start in range(0, len(df), 2):
                yield df.iloc[start:start + 2]
    return wrapper


def _fail_on_call(n, original=None, after=False):
    """Raise Injected on call number n; with after=True, run the call first."""
    count = {"n": 0}

    def wrapper(*a, **kw):
        count["n"] += 1
        if count["n"] == n:
            if after and original:
                original(*a, **kw)
            raise Injected(f"injected failure on call {n}")
        return original(*a, **kw) if original else None
    return wrapper


def run_job(argv, transport, failure=None, target_cls=None) -> int:
    """Run sync.main in-process with `failure` injected (None: clean run)."""
    with contextlib.ExitStack() as stack:
        patch = stack.enter_context
        patch(mock.patch.object(sf, "read_query_batches", _small_batches(sf.read_query_batches)))
        if failure == "read":
            name = "read_query_batches" if transport == "pull" else "unload_to_stage"
            patch(mock.patch.object(sf, name, _fail_on_call(1)))
        elif failure == "mid_write":
            if transport == "pull":
                patch(mock.patch.object(target_cls, "write",
                                        _fail_on_call(2, target_cls.write)))
            else:
                patch(mock.patch.object(target_cls, "bulk_load",
                                        _fail_on_call(1, target_cls.bulk_load, after=True)))
        elif failure == "commit":
            patch(mock.patch.object(target_cls, "commit", _fail_on_call(1, target_cls.commit)))
        elif failure == "state":
            patch(mock.patch.object(change_capture, "save_state", _fail_on_call(1)))
            patch(mock.patch.object(sf, "mark_outbox_exported", _fail_on_call(1)))
        return sync.main(argv)


KILL_SCRIPT = r"""
import os, sys
sys.path.insert(0, {root!r})
import snowflake_source as sf, sync, targets
cls = {{"mysql": targets.MySQLTarget, "mssql": targets.MSSQLTarget}}[{kind!r}]
method = "write" if {transport!r} == "pull" else "bulk_load"
original = getattr(cls, method)
def write_then_die(self, *a, **kw):
    original(self, *a, **kw)
    os._exit(9)          # abrupt termination: no rollback, no close
setattr(cls, method, write_then_die)
sys.exit(sync.main({argv!r}))
"""


def run_killed(argv, transport, kind) -> int:
    """Run the job in a child process that dies right after its first write."""
    code = KILL_SCRIPT.format(root=ROOT, kind=kind, transport=transport, argv=argv)
    return subprocess.run([sys.executable, "-c", code], cwd=ROOT,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode


# --- target side ----------------------------------------------------------------

def recreate_target(target, kind) -> None:
    cur = target.conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    cur.execute(TARGET_DDL[kind].format(t=TABLE, pk=" PRIMARY KEY"))
    target.commit()


def target_rows(tcfg) -> list[tuple]:
    t = targets.make_target(tcfg)       # fresh connection: sees only committed data
    try:
        cur = t.conn.cursor()
        cur.execute(f"SELECT ID, V FROM {TABLE}")
        return sorted((int(i), v) for i, v in cur.fetchall())
    finally:
        t.close()


def compare(expected, actual) -> tuple[bool, str]:
    if actual == expected:
        return True, ""
    return False, f"expected {expected} got {actual}"


# --- driver ---------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--keep", action="store_true")
    opts = ap.parse_args()

    tcfg = TargetConfig.from_env()
    target_cls = {"mysql": targets.MySQLTarget, "mssql": targets.MSSQLTarget}[tcfg.kind]
    method = tcfg.mssql_load_method if tcfg.kind == "mssql" else "load data"
    print(f"Recovery check: target={tcfg.kind} (Transport B load: {method})")

    conn = sf.connect(SnowflakeConfig.from_env())
    cur = conn.cursor()
    admin = targets.make_target(tcfg)
    failures = passes = 0
    try:
        for cc, mode in SCENARIOS:
            for transport in TRANSPORTS:
                for failure in FAILURES:
                    name = f"{cc}/{mode}/{transport}/{failure}"
                    if opts.only not in name:
                        continue
                    if failure == "state" and cc == "none":
                        continue          # a full load saves no state
                    with tempfile.TemporaryDirectory() as tmp:
                        state = os.path.join(tmp, "state.json")
                        argv = job_argv(cc, mode, transport, state)
                        reset_source(cur)
                        recreate_target(admin, tcfg.kind)
                        # Baseline: stream mode starts from a full load.
                        base_argv = (job_argv("none", "truncate", transport, state)
                                     if cc == "stream" else argv)
                        rc_base = run_job(base_argv, transport)
                        change_source(cur, cc)
                        rc_fail = (run_killed(argv, transport, tcfg.kind) if failure == "kill"
                                   else run_job(argv, transport, failure, target_cls))
                        rc_retry = run_job(argv, transport)
                        rc_again = run_job(argv, transport)
                        ok, detail = compare(source_rows(cur), target_rows(tcfg))
                        codes_ok = rc_base == 0 and rc_fail != 0 and rc_retry == 0 and rc_again == 0
                        if not codes_ok:
                            ok = False
                            detail = (f"exit codes base={rc_base} failed={rc_fail} "
                                      f"retry={rc_retry} again={rc_again} {detail}")
                        passes += ok
                        failures += not ok
                        print(f"{'PASS' if ok else 'FAIL'}  {name:34} {detail}", flush=True)
    finally:
        if not opts.keep:
            c = admin.conn.cursor()
            c.execute(f"DROP TABLE IF EXISTS {TABLE}")
            admin.commit()
            for obj in ("STREAM " + STREAM, "TABLE " + OUTBOX, "TABLE " + SRC):
                cur.execute(f"DROP {obj.split()[0]} IF EXISTS {obj.split()[1]}")
            cur.execute(f"REMOVE {STAGE}/{TABLE.lower()}/")
        admin.close()
        conn.close()

    print(f"RESULT: {passes} passed, {failures} failed")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
