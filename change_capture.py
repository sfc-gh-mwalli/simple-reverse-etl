"""Change capture: decide WHICH rows to send, independent of HOW they move.

Each planner returns a LoadPlan (or None when there is nothing to send). A
transport (transports.py) then moves the plan's rows into the target in one
target transaction, after which sync.py commits and calls plan.on_commit.

Change-capture modes
--------------------
  none    full table each run           (pair with --mode truncate)
  hwm     rows where <hwm-col> advanced  (needs a monotonic column; state file)
  stream  Snowflake stream -> outbox     (no source column needed; captures deletes)

High-water-mark (hwm) state
---------------------------
The watermark is the largest <hwm-col> value already delivered to the target.
It is kept in a small JSON file on the job host (--state-file, default
./sync_state.json), keyed by "<source> -> <target>" so one file can track many
source/target pairs:

  {"ANALYTICS.DENTAL.CLAIMS -> CLAIMS": {"watermark": "2026-10-08 15:09:23.237000"}}

Each run: (1) read the saved watermark, falling back to --hwm-start, or to
"no lower bound" if neither exists; (2) read MAX(<hwm-col>) from Snowflake as
the ceiling; (3) load rows with watermark < <hwm-col> <= ceiling; (4) write the
ceiling back to the file only after the target commit succeeds. A failed run
therefore leaves the watermark unchanged and the next run retries the same
window. With --transport pull, large loads commit every --commit-rows rows, so a
retry can re-send rows that were already committed; upsert mode makes that
harmless.

The file is local to one host. In production, keep it on durable storage, or
replace load_state/save_state with a control table or scheduler variable.
Deleting the file (or changing --state-file) causes a reload from --hwm-start.
State files written before keys included the target (keyed by source only) are
still read.

Stream state
------------
Stream mode keeps its position in Snowflake: the stream offset and the outbox
table. See plan_stream and "Stream-based change capture" in README.md.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import snowflake_source as sf

log = logging.getLogger("change_capture")


@dataclass
class LoadPlan:
    """What to send to the target in one run.

    upsert_query  rows to write with the run's --mode (full rows)
    delete_query  key columns of rows to delete (stream mode only)
    params        bind parameters for both queries
    truncate      empty the target before writing (--mode truncate)
    single_transaction
                  True: everything in one target transaction (stream mode).
                  False: the pull transport may commit every --commit-rows rows.
    on_commit     called after the target commit (save watermark / acknowledge)
    summary       formats the completion log line from (upserted, deleted)
    """
    upsert_query: str
    params: dict | None = None
    delete_query: str | None = None
    truncate: bool = False
    single_transaction: bool = False
    on_commit: Callable[[], None] = field(default=lambda: None)
    summary: Callable[[int, int], str] = field(default=lambda n, d: f"{n} rows")


# --- hwm state ---------------------------------------------------------------

def state_key(source: str, target: str) -> str:
    return f"{source} -> {target}"


def load_state(path: str) -> dict:
    """Watermark state: {"<source> -> <target>": {"watermark": value}}; {} if no file."""
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else {}


def save_state(path: str, state: dict) -> None:
    """Persist watermark state. Call only after the target commit succeeds."""
    Path(path).write_text(json.dumps(state, indent=2, default=str))


def _saved_watermark(state: dict, source: str, target: str):
    entry = state.get(state_key(source, target)) or state.get(source) or {}
    return entry.get("watermark")


# --- planners ----------------------------------------------------------------

def plan_full(sf_conn, args) -> LoadPlan:
    return LoadPlan(
        upsert_query=sf.build_full_query(args.source),
        truncate=args.mode == "truncate",
        summary=lambda n, d: f"Full load complete: {n} rows -> {args.target}",
    )


def plan_hwm(sf_conn, args) -> LoadPlan | None:
    """Incremental load of rows whose <hwm-col> advanced since the last run."""
    # 1. Last delivered value: saved state, else --hwm-start, else None (load all).
    state = load_state(args.state_file)
    last = _saved_watermark(state, args.source, args.target)
    if last is None:
        last = args.hwm_start
    # 2. Ceiling captured before reading, so rows updated mid-read wait for next run.
    ceiling = sf.scalar(sf_conn, f"SELECT MAX({args.hwm_col}) FROM {args.source}")
    if ceiling is None or (last is not None and str(ceiling) <= str(last)):
        log.info("No new rows above watermark %r (ceiling %r).", last, ceiling)
        return None
    log.info("HWM window: %r < %s <= %r", last, args.hwm_col, ceiling)

    def save_watermark():
        # 4. Advance the watermark only now that the target has committed.
        state.pop(args.source, None)            # drop a legacy source-only key
        state[state_key(args.source, args.target)] = {"watermark": ceiling}
        save_state(args.state_file, state)

    # 3. The transport reads the window and writes it to the target.
    return LoadPlan(
        upsert_query=sf.build_hwm_query(args.source, args.hwm_col,
                                        has_watermark=last is not None),
        params={"watermark": last, "ceiling": ceiling},
        truncate=args.mode == "truncate",
        on_commit=save_watermark,
        summary=lambda n, d: (f"HWM load complete: {n} rows, "
                              f"watermark advanced to {ceiling!r}"),
    )


def plan_stream(sf_conn, args) -> LoadPlan | None:
    """Stream change capture: stream -> outbox in Snowflake -> target.

    1. Consume the stream into the outbox (this commit advances the offset).
    2. Snapshot the newest un-exported outbox row (the cutoff); only rows up to
       it are handled in this run.
    3. The transport applies the net change per key (reduced in Snowflake):
       full rows to upsert and key columns to delete, in one target transaction.
    4. After the target commit, acknowledge rows up to the cutoff and purge
       delivered rows older than --outbox-retention-days.
    """
    sf.consume_stream_to_outbox(sf_conn, args.stream, args.outbox)
    cutoff = sf.outbox_cutoff(sf_conn, args.outbox)
    if cutoff is None:
        log.info("Stream CDC: no un-exported changes in %s", args.outbox)
        return None

    changes = sf.build_outbox_changes_query(args.outbox, args.key_cols)
    cdc = ", ".join(sf.CDC_COLUMNS)
    keys = ", ".join(args.key_cols)

    def acknowledge():
        sf.mark_outbox_exported(sf_conn, args.outbox, cutoff)
        sf.purge_outbox(sf_conn, args.outbox, args.outbox_retention_days)

    return LoadPlan(
        upsert_query=(f"SELECT * EXCLUDE ({cdc}) FROM ({changes}) "
                      f"WHERE _CDC_ACTION = 'INSERT'"),
        delete_query=f"SELECT {keys} FROM ({changes}) WHERE _CDC_ACTION = 'DELETE'",
        params={"cutoff": cutoff},
        single_transaction=True,
        on_commit=acknowledge,
        summary=lambda n, d: (f"Stream CDC complete: {n} upserts, {d} deletes "
                              f"-> {args.target}"),
    )


PLANNERS = {"none": plan_full, "hwm": plan_hwm, "stream": plan_stream}
