#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Part 7 of the runbook: volume test on both transports.
#
#   1. [S9]  create VOLUME_CLAIMS in Snowflake: 10M wide rows
#   2. initial load, timed: Transport A -> VOLUME_CLAIMS,
#                           Transport B -> VOLUME_CLAIMS_STAGED
#   3. [S10] insert 5M claims and update 1M existing claims
#   4. incremental load of the 6M changed rows, timed, on both transports
# After steps 2 and 4, demo/volume_check.py compares both target tables with
# Snowflake. Prints a timing summary; exits non-zero on any mismatch.
#
#   ./demo/run_volume.sh                                          # MySQL
#   DEMO_TARGET=mssql ./demo/run_volume.sh                        # SQL Server, BULK INSERT
#   DEMO_TARGET=mssql TARGET_MSSQL_LOAD_METHOD=client ./demo/run_volume.sh
#
# Expect several minutes on MySQL and longer on SQL Server under emulation.
# Transport B needs free disk in _unload_tmp of roughly 3x the unloaded data
# (compressed and plain files, plus a UTF-16 copy for SQL Server BULK INSERT).
# ---------------------------------------------------------------------------
set -euo pipefail

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$DEMO")"
# shellcheck source=demo/target_env.sh
source "$DEMO/target_env.sh"
cd "$ROOT"

STAGE="@SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE"
SRC="SIMPLE_REVERSE_ETL_DEMO.DENTAL.VOLUME_CLAIMS"
HWM=(--source "$SRC" --change-capture hwm --hwm-col UPDATED_AT
     --mode upsert --key-cols CLAIM_ID)
A=(python sync.py "${HWM[@]}" --target VOLUME_CLAIMS
   --state-file sync_state_volume_a.json)
B=(python sync.py --transport unload "${HWM[@]}" --target VOLUME_CLAIMS_STAGED
   --state-file sync_state_volume_b.json --stage "$STAGE" --local-dir _unload_tmp)
LOG="$(mktemp -t simple_reverse_etl_volume)"
SUMMARY=()

banner() { printf '\n=== %s ===\n' "$*"; }
now() { python -c 'import time; print(f"{time.time():.1f}")'; }

timed() {   # timed <label> <command...>: run, log, record rows and seconds
    local label="$1"; shift
    local start end rows
    start="$(now)"
    "$@" 2>&1 | tee "$LOG"
    end="$(now)"
    rows="$(sed -nE 's/.*HWM load complete: ([0-9]+) rows.*/\1/p' "$LOG" | tail -1)"
    LAST_ROWS="${rows:-0}"
    SUMMARY+=("$(python -c "
r, s = int('${rows:-0}'), $end - $start
print(f'{\"$label\":<34} {r:>11,} {s:>9.1f} {r / s if s else 0:>11,.0f}')")")
}

banner "0. Prepare $DEMO_TARGET${TARGET_MSSQL_LOAD_METHOD:+ ($TARGET_MSSQL_LOAD_METHOD)}"
demo_start_target
demo_init_target
"$DEMO/target_query.sh" "TRUNCATE TABLE VOLUME_CLAIMS; TRUNCATE TABLE VOLUME_CLAIMS_STAGED;" >/dev/null
rm -f sync_state_volume_a.json sync_state_volume_b.json

banner "1. [S9] Create 10M rows in Snowflake"
python "$DEMO/sf_exec.py" --section S9

banner "2. Initial load (10M rows)"
timed "Initial load, Transport A" "${A[@]}"
timed "Initial load, Transport B" "${B[@]}"
python "$DEMO/volume_check.py" VOLUME_CLAIMS VOLUME_CLAIMS_STAGED

banner "3. [S10] Insert 5M claims, update 1M claims"
python "$DEMO/sf_exec.py" --section S10

banner "4. Incremental load (6M changed rows)"
for t in A B; do
    if [ "$t" = A ]; then timed "Incremental load, Transport A" "${A[@]}"
    else timed "Incremental load, Transport B" "${B[@]}"; fi
    # hwm must select exactly the changed rows: 5M inserts + 1M updates.
    [ "$LAST_ROWS" = 6000000 ] || { echo "Transport $t sent $LAST_ROWS rows, expected 6000000" >&2; exit 1; }
done
python "$DEMO/volume_check.py" VOLUME_CLAIMS VOLUME_CLAIMS_STAGED

banner "Timing summary ($DEMO_TARGET${TARGET_MSSQL_LOAD_METHOD:+, $TARGET_MSSQL_LOAD_METHOD})"
printf '%-34s %11s %9s %11s\n' "Step" "Rows" "Seconds" "Rows/s"
printf '%s\n' "${SUMMARY[@]}"
rm -f "$LOG"
