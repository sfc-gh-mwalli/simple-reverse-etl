#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Unattended end-to-end demo: Snowflake -> local MySQL or SQL Server.
#   Source: SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS
#
#   Transport A - connector pull, hwm           sync.py                     -> DENTAL_CLAIMS
#   Transport B - unload -> stage -> bulk load  sync.py --transport unload  -> DENTAL_CLAIMS_STAGED
#   Stream CDC  - Transport A, then B           both                        -> DENTAL_CLAIMS_CDC
#
# Watermark part: a full load (20 rows), then after 2 updates + 3 inserts + 1 delete
# a delta upsert (-> 23 rows; the deleted claim 10 remains) with each transport;
# both tables must end identical.
# Stream part: create the stream and outbox, full load (22 rows), then a delete +
# update through the stream with Transport A (-> 21 rows, claim 5 gone), then an
# insert + update with Transport B (-> 22 rows); the target must match Snowflake.
# Snowflake steps run the same [Sn] sections of snowsight_demo.sql the presenter uses.
#
#   ./demo/run_demo.sh                                   # MySQL
#   DEMO_TARGET=mssql ./demo/run_demo.sh                 # SQL Server, BULK INSERT
#   DEMO_TARGET=mssql TARGET_MSSQL_LOAD_METHOD=client ./demo/run_demo.sh
#
# Prereq: Docker running and demo/.env.demo filled in (see demo/README.md).
# ---------------------------------------------------------------------------
set -euo pipefail

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$DEMO")"
SRC="SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS"
STREAM="SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_STREAM"
OUTBOX="SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_OUTBOX"
STAGE="@SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE"
STATE_A="$ROOT/sync_state.json"          # Transport A watermark
STATE_B="$ROOT/sync_state_unload.json"   # Transport B watermark
Q="$DEMO/target_query.sh"

banner() { printf '\n\033[1;36m=== %s ===\033[0m\n' "$1"; }
fail() { echo "VERIFICATION FAILED: $1" >&2; exit 1; }
count() { "$Q" "SELECT COUNT(*) AS n FROM $1;" | tail -1 | tr -d '[:space:]'; }

banner "0. Reset ($(printf '%s' "${DEMO_TARGET:-mysql}"))"
"$DEMO/reset_demo.sh"
# shellcheck source=demo/target_env.sh
source "$DEMO/target_env.sh"
cd "$ROOT"

A_HWM=(python sync.py --source "$SRC" --target DENTAL_CLAIMS
       --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID
       --state-file "$STATE_A")
B_HWM=(python sync.py --transport unload --source "$SRC" --target DENTAL_CLAIMS_STAGED
       --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID
       --stage "$STAGE" --state-file "$STATE_B" --local-dir _unload_tmp)
STREAM_ARGS=(--source "$SRC" --target DENTAL_CLAIMS_CDC --change-capture stream
             --stream "$STREAM" --outbox "$OUTBOX" --mode upsert --key-cols CLAIM_ID)

banner "1. Initial load - BOTH transports (no watermark yet: all 20 rows)"
"${A_HWM[@]}"
"${B_HWM[@]}"
"$Q" "SELECT (SELECT COUNT(*) FROM DENTAL_CLAIMS) AS connector_A,
             (SELECT COUNT(*) FROM DENTAL_CLAIMS_STAGED) AS unload_B;"
[ "$(count DENTAL_CLAIMS)" = 20 ] && [ "$(count DENTAL_CLAIMS_STAGED)" = 20 ] \
    || fail "expected 20 rows in both tables after the initial load"

banner "2. Mutate Snowflake (2 updates + 3 inserts + delete claim 10)"
python "$DEMO/sf_exec.py" --section S2

banner "3. Incremental upsert - BOTH transports (only the 5 changed rows)"
"${A_HWM[@]}"
"${B_HWM[@]}"
"$Q" "SELECT (SELECT COUNT(*) FROM DENTAL_CLAIMS) AS connector_A,
             (SELECT COUNT(*) FROM DENTAL_CLAIMS_STAGED) AS unload_B;"
echo "Changed/new claims via Transport A, then Transport B:"
"$Q" "SELECT CLAIM_ID, CLAIM_STATUS, AMOUNT FROM DENTAL_CLAIMS
      WHERE CLAIM_ID IN (1,2,21,22,23) ORDER BY CLAIM_ID;"
"$Q" "SELECT CLAIM_ID, CLAIM_STATUS, AMOUNT FROM DENTAL_CLAIMS_STAGED
      WHERE CLAIM_ID IN (1,2,21,22,23) ORDER BY CLAIM_ID;"
DIFF="$("$Q" "SELECT COUNT(*) AS rows_that_differ FROM DENTAL_CLAIMS a
              LEFT JOIN DENTAL_CLAIMS_STAGED b ON b.CLAIM_ID = a.CLAIM_ID
              WHERE b.CLAIM_ID IS NULL OR a.CLAIM_STATUS <> b.CLAIM_STATUS
                 OR a.AMOUNT <> b.AMOUNT OR a.UPDATED_AT <> b.UPDATED_AT;" | tail -1 | tr -d '[:space:]')"
echo "rows_that_differ = $DIFF"
[ "$(count DENTAL_CLAIMS)" = 23 ] && [ "$(count DENTAL_CLAIMS_STAGED)" = 23 ] && [ "$DIFF" = 0 ] \
    && [ "$(count "DENTAL_CLAIMS WHERE CLAIM_ID = 10")" = 1 ] \
    || fail "expected 23 identical rows in both tables (claim 10 still present) after the delta"

banner "4. Stream CDC - create the stream and outbox ([S5]), then the initial full load"
python "$DEMO/sf_exec.py" --section S5
python sync.py --source "$SRC" --target DENTAL_CLAIMS_CDC --change-capture none --mode truncate
[ "$(count DENTAL_CLAIMS_CDC)" = 22 ] || fail "expected 22 rows after the CDC full load"

banner "5. Delete claim 5, update claim 3 ([S6]); apply via the stream with Transport A"
python "$DEMO/sf_exec.py" --section S6
python sync.py "${STREAM_ARGS[@]}"
[ "$(count DENTAL_CLAIMS_CDC)" = 21 ] && [ "$(count "DENTAL_CLAIMS_CDC WHERE CLAIM_ID = 5")" = 0 ] \
    && [ "$(count "DENTAL_CLAIMS_CDC WHERE CLAIM_ID = 3 AND CLAIM_STATUS = 'PAID'")" = 1 ] \
    || fail "expected claim 5 deleted and claim 3 PAID after the Transport A stream run"

banner "6. Insert claim 24, update claim 4 ([S7]); apply via the stream with Transport B"
python "$DEMO/sf_exec.py" --section S7
python sync.py --transport unload "${STREAM_ARGS[@]}" --stage "$STAGE" --local-dir _unload_tmp
"$Q" "SELECT CLAIM_ID, CLAIM_STATUS, AMOUNT FROM DENTAL_CLAIMS_CDC
      WHERE CLAIM_ID IN (3,4,5,24) ORDER BY CLAIM_ID;"
SF_ROWS="$(python "$DEMO/sf_exec.py" --scalar "SELECT COUNT(*) FROM $SRC")"
CDC_ROWS="$(count DENTAL_CLAIMS_CDC)"
HAS5="$(count "DENTAL_CLAIMS_CDC WHERE CLAIM_ID = 5")"
HAS24="$(count "DENTAL_CLAIMS_CDC WHERE CLAIM_ID = 24")"
echo "Snowflake rows = $SF_ROWS, DENTAL_CLAIMS_CDC rows = $CDC_ROWS, claim 5 present = $HAS5, claim 24 present = $HAS24"
[ "$CDC_ROWS" = "$SF_ROWS" ] && [ "$CDC_ROWS" = 22 ] && [ "$HAS5" = 0 ] && [ "$HAS24" = 1 ] \
    || fail "stream target does not match Snowflake"
OUTBOX_ROWS="$(python "$DEMO/sf_exec.py" --scalar "SELECT COUNT(*) FROM $OUTBOX")"
echo "Outbox rows after delivery = $OUTBOX_ROWS (default: delivered rows are deleted)"
[ "$OUTBOX_ROWS" = 0 ] || fail "outbox still holds delivered rows"

banner "Choosing a transport"
cat <<'TXT'
  Transport A (connector pull):  fewest components; suited to incremental deltas
                                 and moderate volumes. Uses a Snowflake session
                                 for the duration of the load; batched DML writes.
  Transport B (unload + bulk):   compressed unload and native bulk load; suited to
                                 large full or incremental loads. Snowflake
                                 extracts in parallel; the target loads local
                                 files with its native bulk loader.
  Watermark vs stream:           a watermark needs a reliable UPDATED_AT column and
                                 cannot see deletes; a stream captures inserts,
                                 updates, and deletes.
  Network requirements for each option are described in README.md.
TXT

banner "Done: all verifications passed ($DEMO_TARGET)"
