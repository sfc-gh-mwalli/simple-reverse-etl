#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# End-to-end demo: Snowflake -> local MySQL, showing BOTH transports.
#   Source: SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS
#
#   Transport A - live connector pull            (sync.py)         -> DENTAL_CLAIMS
#   Transport B - unload -> stage -> bulk load   (unload_sync.py)  -> DENTAL_CLAIMS_STAGED
#
# For each transport: a FULL load (20 rows), then after a mutate (2 updates +
# 3 inserts) a DELTA upsert (-> 23 rows). Both target tables should end
# identical, demonstrating the two approaches produce the same result.
#
# Prereq: Docker Desktop running, and demo/.env.demo filled in
# (copy demo/.env.demo.example). Runs silently via the stored connection.
# ---------------------------------------------------------------------------
set -euo pipefail

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$DEMO")"
SRC="SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS"
STATE_A="$ROOT/sync_state.json"          # connector transport watermark
STATE_B="$ROOT/sync_state_unload.json"   # unload transport watermark
Q="$DEMO/mysql_query.sh"

banner() { printf '\n\033[1;36m=== %s ===\033[0m\n' "$1"; }

banner "0. Environment"
set -a; source "$DEMO/.env.demo"; set +a
if [ -z "${SF_CONNECTION_NAME:-}" ] && [ -z "${SF_PAT:-}" ]; then
    echo "ERROR: set SF_CONNECTION_NAME or SF_PAT in demo/.env.demo and re-run." >&2
    exit 1
fi
[ -d "$DEMO/.venv" ] || python3 -m venv "$DEMO/.venv"
# shellcheck disable=SC1091
source "$DEMO/.venv/bin/activate"
pip install -q -r "$DEMO/requirements-demo.txt"

banner "1. Start MySQL (Docker)"
docker compose -f "$DEMO/docker-compose.yml" up -d
printf "waiting for MySQL to be healthy"
until docker compose -f "$DEMO/docker-compose.yml" exec -T mysql \
        mysqladmin ping -h localhost -uroot -pdemopw --silent >/dev/null 2>&1; do
    printf "."; sleep 2
done
echo " up."

banner "2. Reset Snowflake source to 20 rows"
python "$DEMO/sf_exec.py" --file "$DEMO/setup_snowflake.sql"
rm -f "$STATE_A" "$STATE_B"

banner "3. FULL load - BOTH transports (from the 20-row baseline)"
echo "-- Transport A: live connector pull -> DENTAL_CLAIMS"
python "$ROOT/sync.py" --source "$SRC" --target DENTAL_CLAIMS \
    --change-capture none --mode truncate --state-file "$STATE_A"
echo "-- Transport B: unload -> stage -> bulk load -> DENTAL_CLAIMS_STAGED"
python "$ROOT/unload_sync.py" --source "$SRC" --target DENTAL_CLAIMS_STAGED \
    --change-capture none --mode truncate --local-dir "$ROOT/_unload_tmp" \
    --state-file "$STATE_B"
echo "Row counts after full load (both expect 20):"
"$Q" "SELECT (SELECT COUNT(*) FROM DENTAL_CLAIMS) AS connector_A,
             (SELECT COUNT(*) FROM DENTAL_CLAIMS_STAGED) AS unload_B;" || true

banner "4. Capture watermark, then mutate Snowflake (2 updates + 3 inserts)"
WM="$(python "$DEMO/sf_exec.py" --scalar "SELECT MAX(UPDATED_AT) FROM $SRC")"
echo "watermark = $WM"
printf '{"%s": {"watermark": "%s"}}\n' "$SRC" "$WM" | tee "$STATE_A" > "$STATE_B"
python "$DEMO/sf_exec.py" --file "$DEMO/mutate_snowflake.sql"

banner "5. DELTA upsert - BOTH transports (hwm on UPDATED_AT)"
echo "-- Transport A: connector upsert -> DENTAL_CLAIMS"
python "$ROOT/sync.py" --source "$SRC" --target DENTAL_CLAIMS \
    --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID \
    --state-file "$STATE_A"
echo "-- Transport B: unload + bulk upsert -> DENTAL_CLAIMS_STAGED"
python "$ROOT/unload_sync.py" --source "$SRC" --target DENTAL_CLAIMS_STAGED \
    --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID \
    --local-dir "$ROOT/_unload_tmp" --state-file "$STATE_B"

banner "6. Verify - both transports produced the same result"
echo "Row counts (both expect 23):"
"$Q" "SELECT (SELECT COUNT(*) FROM DENTAL_CLAIMS) AS connector_A,
             (SELECT COUNT(*) FROM DENTAL_CLAIMS_STAGED) AS unload_B;" || true
echo "Changed/new claims via Transport A (connector):"
"$Q" "SELECT CLAIM_ID, CLAIM_STATUS, AMOUNT FROM DENTAL_CLAIMS
      WHERE CLAIM_ID IN (1,2,21,22,23) ORDER BY CLAIM_ID;" || true
echo "Changed/new claims via Transport B (unload):"
"$Q" "SELECT CLAIM_ID, CLAIM_STATUS, AMOUNT FROM DENTAL_CLAIMS_STAGED
      WHERE CLAIM_ID IN (1,2,21,22,23) ORDER BY CLAIM_ID;" || true
echo "Do the two tables match? (expect match=1)"
"$Q" "SELECT CASE WHEN
        (SELECT COUNT(*) FROM DENTAL_CLAIMS) =
        (SELECT COUNT(*) FROM DENTAL_CLAIMS c JOIN DENTAL_CLAIMS_STAGED s USING (CLAIM_ID)
          WHERE c.CLAIM_STATUS=s.CLAIM_STATUS AND c.AMOUNT=s.AMOUNT)
      THEN 1 ELSE 0 END AS match_;" || true

banner "When to use which"
cat <<'TXT'
  Transport A (connector pull):  simplest, fewest moving parts, great for deltas
                                 and modest volume. Holds a Snowflake session
                                 during the load; row-batched writes.
  Transport B (unload + bulk):   parallel compressed unload + native bulk load;
                                 best for large full loads (30-60M+). Decoupled
                                 and restartable; on-prem only needs the stage.
TXT

banner "Done"
echo "Re-run any time; step 2 resets the source. Stop MySQL: docker compose -f demo/docker-compose.yml down"
