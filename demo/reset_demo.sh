#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Resets the demonstration to its starting state:
#   - starts the MySQL container (if needed), creates any missing target tables,
#     and empties all three
#   - recreates the Snowflake source with 20 rows, its stream and outbox, and
#     empties the demo stage
#   - removes local watermark state and previously retrieved files
# Safe to run repeatedly. Requires Docker running and demo/.env.demo.
# ---------------------------------------------------------------------------
set -euo pipefail

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$DEMO")"

set -a; source "$DEMO/.env.demo"; set +a
[ -d "$DEMO/.venv" ] || python3 -m venv "$DEMO/.venv"
# shellcheck disable=SC1091
source "$DEMO/.venv/bin/activate"
pip install -q --disable-pip-version-check -r "$DEMO/requirements-demo.txt"

echo "Starting MySQL..."
docker compose -f "$DEMO/docker-compose.yml" up -d >/dev/null
until docker compose -f "$DEMO/docker-compose.yml" exec -T mysql \
        mysqladmin ping -h localhost -uroot -pdemopw --silent >/dev/null 2>&1; do
    sleep 2
done
"$DEMO/mysql_query.sh" "$(cat "$DEMO/mysql_init.sql")"
"$DEMO/mysql_query.sh" "TRUNCATE TABLE DENTAL_CLAIMS; TRUNCATE TABLE DENTAL_CLAIMS_STAGED; TRUNCATE TABLE DENTAL_CLAIMS_CDC;"

echo "Resetting Snowflake source, stream, outbox, and stage..."
python "$DEMO/sf_exec.py" --file "$DEMO/setup_snowflake.sql"

rm -rf "$ROOT/sync_state.json" "$ROOT/sync_state_unload.json" "$ROOT/_unload_tmp"

echo "Reset complete: Snowflake source has 20 rows; stream, outbox, stage, and MySQL tables are empty."
