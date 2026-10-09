#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Resets the demonstration to its starting state:
#   - starts the target container (MySQL, or SQL Server with DEMO_TARGET=mssql),
#     creates any missing target tables, and empties them
#   - recreates the Snowflake source with 20 rows, drops the stream and
#     outbox (created live in Part 5) and the Part 7 volume table, and
#     empties the demo stage
#   - removes local watermark state and previously retrieved files
# Safe to run repeatedly. Requires Docker running and demo/.env.demo.
#
#   ./demo/reset_demo.sh                    # MySQL
#   DEMO_TARGET=mssql ./demo/reset_demo.sh  # SQL Server
# ---------------------------------------------------------------------------
set -euo pipefail

DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$DEMO")"
# shellcheck source=demo/target_env.sh
source "$DEMO/target_env.sh"

echo "Starting $DEMO_TARGET..."
demo_start_target
demo_init_target
"$DEMO/target_query.sh" "TRUNCATE TABLE DENTAL_CLAIMS; TRUNCATE TABLE DENTAL_CLAIMS_STAGED; TRUNCATE TABLE DENTAL_CLAIMS_CDC; TRUNCATE TABLE VOLUME_CLAIMS; TRUNCATE TABLE VOLUME_CLAIMS_STAGED;" >/dev/null

echo "Resetting Snowflake source and stage; dropping the stream and outbox..."
python "$DEMO/sf_exec.py" --file "$DEMO/setup_snowflake.sql"

rm -f "$ROOT/sync_state.json" "$ROOT/sync_state_unload.json" "$ROOT"/sync_state_volume_*.json
# Empty (do not delete) _unload_tmp: the SQL Server container mounts it.
mkdir -p "$ROOT/_unload_tmp"
find "$ROOT/_unload_tmp" -mindepth 1 -delete

echo "Reset complete ($DEMO_TARGET): Snowflake source has 20 rows; no stream or outbox yet; stage and target tables are empty."
