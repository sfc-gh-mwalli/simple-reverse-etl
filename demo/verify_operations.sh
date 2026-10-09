#!/usr/bin/env bash
# Operational checks (see verify_operations.py).
#   ./demo/verify_operations.sh                                          # MySQL
#   DEMO_TARGET=mssql ./demo/verify_operations.sh                        # SQL Server, BULK INSERT
#   DEMO_TARGET=mssql TARGET_MSSQL_LOAD_METHOD=client ./demo/verify_operations.sh
# The target container must be running (./demo/reset_demo.sh starts it).
set -euo pipefail
DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$DEMO")"
# shellcheck source=demo/target_env.sh
source "$DEMO/target_env.sh"
cd "$ROOT"
mkdir -p _unload_tmp
python "$DEMO/verify_operations.py" "$@"
