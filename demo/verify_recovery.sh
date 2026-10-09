#!/usr/bin/env bash
# Failure-and-recovery check (see verify_recovery.py).
#   ./demo/verify_recovery.sh                                          # MySQL
#   DEMO_TARGET=mssql ./demo/verify_recovery.sh                        # SQL Server, BULK INSERT
#   DEMO_TARGET=mssql TARGET_MSSQL_LOAD_METHOD=client ./demo/verify_recovery.sh
# The target container must be running (./demo/reset_demo.sh starts it).
set -euo pipefail
DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$DEMO")"
# shellcheck source=demo/target_env.sh
source "$DEMO/target_env.sh"
cd "$ROOT"
mkdir -p _unload_tmp
python "$DEMO/verify_recovery.py" "$@"
