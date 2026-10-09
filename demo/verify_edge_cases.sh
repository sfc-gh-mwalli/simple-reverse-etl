#!/usr/bin/env bash
# Edge-case probes (see verify_edge_cases.py).
#   ./demo/verify_edge_cases.sh                                          # MySQL
#   DEMO_TARGET=mssql ./demo/verify_edge_cases.sh                        # SQL Server, BULK INSERT
#   DEMO_TARGET=mssql TARGET_MSSQL_LOAD_METHOD=client ./demo/verify_edge_cases.sh
# The target container must be running (./demo/reset_demo.sh starts it).
set -euo pipefail
DEMO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(dirname "$DEMO")"
# shellcheck source=demo/target_env.sh
source "$DEMO/target_env.sh"
cd "$ROOT"
mkdir -p _unload_tmp
python "$DEMO/verify_edge_cases.py" "$@"
