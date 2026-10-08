#!/usr/bin/env bash
# Run SQL against the demo target selected by DEMO_TARGET (mysql | mssql):
#   DEMO_TARGET=mssql demo/target_query.sh "SELECT COUNT(*) AS n FROM DENTAL_CLAIMS;"
# Output is tab-separated with a header row for both databases.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
case "${DEMO_TARGET:-mysql}" in
  mysql)
    exec "$DIR/mysql_query.sh" "$1"
    ;;
  mssql)
    # -W trims padding; the sed drops sqlcmd's "----" underline row.
    docker compose -f "$DIR/docker-compose.yml" --profile mssql exec -T mssql \
        /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P Demo_Passw0rd -C -b \
        -d DENTAL_RPT -W -s $'\t' -Q "SET NOCOUNT ON; $1" | sed $'/^[-\t]*$/d'
    ;;
  *)
    echo "DEMO_TARGET must be mysql or mssql" >&2; exit 1
    ;;
esac
