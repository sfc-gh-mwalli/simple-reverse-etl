# shellcheck shell=bash
# ---------------------------------------------------------------------------
# Sourced by the demo scripts. Selects the demo target database:
#
#   DEMO_TARGET=mysql  (default)  local MySQL container
#   DEMO_TARGET=mssql             local SQL Server container
#
# Loads Snowflake settings from demo/.env.demo, then sets the TARGET_* variables
# for the selected container, activates demo/.venv, and defines:
#   demo_start_target   start the target container and wait until it is healthy
#   demo_init_target    create the demo database and tables (idempotent)
#
# Optional for SQL Server: TARGET_MSSQL_LOAD_METHOD=bulk_insert (default) | client
# ---------------------------------------------------------------------------
DEMO="${DEMO:-$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)}"
ROOT="${ROOT:-$(dirname "$DEMO")}"
DEMO_TARGET="${DEMO_TARGET:-mysql}"
COMPOSE=(docker compose -f "$DEMO/docker-compose.yml")

set -a; source "$DEMO/.env.demo"; set +a

case "$DEMO_TARGET" in
  mysql)
    DEMO_SERVICE=mysql
    ;;
  mssql)
    DEMO_SERVICE=mssql
    COMPOSE+=(--profile mssql)
    # Local demo container only; the SA password is a throwaway.
    export TARGET_KIND=mssql TARGET_HOST=127.0.0.1 TARGET_PORT=1433 \
           TARGET_DATABASE=DENTAL_RPT TARGET_USER=sa TARGET_PASSWORD=Demo_Passw0rd \
           TARGET_MSSQL_TRUST_SERVER_CERT=yes \
           TARGET_MSSQL_LOAD_METHOD="${TARGET_MSSQL_LOAD_METHOD:-bulk_insert}" \
           TARGET_MSSQL_BULK_DIR=/var/opt/unload
    ;;
  *)
    echo "DEMO_TARGET must be mysql or mssql, got '$DEMO_TARGET'" >&2
    return 1 2>/dev/null || exit 1
    ;;
esac
export DEMO_TARGET

[ -d "$DEMO/.venv" ] || python3 -m venv "$DEMO/.venv"
# shellcheck disable=SC1091
source "$DEMO/.venv/bin/activate"
pip install -q --disable-pip-version-check -r "$DEMO/requirements-demo.txt"

demo_start_target() {
    # The SQL Server container mounts --local-dir; create it first so Docker
    # does not create it as root.
    mkdir -p "$ROOT/_unload_tmp"
    "${COMPOSE[@]}" up -d "$DEMO_SERVICE" >/dev/null
    printf "Waiting for %s" "$DEMO_TARGET"
    local status=""
    for _ in $(seq 1 90); do
        status="$(docker inspect -f '{{.State.Health.Status}}' \
                  "simple-reverse-etl-demo-$DEMO_SERVICE" 2>/dev/null || true)"
        [ "$status" = "healthy" ] && { echo " ready."; return 0; }
        printf "."; sleep 2
    done
    echo " not healthy (status: ${status:-unknown})." >&2
    return 1
}

demo_init_target() {
    if [ "$DEMO_TARGET" = mysql ]; then
        "$DEMO/target_query.sh" "$(cat "$DEMO/mysql_init.sql")" >/dev/null
    else
        # Piped in rather than bind-mounted: a single-file mount keeps pointing
        # at the old file after an editor replaces it.
        "${COMPOSE[@]}" exec -T mssql /opt/mssql-tools18/bin/sqlcmd \
            -S localhost -U sa -P "$TARGET_PASSWORD" -C -b -i /dev/stdin \
            < "$DEMO/mssql_init.sql" >/dev/null
    fi
}
