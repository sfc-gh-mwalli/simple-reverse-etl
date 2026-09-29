#!/usr/bin/env bash
# Run a query against the demo MySQL container: demo/mysql_query.sh "SELECT ..."
# MYSQL_PWD is passed into the container so the mysql client doesn't warn about
# a password on the command line.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker compose -f "$DIR/docker-compose.yml" exec -T -e MYSQL_PWD=demopw mysql \
    mysql -uroot DENTAL_RPT -e "$1"
