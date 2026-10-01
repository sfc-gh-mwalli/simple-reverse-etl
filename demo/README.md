# SimpleReverseETL demonstration

This demonstration runs both SimpleReverseETL transports against a Snowflake source table
and a local MySQL instance in Docker. The local MySQL instance stands in for an
on-premises target database. For each transport, the script performs a full load followed
by an incremental upsert, then verifies that both transports produce identical results.

For architecture, network requirements, and production guidance, see the
[top-level README](../README.md).

## Scope

The demonstration covers:

- **Transport A** (`sync.py`): connector pull into the MySQL table `DENTAL_CLAIMS`.
- **Transport B** (`unload_sync.py`): unload to a Snowflake stage, file retrieval, and
  MySQL bulk load into `DENTAL_CLAIMS_STAGED`.
- A full refresh (`--change-capture none --mode truncate`) and an incremental upsert
  (`--change-capture hwm --mode upsert`) with each transport.
- Outbound-only connectivity from the job host: all connections to Snowflake are initiated
  by the job.

It does not cover SQL Server targets, stream-based change capture, external stages, or
orchestration. See [Variations](#variations).

## Prerequisites

- **Docker** with Docker Compose, able to publish port `3306` on `localhost`.
- **Python 3.11 or later** available as `python3`.
- **A Snowflake account and role** with privileges to create a database
  (`CREATE DATABASE` on the account), and a warehouse the role can use. The demonstration
  creates and repeatedly replaces `SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS`.
- **Non-interactive authentication.** The script opens several Snowflake sessions. Use a
  named connection in `~/.snowflake/connections.toml` configured for key-pair or
  password authentication, or a programmatic access token. Browser-based SSO will prompt
  on each session and is not recommended for this demonstration.

## Configuration

Create the demonstration environment file from the template and edit it:

```bash
cp demo/.env.demo.example demo/.env.demo
```

Configure **one** of the following authentication options:

| Option | Variables | Notes |
|---|---|---|
| Named connection | `SF_CONNECTION_NAME` | Account, user, and credentials are read from `connections.toml`. Leave `SF_PAT` empty. |
| Programmatic access token | `SF_ACCOUNT`, `SF_USER`, `SF_AUTH_METHOD=pat`, `SF_PAT` | Leave `SF_CONNECTION_NAME` empty. |

In both cases, set `SF_ROLE` and `SF_WAREHOUSE`. Retain `SF_DATABASE=SIMPLE_REVERSE_ETL_DEMO`
and `SF_SCHEMA=DENTAL`. The `TARGET_*` values point to the local container and do not
need to change. `demo/.env.demo` is excluded from version control.

## Running the demonstration

From the repository root:

```bash
./demo/run_demo.sh
```

On the first run the script creates a virtual environment in `demo/.venv` and installs
`demo/requirements-demo.txt`. Subsequent runs reuse it. The script is idempotent: each run
resets the Snowflake source and the local state files.

## Execution steps and expected results

| Step | Action | Expected result |
|---|---|---|
| 0 | Load `demo/.env.demo`, prepare the virtual environment. | No errors. |
| 1 | Start MySQL 8.4 with `--local-infile=1` (required by Transport B) and wait for it to become healthy. Target tables are created by `mysql_init.sql`. | Container `simple-reverse-etl-demo-mysql` is running. |
| 2 | Run `setup_snowflake.sql` to recreate the source with 20 claims; remove previous watermark state. | Source contains 20 rows. |
| 3 | Full load with Transport A into `DENTAL_CLAIMS` and Transport B into `DENTAL_CLAIMS_STAGED`. | Both tables contain 20 rows. |
| 4 | Record `MAX(UPDATED_AT)` as the watermark, then run `mutate_snowflake.sql`: claims 1 and 2 are set to `PAID` with the amount increased by 100, and claims 21, 22, and 23 are inserted. | Five source rows are newer than the watermark. |
| 5 | Incremental upsert with both transports on `UPDATED_AT`, keyed on `CLAIM_ID`. | Only the five changed rows are transferred; both tables contain 23 rows. |
| 6 | Compare the two target tables. | Row counts match, claims 1, 2, 21, 22, and 23 are identical in both tables, and the comparison query returns `match_ = 1`. |

The script stops with a non-zero exit status if a setup or synchronization step fails. The
verification queries in step 6 report their results but do not change the exit status.

## Inspecting the results

Query the local target:

```bash
demo/mysql_query.sh "SELECT COUNT(*) AS n FROM DENTAL_CLAIMS;"
demo/mysql_query.sh "SELECT * FROM DENTAL_CLAIMS_STAGED ORDER BY CLAIM_ID LIMIT 5;"
```

To run an additional incremental synchronization after changing the Snowflake source,
load the environment and invoke either transport directly:

```bash
set -a; source demo/.env.demo; set +a
source demo/.venv/bin/activate

python sync.py --source SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS --target DENTAL_CLAIMS \
    --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID \
    --state-file sync_state.json

python unload_sync.py --source SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS \
    --target DENTAL_CLAIMS_STAGED \
    --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID \
    --state-file sync_state_unload.json --local-dir _unload_tmp
```

## Variations

- **Stage.** Transport B unloads to the Snowflake user stage (`@~/simple_reverse_etl`),
  which requires no setup. Production deployments typically use an external stage; the
  connectivity and credential implications are described in the
  [top-level README](../README.md#transport-b-with-an-external-stage).
- **SQL Server.** Transport A supports SQL Server with `TARGET_KIND=mssql`. This requires
  the Microsoft ODBC Driver for SQL Server, `pyodbc`, and a `<table>_stg` staging table for
  the `MERGE`-based upsert. Transport B bulk load for SQL Server is not implemented.
- **Stream-based change capture.** For sources without a reliable modification timestamp,
  see [sql/01_snowflake_setup.sql](../sql/01_snowflake_setup.sql) and the `--change-capture
  stream` option of `sync.py`.

## Credentials used by the demonstration

The MySQL container uses a fixed root password (`demopw`) and is published only on
`localhost`. It is intended solely for local demonstration and must not be reused
elsewhere. Snowflake credentials are read from `demo/.env.demo` or `connections.toml` and
are never written to the repository.

## Cleanup

Stop the container and remove its data volume:

```bash
docker compose -f demo/docker-compose.yml down -v
```

Remove the Snowflake objects, local state, and unloaded files:

```sql
DROP DATABASE IF EXISTS SIMPLE_REVERSE_ETL_DEMO;
REMOVE @~/simple_reverse_etl;
```

```bash
rm -rf sync_state.json sync_state_unload.json _unload_tmp demo/.venv
```
