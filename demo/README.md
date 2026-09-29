# Demo runbook — Snowflake to local MySQL, both transports

A turnkey, **silent** demo of both data-movement transports: read from Snowflake,
write to a local MySQL in Docker. Shows a full load and an incremental delta upsert
for each, then checks that both produce the same result.

## What it proves
- Snowflake to on-prem MySQL with **plain Python** (no ETL platform).
- **Outbound-only** from the job's perspective — the firewall-friendly shape.
- **Two transports** side by side:
  - Transport A — live connector pull (`sync.py`) into `DENTAL_CLAIMS`
  - Transport B — unload to stage, pull, bulk load (`unload_sync.py`) into `DENTAL_CLAIMS_STAGED`
- **Full refresh** and **incremental delta upsert** (merge on primary key).

## Prerequisites
- Docker Desktop running.
- `demo/.env.demo` filled in (copy `demo/.env.demo.example`). Using a stored
  `SF_CONNECTION_NAME` with password/key-pair auth makes the run fully silent — no
  browser, no prompts.
- The demo (re)creates the Snowflake source `SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS`
  via `setup_snowflake.sql`, so the role in your connection needs to create a database.

## Run it
```bash
cp demo/.env.demo.example demo/.env.demo   # edit it
./demo/run_demo.sh
```
First run builds a venv and installs `demo/requirements-demo.txt` (~30s); later runs are instant.

## What each step shows (talk track)
1. **Start MySQL** — local target in Docker (stands in for the on-prem SQL box), started with `--local-infile=1` so Transport B can bulk-load.
2. **Reset source** — 20 seeded claims in Snowflake.
3. **FULL load, both transports** — A into `DENTAL_CLAIMS`, B into `DENTAL_CLAIMS_STAGED`; both reach **20 rows**.
4. **Mutate Snowflake** — 2 updates (claims 1 & 2 to `PAID`, +100) + 3 inserts (21/22/23). A watermark (`MAX(UPDATED_AT)`) is captured first.
5. **DELTA upsert, both transports** — only the 5 changed rows flow; both tables reach **23 rows**.
6. **Verify** — row counts match, the changed/new claims look identical from both, and a join check prints `match_=1`.

## Poke at it live
```bash
demo/mysql_query.sh "SELECT COUNT(*) AS n FROM DENTAL_CLAIMS;"
demo/mysql_query.sh "SELECT * FROM DENTAL_CLAIMS_STAGED ORDER BY CLAIM_ID LIMIT 5;"

# run another delta by hand after changing Snowflake
python sync.py --source SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS --target DENTAL_CLAIMS \
    --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID
```

## Notes / variations
- **Transport B storage:** the demo unloads to the Snowflake user stage (`@~`) — zero
  setup. Production would use an external S3/ADLS/GCS stage; see the top-level README.
- **SQL Server target:** set `TARGET_KIND=mssql` (needs the ODBC driver + `pyodbc`, and a
  `<table>_stg` staging table for the MERGE upsert). Transport B bulk load for SQL Server
  uses `BULK INSERT`/`bcp` and is environment-specific — the local demo uses MySQL.
- **Stream-based CDC** (no watermark column) is documented in `sql/01_snowflake_setup.sql`.

## Teardown
```bash
docker compose -f demo/docker-compose.yml down -v     # stop MySQL + remove data
# optional, in Snowflake: DROP DATABASE SIMPLE_REVERSE_ETL_DEMO;
```
