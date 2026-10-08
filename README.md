# SimpleReverseETL

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Snowflake](https://img.shields.io/badge/Snowflake-source-29B5E8.svg)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB.svg)](https://www.python.org/)

SimpleReverseETL is a reference implementation for delivering data from Snowflake to
on-premises relational databases (MySQL and Microsoft SQL Server) using a lightweight
Python job that runs inside the on-premises network. It provides two data-movement
transports, incremental change capture, and idempotent upsert semantics, and is intended
as a starting point for teams that need to feed downstream operational or reporting
databases without introducing an additional integration platform.

## Contents

- [Architecture](#architecture)
- [Network and firewall requirements](#network-and-firewall-requirements)
- [Choosing a transport](#choosing-a-transport)
- [Change capture and write semantics](#change-capture-and-write-semantics)
  - [Stream-based change capture](#stream-based-change-capture)
- [Configuration](#configuration)
- [Usage](#usage)
- [Production considerations](#production-considerations)
- [Known limitations](#known-limitations)
- [Demo](#demo)
- [Repository layout](#repository-layout)
- [License](#license)
- [Disclaimer](#disclaimer)

## Architecture

The job is deployed on a host inside the on-premises network and **initiates every
connection itself**. It never listens for, or depends on, inbound connections from
Snowflake or any other cloud service. This matches the common enterprise posture in
which traffic from the internet into the data center is denied, while controlled
egress from approved hosts is permitted.

```mermaid
flowchart LR
  subgraph onprem [On-premises network]
    job["SimpleReverseETL job"]
    db[("MySQL / SQL Server")]
  end
  subgraph cloud [Cloud]
    sf[("Snowflake account")]
    stg[("Stage storage")]
  end
  job -->|"outbound HTTPS"| sf
  job -->|"outbound HTTPS"| stg
  job -->|"local network"| db
```

A single job, `sync.py`, combines a change-capture mode (`--change-capture`: which rows
to send) with one of two transports (`--transport`: how they move). Every combination is
supported, and both transports write to the same target tables with the same write
modes.

**Transport A — connector pull (`--transport pull`, the default).** The job queries
Snowflake through the Snowflake Connector for Python, consumes the result as Apache Arrow
batches (`fetch_pandas_batches`), and writes each batch to the target with batched
parameterized statements. Upserts use `INSERT ... ON DUPLICATE KEY UPDATE` on MySQL and a
session temporary table plus `MERGE` on SQL Server. Memory use is bounded by batch size
rather than result size.

**Transport B — unload and bulk load (`--transport unload`).** The job issues
`COPY INTO <stage>` so that Snowflake writes the result as compressed CSV files, retrieves
the files, and loads them with the target database's native bulk loader
(`LOAD DATA LOCAL INFILE` on MySQL, `BULK INSERT` on SQL Server). The Snowflake session
is only needed for the unload and file retrieval; the load itself is decoupled from
Snowflake and can be retried from the retrieved files.

The code follows the same split: [change_capture.py](change_capture.py) builds a plan of
what to send, [transports.py](transports.py) moves it, [targets.py](targets.py) writes to
MySQL or SQL Server, and [sync.py](sync.py) commits the target and then records progress.

## Network and firewall requirements

This design removes the need for **inbound** connectivity into the on-premises network.
It does **not** remove the need for connectivity altogether: the on-premises host must be
permitted to make **outbound** connections to Snowflake, to the storage behind the stage,
or both, depending on the transport. If the host has no approved egress path, neither
transport will work without a change to network policy. Confirm this requirement with
your network and security teams before adopting the approach.

### Transport A, and Transport B as implemented in this repository

Both require outbound HTTPS from the job host to the Snowflake account. Note that the
account hostname alone is not sufficient:

| Endpoint | Port | Purpose |
|---|---|---|
| Account hostname (`<account>.snowflakecomputing.com`) | 443 | Authentication and query execution |
| Stage storage hosts (Snowflake-managed S3, Azure Blob, or GCS) | 443 | File transfer (`GET`/`PUT`) and retrieval of larger result sets |
| OCSP cache and responder hosts | 80 | TLS certificate revocation checks performed by the connector |

Obtain the authoritative list for your account with
[`SYSTEM$ALLOWLIST()`](https://docs.snowflake.com/en/sql-reference/functions/system_allowlist)
and validate connectivity from the job host with
[SnowCD](https://docs.snowflake.com/en/user-guide/snowcd), or with the connector's built-in
diagnostics (`enable_connection_diag=True`). Additional points:

- **Snowflake network policies.** If the account or service user is governed by a network
  policy, the public egress IP addresses of the job host (or its proxy/NAT) must be allowed.
- **Proxies.** The Snowflake Connector for Python 3.x/4.x honors the `HTTP_PROXY`,
  `HTTPS_PROXY`, and `NO_PROXY` environment variables; `NO_PROXY` can route stage storage
  traffic (for example `.amazonaws.com`) around the proxy.
- **TLS inspection.** Snowflake does not support proxies that re-sign TLS traffic. Where an
  inspecting proxy is in the path, configure it to pass Snowflake and stage-storage traffic
  through unmodified.
- **Private connectivity.** Where egress over the public internet is not permitted, AWS
  PrivateLink, Azure Private Link, or Google Cloud Private Service Connect can carry this
  traffic over a private path. Use
  [`SYSTEM$ALLOWLIST_PRIVATELINK()`](https://docs.snowflake.com/en/sql-reference/functions/system_allowlist_privatelink)
  for the corresponding host list.
- **Connector versions.** This implementation is validated with connector 4.x. The
  [5.x connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-universal-core)
  (currently in preview) reads proxy environment variables only when `use_proxy_env=True`
  is set, and replaces OCSP checks with optional CRL checks, which changes the endpoints
  above. Pin the connector version in production and review these items before upgrading.

As implemented here, Transport B unloads to the Snowflake user stage and retrieves files
with `GET`, so its connectivity requirements are the same as Transport A's.

### Transport B with an external stage

In production, Transport B is typically configured against an **external stage** backed
by object storage (Amazon S3, Azure Blob Storage, or Google Cloud Storage). This changes
the connectivity profile:

- Snowflake writes to the bucket through a
  [storage integration](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration);
  the on-premises host reads from the bucket with its **own** credentials (for example an
  IAM role or user, a SAS token, or a service principal) using the cloud provider's SDK or
  CLI. `GET` is not supported for external stages.
- The job host therefore requires outbound HTTPS to the **object storage endpoint**, and the
  bucket policy must permit access from the job host's identity and network location.
- If the unload is issued by the job, the host still requires connectivity to Snowflake for
  the `COPY INTO` statement. If the unload is instead scheduled inside Snowflake (for
  example with a [task](https://docs.snowflake.com/en/user-guide/tasks-intro)), the job
  host needs **only** bucket access and no direct Snowflake connectivity. This variant is
  often the easiest to approve, but it requires a way for the job to detect newly
  unloaded files and is not implemented in this repository.

| Deployment | Job host needs outbound access to |
|---|---|
| Transport A | Snowflake account, stage storage, OCSP |
| Transport B, internal stage (this repository) | Snowflake account, stage storage, OCSP |
| Transport B, external stage, unload issued by the job | Snowflake account and OCSP (for `COPY INTO`), plus the customer bucket |
| Transport B, external stage, unload scheduled in Snowflake | The customer bucket only |

## Choosing a transport

| Consideration | Transport A — connector pull | Transport B — unload and bulk load |
|---|---|---|
| Components | Job, Snowflake, target | Job, Snowflake, stage storage, target |
| Suitable volumes | Incremental deltas and moderate tables | Large full or incremental loads (tens of millions of rows) |
| Load mechanism | Batched parameterized DML | Native bulk load |
| Snowflake session | Used for the duration of the load | Used only for unload and retrieval |
| Recovery | Re-query the source | Re-load from retrieved files |
| Change capture | `none`, `hwm`, `stream` | `none`, `hwm`, `stream` |
| Targets | MySQL, SQL Server | MySQL (`LOAD DATA LOCAL INFILE`), SQL Server (`BULK INSERT` or client-side batches) |

Transport A is the simpler option and is well suited to scheduled delta synchronization.
Transport B is preferable when full reloads or large deltas make row-level DML the
bottleneck, and is the natural fit when an object-storage hand-off is the approved
integration pattern.

## Change capture and write semantics

**Change capture** (`--change-capture`) determines which rows are sent:

- `none` — the full source on every run. Typically combined with `--mode truncate`.
- `hwm` — rows whose monotonic high-water-mark column (for example `UPDATED_AT`) is greater
  than the last delivered value (the watermark). Each run reads `MAX(col)` as a ceiling
  before extraction, loads the rows between the watermark and the ceiling, and records the
  ceiling as the new watermark only after the target commit succeeds, so a failed run is
  safely retried. The watermark is stored in a JSON file on the job host (`--state-file`,
  default `./sync_state.json`), keyed by source and target table, so one file can track
  several source/target pairs. On the first run, `--hwm-start`
  sets the starting point; without it, all existing rows are loaded. Keep the state file
  on durable storage in production, or replace `load_state`/`save_state` in
  [change_capture.py](change_capture.py) with a control
  table or scheduler variable.
- `stream` — Both transports. Uses a Snowflake stream instead of a watermark column, and
  also propagates deletes. See [Stream-based change capture](#stream-based-change-capture).

**Choosing `hwm` or `stream`.**

| Consideration | `hwm` | `stream` |
|---|---|---|
| Source requirement | A reliable, monotonically increasing column such as `UPDATED_AT` | None; change tracking is enabled on the table |
| Inserts and updates | Yes | Yes |
| Deletes | No; deleted rows remain in the target | Yes |
| Position stored in | JSON state file on the job host | Stream offset and outbox table in Snowflake |
| Snowflake objects to create | None | Stream and outbox table |
| Transports | A and B | A and B |
| Operational risk | Rows updated without changing the column are missed | Stream becomes stale if not consumed within the retention period |

### Stream-based change capture

A Snowflake [stream](https://docs.snowflake.com/en/user-guide/streams-intro) records the
inserts, updates, and deletes committed to a table after a point in time (its offset).
Selecting from a stream does not change the offset; the offset advances only when the
stream is read by a DML statement that commits. The job uses this to make delivery
reliable across two systems that cannot share a transaction:

1. **Consume.** `INSERT INTO <outbox> SELECT ... FROM <stream>` copies all pending changes
   into an outbox table in Snowflake, together with the change type, an update flag, and a
   load timestamp. Committing this statement advances the stream offset; the changes are
   now held in the outbox.
2. **Snapshot and reduce.** The job records the newest `_CDC_LOADED_AT` among outbox rows
   not yet exported (the cutoff) and handles only rows up to it. A stream represents an
   update as two rows: a `DELETE` with the old values and an `INSERT` with the new values,
   both with `METADATA$ISUPDATE = TRUE`. A query in Snowflake discards the old-value rows
   and keeps only the most recent change for each key (`QUALIFY ROW_NUMBER() ...`), so the
   outbox can safely hold the changes from several consumes.
3. **Apply.** Remaining inserts are upserted on `--key-cols`, and plain deletes are deleted
   by key, in one target transaction. Transport A reads the reduced result in Arrow
   batches. Transport B unloads it twice, as full rows to `<stage>/<target>/upsert/` and
   as key columns to `<stage>/<target>/delete/`, then bulk-loads the upserts and
   bulk-deletes the keys through a temporary table.
4. **Acknowledge.** After the target commit, the job marks outbox rows up to the cutoff as
   exported. Rows consumed after the snapshot remain pending for the next run.
5. **Purge.** The job then deletes delivered rows from the outbox. By default this happens
   immediately, so the outbox only holds changes not yet delivered;
   `--outbox-retention-days N` keeps delivered rows for N days, for example as an audit
   trail. Rows that have not been delivered are never deleted.

If the job fails after step 1, the changes stay in the outbox and are applied on the next
run. If it fails after step 3 but before step 4, they are applied again. Upserts and
deletes by key are idempotent, so delivery is at-least-once with a correct final state.
A failure between steps 4 and 5 leaves delivered rows in the outbox until the next
successful run purges them.

**Setup.** Run once, as described in [sql/01_snowflake_setup.sql](sql/01_snowflake_setup.sql):

- Enable change tracking on the source table. Only the table owner can do this; creating
  a stream also enables it if the creating role owns the table.
- Create a stream on the source table. Use a standard stream; an append-only stream does
  not report updates or deletes.
- Create the outbox table with the source columns **in the same order**, followed by
  `_CDC_ACTION`, `_CDC_ISUPDATE`, `_CDC_LOADED_AT`, and `_CDC_EXPORTED`. The consume
  statement inserts by position. Create it as a **transient** table: it is short-lived
  working data, so Fail-safe storage adds cost without a recovery benefit; one day of
  Time Travel still allows `UNDROP`. If the outbox is ever lost, recreate the stream and
  perform a full load. Use a permanent table only if delivered rows are retained as an
  audit trail.
- Perform an initial full load (`--change-capture none --mode truncate`) after creating the
  stream. Changes recorded between stream creation and the full load are applied again on
  the first stream run, which is harmless.

**Privileges.** The job's role needs `USAGE` on the database and schema, `SELECT` on both
the stream and its source table, and `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on the
outbox.

**Operational notes.**

- *Staleness.* A stream becomes stale if it is not consumed within the source table's data
  retention period, extended up to `MAX_DATA_EXTENSION_TIME_IN_DAYS` (14 days by default).
  A stale stream cannot be read; it must be recreated and the target fully reloaded.
  Monitor `STALE_AFTER` in `SHOW STREAMS` and schedule runs well inside that window.
- *Recreating the source.* Replacing the source table (`CREATE OR REPLACE TABLE`) breaks
  the stream. Recreate the stream and perform a full load.
- *Outbox size.* With the default `--outbox-retention-days 0`, the outbox is emptied of
  delivered rows after every successful run. With a retention period, it holds that many
  days of changes; each update is stored as two rows.
- *Volume.* The reduction runs in Snowflake and both transports apply the result in a
  single target transaction. This suits regular deltas; for a very large backlog, perform
  a full load and recreate the stream instead.
- *Alternative.* The `CHANGES` clause reads change-tracking data between two timestamps
  without a stream or outbox. Its position must then be tracked by the job, as with `hwm`.
  An example is included in `sql/01_snowflake_setup.sql`; it is not implemented in the job.

### Write modes

`--mode truncate` replaces the target contents; `--mode upsert` merges on
`--key-cols`, which must correspond to a primary or unique key on the target;
`--mode append` inserts only. Stream mode requires `--mode upsert`.

**NULL handling (Transport B).** SQL `NULL` values are unloaded as an explicit sentinel and
converted back to `NULL` during the bulk load, so `NULL` and empty strings remain distinct
through the round trip.

## Configuration

Configuration is read from environment variables; [.env.example](.env.example) provides a
template. Load the file into the environment before running a job
(for example `set -a; source .env; set +a`) or supply the variables from your scheduler
or secret store.

| Variable | Description |
|---|---|
| `SF_CONNECTION_NAME` | Named entry in `~/.snowflake/connections.toml`. When set, account, user, and credentials are taken from that entry. |
| `SF_ACCOUNT`, `SF_USER` | Account identifier and user, when not using a named connection. |
| `SF_AUTH_METHOD` | `keypair` or `pat`. |
| `SF_PRIVATE_KEY_PATH`, `SF_PRIVATE_KEY_PASSPHRASE` | PKCS#8 private key for key-pair authentication. |
| `SF_PAT` | Programmatic access token. |
| `SF_ROLE`, `SF_WAREHOUSE`, `SF_DATABASE`, `SF_SCHEMA` | Session context, applied on top of either method. |
| `TARGET_KIND` | `mysql` or `mssql`. |
| `TARGET_HOST`, `TARGET_PORT`, `TARGET_DATABASE`, `TARGET_USER`, `TARGET_PASSWORD` | Target database connection. |
| `TARGET_ODBC_DRIVER` | ODBC driver name for SQL Server (default `ODBC Driver 18 for SQL Server`). |
| `TARGET_MSSQL_ENCRYPT` | SQL Server: encrypt the connection (`yes` or `no`, default `yes`). |
| `TARGET_MSSQL_TRUST_SERVER_CERT` | SQL Server: skip server certificate validation (default `no`). Set `yes` only for test servers with self-signed certificates. |
| `TARGET_MSSQL_LOAD_METHOD` | SQL Server, Transport B: `bulk_insert` (default) or `client`. See [SQL Server targets](#sql-server-targets). |
| `TARGET_MSSQL_BULK_DIR` | SQL Server, `bulk_insert` only: the `--local-dir` folder as SQL Server sees it, for example `\\fileserver\share\unload`. |
| `LOG_LEVEL` | Python logging level (default `INFO`). |

**Authentication.** For unattended execution, use
[key-pair authentication](https://docs.snowflake.com/en/user-guide/key-pair-auth) with a
dedicated service user. Programmatic access tokens are supported for evaluation and
short-lived use.

## Usage

Install dependencies (the SQL Server driver additionally requires the Microsoft ODBC
Driver for SQL Server at the operating-system level):

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Transport A (the default, `--transport pull`):

```bash
# Full refresh
python sync.py --source ANALYTICS.DENTAL.DENTAL_CLAIMS --target DENTAL_CLAIMS \
    --change-capture none --mode truncate

# Incremental upsert on a high-water-mark column
python sync.py --source ANALYTICS.DENTAL.DENTAL_CLAIMS --target DENTAL_CLAIMS \
    --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID

# Stream-based change capture, including deletes
python sync.py --source ANALYTICS.DENTAL.DENTAL_CLAIMS --target DENTAL_CLAIMS \
    --change-capture stream --stream ANALYTICS.DENTAL.CLAIMS_STREAM \
    --outbox ANALYTICS.DENTAL.CLAIMS_OUTBOX --mode upsert --key-cols CLAIM_ID
```

Transport B (`--transport unload`) accepts the same change-capture and write options,
plus `--stage` and `--local-dir`:

```bash
# Incremental upsert via unload and bulk load
python sync.py --transport unload \
    --source ANALYTICS.DENTAL.DENTAL_CLAIMS --target DENTAL_CLAIMS \
    --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID \
    --stage @~/simple_reverse_etl --local-dir /var/lib/simple-reverse-etl/unload

# Stream-based change capture, including deletes
python sync.py --transport unload \
    --source ANALYTICS.DENTAL.DENTAL_CLAIMS --target DENTAL_CLAIMS \
    --change-capture stream --stream ANALYTICS.DENTAL.CLAIMS_STREAM \
    --outbox ANALYTICS.DENTAL.CLAIMS_OUTBOX --mode upsert --key-cols CLAIM_ID \
    --stage @~/simple_reverse_etl --local-dir /var/lib/simple-reverse-etl/unload
```

The job exits non-zero on failure, rolls back the target transaction, and leaves the
watermark or outbox state unchanged. Options that do not apply to the chosen transport
(for example `--stage` with `pull`, or `--commit-rows` with `unload`) are rejected. Run
`python sync.py --help` for the full option list.

### SQL Server targets

- **Driver.** Install the Microsoft ODBC Driver 18 for SQL Server on the job host. On
  macOS with Homebrew, the driver supports OpenSSL 1.1 or 3; if `openssl@4` is installed,
  `/opt/homebrew/opt/openssl` must point to `openssl@3`.
- **Upserts** use a session temporary table and one `MERGE ... WITH (HOLDLOCK)` per batch.
  No staging tables need to be created in the target database.
- **Transport B load methods** (`TARGET_MSSQL_LOAD_METHOD`):
  - `bulk_insert` (default): SQL Server reads the files itself with `BULK INSERT`. The job
    writes the files to `--local-dir`, which must be a folder SQL Server can also read; set
    `TARGET_MSSQL_BULK_DIR` to that folder as SQL Server sees it (for example a UNC share).
    The login needs `ADMINISTER BULK OPERATIONS` (or the `bulkadmin` role) and the SQL
    Server service account needs read access to the folder.
  - `client`: the job reads the files and sends them in batches with `fast_executemany`.
    No shared folder or bulk permission is needed; throughput is lower than `bulk_insert`.
  - `bcp` is not used because it cannot parse the quoted CSV fields that the unload writes.
- **Certificates.** Connections are encrypted and the server certificate is validated by
  default. Install the server's CA certificate on the job host rather than setting
  `TARGET_MSSQL_TRUST_SERVER_CERT=yes`.
- **Timestamps** keep their fractional seconds: parameters are bound with explicit
  precision, because `pyodbc` with `fast_executemany` otherwise drops milliseconds.
- **Unicode.** Text parameters are bound as Unicode, because `pyodbc` otherwise sends
  strings for `NVARCHAR(MAX)` columns as non-Unicode and characters outside the server
  code page are lost. With `bulk_insert`, each unloaded file is converted to UTF-16 next
  to the original and loaded with `DATAFILETYPE='widechar'`, which `BULK INSERT` reads
  the same way on Windows and Linux (it does not reliably read UTF-8). Allow about twice
  the file size in `--local-dir`.
- **Empty strings.** `BULK INSERT` loads a quoted empty string as `NULL`. Because SQL
  `NULL` values arrive as the sentinel, the load maps that `NULL` back to `''`, so empty
  strings and `NULL` stay distinct with both load methods.
- **Fidelity check.** [demo/verify_fidelity.sh](demo/verify_fidelity.sh) round-trips
  `NULL`, empty strings, the sentinel text, non-ASCII text, quotes, commas, embedded
  newlines, and padded text through both transports and compares every value with
  Snowflake, on either demo target.

## Production considerations

- **Service identity and privileges.** Run under a dedicated service user and role granted
  the minimum required: `USAGE` on the warehouse, database, and schema and `SELECT` on the
  source; for stream mode, `SELECT` on the stream and its source table and `SELECT`,
  `INSERT`, `UPDATE`, and `DELETE` on the outbox; for Transport B with a named stage,
  the appropriate stage privileges (`READ` and `WRITE` on an internal stage).
- **Secrets.** Do not deploy a populated `.env` file. Supply credentials from an approved
  secret store and prefer key-pair authentication over passwords or long-lived tokens.
- **Scheduling.** The job has no scheduler of its own. Invoke it from the orchestrator
  already in use (cron, Airflow, Control-M, and so on). Runs for a given target should not
  overlap.
- **Target schema.** Target tables must exist before the first run, with column names
  matching the source projection and a key that supports the chosen upsert.
- **MySQL truncate.** `TRUNCATE` commits implicitly on MySQL, so a full load that fails
  after it leaves the table empty until the next successful run. On SQL Server, `TRUNCATE`
  is part of the load transaction.
- **Volume.** Prefer incremental change capture over full reloads, and prefer Transport B
  where row-level DML becomes the bottleneck. Transport A read throughput can be increased
  further with `cursor.get_result_batches()` for parallel retrieval.
- **Stream retention.** In stream mode, schedule runs well inside the stream's
  `STALE_AFTER` window; see [Stream-based change capture](#stream-based-change-capture).

## Known limitations

- SQL Server `BULK INSERT` has been tested with a folder mounted into the SQL Server
  container. Reading from a network share depends on the share permissions of the SQL
  Server service account and has not been tested.
- Retrieval from external stages (cloud SDK instead of `GET`) and Snowflake-scheduled
  unloads are described above but not implemented.
- Schema evolution is not managed; source and target structures must be kept aligned.

## Demo

A self-contained demonstration runs both transports against a Snowflake table and a local
MySQL or SQL Server instance in Docker (`DEMO_TARGET=mysql|mssql`). It performs a full
load and an incremental upsert with each transport, applies inserts, updates, and deletes
through a stream, and verifies the results against Snowflake. See
[demo/README.md](demo/README.md).

## Repository layout

```
sync.py               Command-line entry point: --transport pull|unload, commit, progress
change_capture.py     Which rows to send: full, hwm (watermark state), stream (outbox)
transports.py         How rows move: pull (Arrow batches) or unload (stage, GET, bulk load)
targets.py            MySQL and SQL Server writers (upsert, delete, bulk load, bulk delete)
snowflake_source.py   Connection, Arrow reads, unload and GET, stream and outbox helpers
config.py             Environment-based configuration for Snowflake and the target
sql/                  One-time Snowflake setup for stream-based change capture
demo/                 Docker and Snowflake demonstration (MySQL or SQL Server)
```

## License

Released under the MIT License; see [LICENSE](LICENSE). Third-party components and their
licenses are listed in [NOTICE](NOTICE). Contributions are not accepted; see
[CONTRIBUTING.md](CONTRIBUTING.md). To report a security issue, see
[SECURITY.md](SECURITY.md).

## Disclaimer

This software is provided "as is", without warranty of any kind, express or implied.

This is not an official Snowflake product or offering. It is an independent reference
implementation built on documented Snowflake features and is not endorsed, supported, or
maintained by Snowflake Inc. Running it consumes compute in your Snowflake account. Use it
at your own risk.
