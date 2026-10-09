# SimpleReverseETL demonstration runbook

This runbook is a presenter script for demonstrating both SimpleReverseETL transports and
both incremental change-capture methods, against either a MySQL or a SQL Server target.
For each transport it shows the source data in Snowflake, walks through the relevant
code, runs a full load and an incremental load, and shows the result in TablePlus. For
Transport B it also shows the unloaded files on the stage. A final part shows
stream-based change capture, including deletes.

For architecture, network requirements, and production guidance, see the
[top-level README](../README.md). For an unattended run of the whole sequence, use
`./demo/run_demo.sh`. To check that awkward values (NULL, empty strings, non-ASCII text,
quotes, newlines, booleans) survive both transports unchanged, run
`./demo/verify_fidelity.sh`. Three further suites test failure recovery
(`./demo/verify_recovery.sh`), data types and edge cases (`./demo/verify_edge_cases.sh`),
and the run lock, timeouts, and TLS (`./demo/verify_operations.sh`); see
[Tested versions](../README.md#tested-versions). They create and drop their own tables. To
measure throughput at volume, see
[Part 7](#part-7-volume-test-optional-not-for-the-live-session). All accept
`DEMO_TARGET=mssql`.

**Presentation time:** approximately 25 minutes for Parts 1 to 4 and 6, plus 10 minutes for
Part 5 (stream-based change capture). Part 5 can be skipped.

## Contents

- [Setup (once, before the session)](#setup-once-before-the-session)
- [Reset before each presentation](#reset-before-each-presentation)
- [Part 1: Source data in Snowflake](#part-1-source-data-in-snowflake)
- [Part 2: Transport A, connector pull](#part-2-transport-a-connector-pull)
- [Part 3: Transport B, unload and bulk load](#part-3-transport-b-unload-and-bulk-load)
- [Part 4: Incremental load on both transports](#part-4-incremental-load-on-both-transports)
- [Part 5: Stream-based change capture](#part-5-stream-based-change-capture)
- [Part 6: Summary](#part-6-summary)
- [Part 7: Volume test (optional, not for the live session)](#part-7-volume-test-optional-not-for-the-live-session)
- [Troubleshooting](#troubleshooting)
- [Cleanup](#cleanup)

## Setup (once, before the session)

1. **Docker Desktop.** Start Docker Desktop and leave it running.
2. **Snowflake connection.** Create `demo/.env.demo` from the template and set
   `SF_CONNECTION_NAME` (or `SF_ACCOUNT`, `SF_USER`, `SF_PAT`), `SF_ROLE`, and
   `SF_WAREHOUSE`. The role must be able to create a database. Use non-interactive
   authentication (key-pair, password, or PAT); browser SSO prompts on every session.

   ```bash
   cp demo/.env.demo.example demo/.env.demo
   ```

3. **TablePlus.** Install TablePlus (`brew install --cask tableplus`) and create a
   connection for the target you will present (or both):

   | Field | MySQL | SQL Server |
   |---|---|---|
   | Connection type | MySQL | Microsoft SQL Server |
   | Name | SimpleReverseETL MySQL | SimpleReverseETL SQL Server |
   | Host | `127.0.0.1` | `127.0.0.1` |
   | Port | `3306` | `1433` |
   | User | `root` | `sa` |
   | Password | `demopw` | `Demo_Passw0rd` |
   | Database | `DENTAL_RPT` | `DENTAL_RPT` (tables are in schema `dbo`) |

   The target container must be running for the connection test to succeed; the reset
   step below starts it.

4. **Snowsight.** Open a new SQL worksheet, paste in the contents of
   [demo/snowsight_demo.sql](snowsight_demo.sql), and set the worksheet role and warehouse
   to match `demo/.env.demo`. Each section of the worksheet is labeled `[S1]` to `[S8]`.
   The worksheet references objects created by the reset step, so run the reset once
   before using it.

5. **Screen layout.** Arrange four windows: a terminal in the repository root, the code
   editor, Snowsight, and TablePlus.

## Reset before each presentation

The demo runs against MySQL by default, or against SQL Server when `DEMO_TARGET=mssql` is
set. Choose one and use it for the whole presentation.

In the terminal, from the repository root:

```bash
./demo/reset_demo.sh                     # MySQL
DEMO_TARGET=mssql ./demo/reset_demo.sh   # SQL Server
```

The reset script:

- starts the target container, creates any missing target tables, and empties
  `DENTAL_CLAIMS`, `DENTAL_CLAIMS_STAGED`, and `DENTAL_CLAIMS_CDC`
- recreates the Snowflake source with 20 rows, creates and empties the stage
  `UNLOAD_STAGE`, and drops the stream `CLAIMS_STREAM` and the outbox `CLAIMS_OUTBOX`,
  which you create live in Part 5
- removes the local watermark files and any previously retrieved files

Then prepare the terminal session that you will present from. This loads the Snowflake
settings, sets the target connection, and activates the virtual environment:

```bash
export DEMO_TARGET=mysql                 # or: export DEMO_TARGET=mssql
source demo/target_env.sh
export SRC=SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS
```

All commands in Parts 1 to 5 are identical for both targets.

For MySQL, the job requires a verified server certificate. `target_env.sh` copies the CA
certificate that the MySQL container generated to `demo/.mysql-demo-ca.pem` and sets
`TARGET_MYSQL_SSL_CA` to it. Host-name checking is turned off for the demo only, because
that certificate is not issued for `127.0.0.1`. If the MySQL container was not yet running
when you sourced the script, run `./demo/reset_demo.sh` (which starts it and copies the
certificate) and source the script again.

**Check:** the script ends with `Reset complete`. In TablePlus, all three tables are empty.

### Running against SQL Server

- **Prerequisites.** Microsoft ODBC Driver 18 for SQL Server on your machine (see
  [SQL Server targets](../README.md#sql-server-targets)). The first start downloads the
  `mcr.microsoft.com/mssql/server:2022-latest` image and accepts the SQL Server Developer
  edition license. On Apple silicon the image runs under emulation.
- **TablePlus.** Use the SQL Server connection from preparation step 3.
- **Transport B load method.** By default SQL Server reads the unloaded files itself with
  `BULK INSERT`; the demo mounts `_unload_tmp` into the container at `/var/opt/unload` to
  stand in for a shared folder. To show the client-side method instead, run
  `export TARGET_MSSQL_LOAD_METHOD=client` before `source demo/target_env.sh`.
- **Log lines.** Transport B additionally logs one line per file, for example
  `DENTAL_CLAIMS_STAGED: 20 rows from upsert/data_0_0_0.csv (bulk_insert)`.
- **Startup time.** Under emulation SQL Server can take a minute to accept connections
  after the container starts. The reset script waits for it.

---

## Part 1: Source data in Snowflake

**Snowsight: run `[S1]`.**

- The first query shows 20 dental claims with `CLAIM_ID`, `MEMBER_ID`, `CLAIM_STATUS`,
  `AMOUNT`, and `UPDATED_AT`.
- The second query returns `ROW_COUNT = 20` and the latest `UPDATED_AT`.

**Talking points**

- `UPDATED_AT` is the high-water-mark column. Incremental runs select only rows whose
  `UPDATED_AT` is later than the last successful run.
- The target is on premises, behind a firewall that blocks inbound connections. The job
  therefore runs on premises and makes only outbound connections. Show the diagram and
  the network requirements table in the [top-level README](../README.md#network-and-firewall-requirements).

**TablePlus:** open `DENTAL_CLAIMS`, `DENTAL_CLAIMS_STAGED`, and `DENTAL_CLAIMS_CDC`. All
three are empty.

---

## Part 2: Transport A, connector pull

### Code walkthrough

| File | What to show |
|---|---|
| [config.py](../config.py) | All settings come from environment variables. Credentials are never in code. |
| [snowflake_source.py](../snowflake_source.py#L42) `connect()` | Uses a named connection, key-pair, or PAT. The connection is outbound HTTPS to Snowflake. |
| [snowflake_source.py](../snowflake_source.py#L145) `build_hwm_query()` | Selects rows between the last watermark and a ceiling captured before the read, so rows updated during the read are not skipped. |
| [snowflake_source.py](../snowflake_source.py#L111) `read_query_batches()` | Streams the result as Apache Arrow batches with `fetch_pandas_batches()`. Memory use depends on batch size, not result size. |
| [transports.py](../transports.py#L45) `pull_apply()` | Transport A: writes each Arrow batch to the target. |
| [targets.py](../targets.py#L83) `MySQLTarget.write()` | MySQL: batched `INSERT ... ON DUPLICATE KEY UPDATE`, an upsert on the primary key. |
| [targets.py](../targets.py#L398) `MSSQLTarget.write()` | SQL Server: batched inserts into a session temporary table, then one `MERGE`. |
| [change_capture.py](../change_capture.py#L13) module docstring, "High-water-mark (hwm) state" | Where the watermark is stored, its format, and the four-step cycle. |
| [change_capture.py](../change_capture.py#L110) `plan_hwm()` | The four steps in code: read the saved watermark, read the ceiling, load and commit, and only then save the new watermark. The watermark is saved only after the target commit in [sync.py](../sync.py#L181) `main()`, so a failed run leaves it unchanged and is retried from the same point. |

### Run the initial load

```bash
python sync.py --source $SRC --target DENTAL_CLAIMS \
    --change-capture hwm --hwm-col UPDATED_AT \
    --mode upsert --key-cols CLAIM_ID
```

**Expected output:**

- `HWM window: None < UPDATED_AT <= ...`. `None` means no watermark has been saved yet, so
  every row up to the ceiling is loaded.
- `HWM load complete: 20 rows, watermark advanced to ...`

### Show the watermark

The watermark is the largest `UPDATED_AT` value already delivered to the target. It is stored
in a JSON file on the job host, not in Snowflake or the target database:

```bash
cat sync_state.json
```

```json
{
  "SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS -> DENTAL_CLAIMS": {
    "watermark": "2026-10-08 08:17:10.349000"
  }
}
```

**Talking points**

- The file is keyed by source and target table (`<source> -> <target>`), so one file can
  track many source/target pairs, including the same source loaded into two targets.
- The value matches `LATEST_UPDATE` from `[S1]` in Snowsight.
- The watermark is written only after the target commit succeeds. If a run fails, the
  watermark does not move and the next run retries the same window.
- Running the same command again now transfers nothing
  (`No new rows above watermark ...`). Optionally, run it to show this.
- In production the file must be on durable storage, or the state can be kept in a
  control table or a scheduler variable instead. Deleting the file causes a full reload.
  Use `--hwm-start` to set a starting point for the first run instead.

**TablePlus:** refresh `DENTAL_CLAIMS` (Cmd+R). It contains 20 rows that match Snowflake.

---

## Part 3: Transport B, unload and bulk load

### Code walkthrough

| File | What to show |
|---|---|
| [transports.py](../transports.py#L107) `unload_apply()` | Transport B: unload to the stage, retrieve the files, and bulk load. Change capture is the same code as Transport A; only `--transport unload` differs. |
| [snowflake_source.py](../snowflake_source.py#L291) `unload_to_stage()` | `COPY INTO @stage` writes gzip-compressed CSV files. Snowflake does the export work, in parallel for large results. NULLs are written as a sentinel so they can be told apart from empty strings. |
| [snowflake_source.py](../snowflake_source.py#L327) `get_files()` | `GET` downloads the files over outbound HTTPS. With an external stage in production, the job reads the bucket with the cloud provider's SDK instead. |
| [targets.py](../targets.py#L142) `MySQLTarget.bulk_load()` | MySQL: `LOAD DATA LOCAL INFILE ... REPLACE`, the native bulk loader, with sentinel values converted back to NULL. |
| [targets.py](../targets.py#L428) `MSSQLTarget.bulk_load()` | SQL Server: `BULK INSERT` (or client-side batches) into a text temporary table, sentinel values converted back to NULL, then conversion into a typed temporary table indexed on the key and one `MERGE`. |

### Run the initial load

```bash
python sync.py --transport unload --source $SRC --target DENTAL_CLAIMS_STAGED \
    --change-capture hwm --hwm-col UPDATED_AT \
    --mode upsert --key-cols CLAIM_ID \
    --stage @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE \
    --state-file sync_state_unload.json --local-dir _unload_tmp --keep-files
```

`--keep-files` keeps the unloaded files on the stage and in `_unload_tmp` so they can be
shown next. Without it, a successful run deletes them.

**Expected output:**

- `Unloaded 20 rows to @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE/dental_claims_staged/upsert/`
- `Bulk-loaded 1 file(s) (20 rows) -> DENTAL_CLAIMS_STAGED`
- `HWM load complete: 20 rows, watermark advanced to ...`

### Show the files on the stage

**Snowsight: run `[S3]`.**

- `LIST @UNLOAD_STAGE` shows `dental_claims_staged/upsert/data_0_0_0.csv.gz` with its size
  and MD5.
- The second query reads the compressed file in place and shows its 20 rows.

`UNLOAD_STAGE` is a Snowflake internal stage. In production this is usually an external
stage on Amazon S3, Azure Blob Storage, or Google Cloud Storage, and the files would appear
in that bucket. The firewall implications are described in the
[top-level README](../README.md#transport-b-with-an-external-stage). You can also browse
the files from the `UNLOAD_STAGE` page in the Snowsight object explorer
(`SIMPLE_REVERSE_ETL_DEMO` » `DENTAL` » Stages).

### Show the files retrieved on premises

```bash
ls -l _unload_tmp/upsert
gzip -dc _unload_tmp/upsert/*.gz | head -5
```

The compressed file is what was transferred. The decompressed CSV is what the target loaded.
On SQL Server with `bulk_insert`, a `.utf16` copy also appears: `BULK INSERT` reads that
UTF-16 version so non-ASCII text loads correctly on every platform.

**TablePlus:** refresh `DENTAL_CLAIMS_STAGED`. It contains the same 20 rows as
`DENTAL_CLAIMS`.

---

## Part 4: Incremental load on both transports

### Change the source

**Snowsight: run `[S2]`.**

- Claims 1 and 2 are set to `PAID` with the amount increased by 100.
- Claims 21, 22, and 23 are inserted.
- Claim 10 is deleted.
- The first query shows the five changed rows with a current `UPDATED_AT`; claim 10 is
  not returned. The second query returns `ROW_COUNT = 22`.

### Run both transports again

Use the same commands as before (press the Up arrow in the terminal):

```bash
python sync.py --source $SRC --target DENTAL_CLAIMS \
    --change-capture hwm --hwm-col UPDATED_AT \
    --mode upsert --key-cols CLAIM_ID

python sync.py --transport unload --source $SRC --target DENTAL_CLAIMS_STAGED \
    --change-capture hwm --hwm-col UPDATED_AT \
    --mode upsert --key-cols CLAIM_ID \
    --stage @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE \
    --state-file sync_state_unload.json --local-dir _unload_tmp --keep-files
```

**Expected output:**

- Transport A: `HWM load complete: 5 rows`
- Transport B: `Unloaded 5 rows` and `Bulk-loaded 1 file(s) (5 rows)`

Only the five changed rows were transferred, not the whole table. Nothing was sent for
claim 10: a deleted row has no `UPDATED_AT` to compare, so the watermark query simply
stops returning it.

**Show that the watermark advanced:**

```bash
cat sync_state.json sync_state_unload.json
```

Both watermarks now equal the `UPDATED_AT` of the rows changed in `[S2]`. Each transport
keeps its own state file because each one tracks delivery to a different target table.

**Snowsight: run `[S3]` again.** The stage now contains only the five-row delta file.

**TablePlus:** refresh both tables. Each contains 23 rows, one more than Snowflake.
Claims 1 and 2 show `PAID` with the updated amounts, claims 21 to 23 are present, and
claim 10 is still there even though it was deleted in Snowflake. This is the main
limitation of the watermark approach; Part 5 shows how a stream handles it. Optionally, open a SQL editor in
TablePlus (Cmd+E) and confirm that the two tables are identical:

```sql
SELECT COUNT(*) AS rows_that_differ
FROM DENTAL_CLAIMS a
LEFT JOIN DENTAL_CLAIMS_STAGED b ON b.CLAIM_ID = a.CLAIM_ID
WHERE b.CLAIM_ID IS NULL
   OR a.CLAIM_STATUS <> b.CLAIM_STATUS
   OR a.AMOUNT <> b.AMOUNT;
```

**Expected result:** `rows_that_differ = 0`.

**Optional: Snowsight `[S4]`** shows the statements the job ran in Snowflake, including the
`COPY INTO` unloads and the incremental `SELECT` statements.

---

## Part 5: Stream-based change capture

The watermark approach needs a reliable `UPDATED_AT` column and cannot see deletes: a row
deleted in Snowflake simply stops appearing in the query. A Snowflake stream records
inserts, updates, and deletes without relying on any column. This part sets up a stream,
loads a third table, `DENTAL_CLAIMS_CDC`, and then applies a delete, updates, and an
insert through the stream, once with each transport.

### Set up the stream

**Snowsight: run `[S5]`.** This is the one-time setup, the demo version of
[sql/01_snowflake_setup.sql](../sql/01_snowflake_setup.sql):

- `ALTER TABLE ... SET CHANGE_TRACKING = TRUE` enables change tracking on the source.
- `CREATE STREAM CLAIMS_STREAM` creates a standard stream. It starts empty, and
  `SYSTEM$STREAM_HAS_DATA` returns `FALSE`.
- `CREATE TRANSIENT TABLE CLAIMS_OUTBOX` creates the outbox: the source columns in the
  same order, followed by four `_CDC_*` columns.
- `SHOW STREAMS` shows the stream's `MODE` and `STALE_AFTER`.

**Talking points**

- The order matters: create the stream first, then run the initial full load, so no change
  made in between is missed. A change made between the two is simply applied again by the
  first stream run, which is harmless.
- The outbox is transient because it is short-lived working data; Fail-safe storage would
  add cost without a recovery benefit.

### Code walkthrough

| File | What to show |
|---|---|
| [snowflake_source.py](../snowflake_source.py#L165) `consume_stream_to_outbox()` | `INSERT INTO outbox SELECT ... FROM stream` in one transaction. Committing it advances the stream offset; the changes are now held in the outbox. |
| [change_capture.py](../change_capture.py#L149) `plan_stream()` | Consumes the stream, snapshots the cutoff, and plans two queries: rows to upsert and keys to delete. |
| [snowflake_source.py](../snowflake_source.py#L209) `build_outbox_changes_query()` | Runs in Snowflake: keeps one row per key, from the latest consume and, within a consume, the `INSERT` over the `DELETE` (`QUALIFY ROW_NUMBER()`). An update becomes an upsert; an update that changes a key also deletes the old key. |
| [snowflake_source.py](../snowflake_source.py#L247) `purge_outbox()` | After the target commit and the acknowledgement, deletes delivered rows (immediately by default, or after `--outbox-retention-days`). |

**Talking points**

- A stream offset advances only when a DML statement that reads the stream commits.
  Snowflake and the target database cannot share a transaction, so the outbox is the
  hand-off point: if the target write fails, the changes are still in the outbox and the
  next run applies them.
- Applying the same change twice gives the same result, so delivery is at-least-once with
  a correct final state.

### Run the initial load

A stream carries changes, not the existing data, so the target starts with a full load:

```bash
python sync.py --source $SRC --target DENTAL_CLAIMS_CDC \
    --change-capture none --mode truncate
```

**Expected output:** `Full load complete: 22 rows -> DENTAL_CLAIMS_CDC`

**Option: initial load with Transport B.** The initial load is an ordinary full load, so
it can also go through the unload path. In production this is usually the better choice,
because the initial load is the one large transfer; the stream runs that follow are small
deltas and work with either transport.

```bash
python sync.py --transport unload --source $SRC --target DENTAL_CLAIMS_CDC \
    --change-capture none --mode truncate \
    --stage @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE --local-dir _unload_tmp --keep-files
```

**Expected output:** `Unloaded 22 rows to .../dental_claims_cdc/upsert/`,
`Bulk-loaded 1 file(s) (22 rows) -> DENTAL_CLAIMS_CDC`, and
`Full load complete: 22 rows -> DENTAL_CLAIMS_CDC`.

**TablePlus:** refresh `DENTAL_CLAIMS_CDC`. It contains 22 rows, matching Snowflake. Claim 10
is not there, because the full load reads the current table, unlike the two watermark
tables.

### Delete and update in Snowflake

**Snowsight: run `[S6]`.**

- Claim 5 is deleted.
- Claim 3 changes from `DENIED` to `PAID`, and its amount increases by 50.
- The stream query shows exactly three rows: a `DELETE` for claim 5 with
  `METADATA$ISUPDATE = FALSE`, and an update pair for claim 3 (a `DELETE` with the old
  values and an `INSERT` with the new values, both with `METADATA$ISUPDATE = TRUE`).
- Selecting from the stream does not consume it. `SYSTEM$STREAM_HAS_DATA` returns `TRUE`.

### Apply the changes with Transport A

```bash
python sync.py --source $SRC --target DENTAL_CLAIMS_CDC \
    --change-capture stream \
    --stream SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_STREAM \
    --outbox SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_OUTBOX \
    --outbox-retention-days 1 \
    --mode upsert --key-cols CLAIM_ID
```

**Expected output:**

- `Consumed 3 change rows from ... CLAIMS_STREAM into ... CLAIMS_OUTBOX`
- `Purged 0 delivered row(s) from ... CLAIMS_OUTBOX (retention 1 day(s))`
- `Stream CDC complete: 1 upserts, 1 deletes -> DENTAL_CLAIMS_CDC`

The three stream rows became one upsert (claim 3, new values only) and one delete
(claim 5).

**TablePlus:** refresh `DENTAL_CLAIMS_CDC`.

- It contains 21 rows. Claim 5 is gone, and claim 3 shows `PAID`.
- Compare with `DENTAL_CLAIMS`, which was loaded with the watermark: claims 5 and 10 are
  still there. A watermark cannot detect deletes, and claim 3 is unchanged there until the next
  watermark run.

**Snowsight:** run the stream query from `[S6]` again. The stream is now empty.

### Insert and update in Snowflake

**Snowsight: run `[S7]`.** Claim 24 is inserted, and claim 4 is set to `PAID` with its
amount increased by 25. The stream query shows one `INSERT` for claim 24 and an update
pair for claim 4.

### Apply the changes with Transport B

The same stream change capture works through the unload path:

```bash
python sync.py --transport unload --source $SRC --target DENTAL_CLAIMS_CDC \
    --change-capture stream \
    --stream SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_STREAM \
    --outbox SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_OUTBOX \
    --outbox-retention-days 1 \
    --mode upsert --key-cols CLAIM_ID \
    --stage @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE --local-dir _unload_tmp --keep-files
```

**Expected output:**

- `Consumed 3 change rows`
- `Unloaded 2 rows to @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE/dental_claims_cdc/upsert/`
- `Unloaded 0 rows to .../dental_claims_cdc/delete/` (this run has no deletes)
- `Bulk-loaded 1 file(s) (2 rows) -> DENTAL_CLAIMS_CDC`
- `Purged 0 delivered row(s) from ... CLAIMS_OUTBOX (retention 1 day(s))`
- `Stream CDC complete: 2 upserts, 0 deletes -> DENTAL_CLAIMS_CDC`

**Snowsight: run `[S3]`.** `LIST @UNLOAD_STAGE` shows a file under `dental_claims_cdc/upsert/`
with the rows for claims 4 and 24. When a run includes deletes, a file with the deleted
keys appears under `dental_claims_cdc/delete/` as well.

**TablePlus:** refresh `DENTAL_CLAIMS_CDC`. It contains 22 rows, matching Snowflake: claim
24 is present, claim 4 shows `PAID`, and claim 5 is still gone.

### Show the outbox

**Snowsight: run `[S8]`.** The outbox lists the six changes consumed by the two runs,
grouped by `_CDC_LOADED_AT`, with `_CDC_EXPORTED = TRUE` for all rows. Each update appears
as its `DELETE` and `INSERT` halves.

**Talking point:** the demo commands pass `--outbox-retention-days 1` so these delivered
rows stay visible. By default the job deletes delivered rows right after each successful
run, so in production the outbox only holds changes that have not been delivered yet.

Optionally, run the stream command once more. It reports `Consumed 0 change rows` and
`Stream CDC: no un-exported changes`.

---

## Part 6: Summary

| | Transport A: connector pull | Transport B: unload and bulk load |
|---|---|---|
| Best for | Incremental deltas and moderate volumes | Large full or incremental loads |
| Load method | Batched upsert statements | Native bulk loader |
| After a failure | Rerun; the window is queried again | Rerun; the window is unloaded again (a failed run's files stay in `_unload_tmp` for inspection) |
| Change capture | `none`, `hwm`, `stream` | `none`, `hwm`, `stream` |
| Target load | MySQL `INSERT ... ON DUPLICATE KEY UPDATE`; SQL Server `MERGE` | MySQL `LOAD DATA LOCAL INFILE`; SQL Server `BULK INSERT` |
| Job host connects to | Snowflake (and stage storage) | Snowflake and stage storage, or only the bucket if Snowflake schedules the unload (described in the main README, not implemented) |

| | Watermark (`hwm`) | Stream |
|---|---|---|
| Needs | A reliable `UPDATED_AT` column | Change tracking on the table |
| Captures deletes | No | Yes |
| Position kept in | JSON file on the job host | Stream offset and outbox in Snowflake |
| Main risk | Updates that do not change the column are missed | Stream goes stale if not consumed within the retention period |

Close with the network caveat: no inbound access to the data center is required, but the
job host does need approved outbound access to Snowflake, to the stage storage, or to both.
Use `SYSTEM$ALLOWLIST()` to get the list of hosts for an account.

---

## Part 7: Volume test (optional, not for the live session)

The 20-row table keeps the demo readable but says nothing about throughput. Part 7 loads
a wide, realistic table at volume on both transports and times it. It takes too long to
run during a presentation: about 15 minutes on MySQL and about 45 minutes on SQL Server
under emulation. Run it beforehand and show the results, or use it to test performance
after changing the code or the target configuration. What it showed is summarized in
[Performance](../README.md#performance) in the main README.

**What it does:**

1. **`[S9]`** creates `VOLUME_CLAIMS` in Snowflake: 10,000,000 fictitious dental claims
   with 21 columns (IDs, plan and procedure codes, dates, four amounts, a nullable denial
   reason, a free-text note of up to 400 characters, a `BOOLEAN`, and `UPDATED_AT`).
   About 30% of the notes are NULL and some are empty strings or contain quotes and
   commas. Generation takes about 20 seconds.
2. **Initial load.** Both transports load the full table with the production command
   (`--change-capture hwm --mode upsert`; there is no watermark yet, so every row is
   sent): Transport A into `VOLUME_CLAIMS`, Transport B into `VOLUME_CLAIMS_STAGED`.
3. **`[S10]`** makes a large upstream change: it inserts 5,000,000 new claims and updates
   1,000,000 existing ones (status, paid amounts, denial reason, note).
4. **Incremental load.** Both transports run again. The watermark selects exactly the
   6,000,000 changed rows, which are applied as upserts to tables that already hold
   10,000,000 rows.

After steps 2 and 4, [volume_check.py](volume_check.py) compares 19 aggregates of each
target table with Snowflake: row count, key range, the four amount sums, rows per status,
NULL and empty-string counts, total note length, the date range, and the `BOOLEAN` count.
A lost update, a truncated note, or a mis-converted value changes at least one of them.

**Run it:**

```bash
./demo/run_volume.sh                                           # MySQL
DEMO_TARGET=mssql ./demo/run_volume.sh                         # SQL Server, BULK INSERT
DEMO_TARGET=mssql TARGET_MSSQL_LOAD_METHOD=client ./demo/run_volume.sh
```

The script ends with a timing summary and exits non-zero if any check fails or if an
incremental run does not send exactly 6,000,000 rows. Transport B needs about 3 GB free
in `_unload_tmp` (more for SQL Server `bulk_insert`, which writes a UTF-16 copy of each
file). The individual commands, if you want to run a step by hand in Snowsight and the
terminal:

```bash
SRC=SIMPLE_REVERSE_ETL_DEMO.DENTAL.VOLUME_CLAIMS
time python sync.py --source $SRC --target VOLUME_CLAIMS \
    --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID \
    --state-file sync_state_volume_a.json

time python sync.py --transport unload --source $SRC --target VOLUME_CLAIMS_STAGED \
    --change-capture hwm --hwm-col UPDATED_AT --mode upsert --key-cols CLAIM_ID \
    --stage @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE \
    --state-file sync_state_volume_b.json --local-dir _unload_tmp
```

**Measured results** (Apple silicon laptop, Docker with 8 CPUs and 8 GB, Snowflake small
warehouse; seconds, with rows per second in parentheses):

| Target | Initial load, 10M rows: A | Initial load: B | Incremental, 5M inserts + 1M updates: A | Incremental: B |
|---|---|---|---|---|
| MySQL 8.4 | 260 s (38k/s) | 127 s (79k/s) | 165 s (36k/s) | 99 s (61k/s) |
| SQL Server 2022, `bulk_insert` | 505 s (20k/s) | 518 s (19k/s) | 298 s (20k/s) | 355 s (17k/s) |
| SQL Server 2022, `client` | 489 s (20k/s) | 764 s (13k/s) | 296 s (20k/s) | 507 s (12k/s) |

All runs matched Snowflake on every check. Run-to-run variation on the emulated SQL Server
container was large: Transport A's initial load took 387 s in one run and 505 s in
another with the same code.

`reset_demo.sh` drops `VOLUME_CLAIMS` and empties the two volume target tables.

---

## Troubleshooting

| Symptom | Resolution |
|---|---|
| `Cannot connect to the Docker daemon` | Start Docker Desktop, wait until it is running, and run `./demo/reset_demo.sh` again. |
| `No new rows above watermark` | The source has not changed since the last run. Run `[S2]` in Snowsight, or reset the demo. |
| Snowflake authentication prompt or failure | Check `demo/.env.demo`. Test the connection with `snow connection test -c <connection name>`. |
| `$SRC` is empty | Run the three terminal preparation commands from [Reset before each presentation](#reset-before-each-presentation) in the current terminal. |
| TablePlus shows old data | Refresh the table with Cmd+R. |
| Snowsight reports that `UNLOAD_STAGE` or `UNLOAD_CSV` does not exist | Run `./demo/reset_demo.sh`. It creates these objects. |
| Snowsight or a stream run reports that `CLAIMS_STREAM` or `CLAIMS_OUTBOX` does not exist | Run `[S5]`. The reset drops both so that they can be created live in Part 5. |
| Stream run reports `Consumed 0 change rows` unexpectedly | The changes were already consumed by an earlier run, or were made before `[S5]` created the stream. Make another change in Snowsight, or reset the demo. |
| Stream run fails because the stream is stale or its source table was replaced | Run `./demo/reset_demo.sh`, then `[S5]`, and start Part 5 again. |
| TablePlus cannot connect | Use host `127.0.0.1`, not `localhost`, confirm the container is running with `docker ps`, and check that the connection type matches the target (MySQL on 3306, SQL Server on 1433). |
| SQL Server: `Login failed` or connection refused right after a reset | SQL Server is still starting under emulation. Wait a minute and retry. |
| SQL Server: `Can't open lib 'ODBC Driver 18 for SQL Server'` or an OpenSSL load error | Install the driver (`brew tap microsoft/mssql-release && brew install msodbcsql18`). On macOS with `openssl@4` installed, point `/opt/homebrew/opt/openssl` at `openssl@3`; see [SQL Server targets](../README.md#sql-server-targets). |
| SQL Server: `BULK INSERT` cannot open the file | The container must see `_unload_tmp` at `/var/opt/unload`. If `_unload_tmp` was deleted while the container was running, recreate it: `docker compose -f demo/docker-compose.yml --profile mssql up -d --force-recreate mssql`. Or use `TARGET_MSSQL_LOAD_METHOD=client`. |
| MySQL: `Loading local data is disabled` | The server must run with `local_infile` enabled. The demo container does; run `./demo/reset_demo.sh` to restart it with the demo settings. |

## Cleanup

After the presentation, stop the target containers (MySQL and SQL Server) and remove their
data, then remove the local state and retrieved files:

```bash
docker compose -f demo/docker-compose.yml --profile mssql down -v
rm -rf sync_state.json sync_state_unload.json sync_state_volume_*.json _unload_tmp
```

To remove the Snowflake objects, run the following in Snowsight. This includes the stage
and its files.

```sql
DROP DATABASE IF EXISTS SIMPLE_REVERSE_ETL_DEMO;
```
