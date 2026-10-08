# SimpleReverseETL demonstration runbook

This runbook is a presenter script for demonstrating both SimpleReverseETL transports and
both incremental change-capture methods. For each transport it shows the source data in
Snowflake, walks through the relevant code, runs a full load and an incremental load into
MySQL, and shows the result in TablePlus. For Transport B it also shows the unloaded files
on the stage. A final part shows stream-based change capture, including deletes.

For architecture, network requirements, and production guidance, see the
[top-level README](../README.md). For an unattended run of the whole sequence, use
`./demo/run_demo.sh`.

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

3. **TablePlus.** Install TablePlus (`brew install --cask tableplus`) and create a MySQL
   connection named **SimpleReverseETL Demo**:

   | Field | Value |
   |---|---|
   | Host | `127.0.0.1` |
   | Port | `3306` |
   | User | `root` |
   | Password | `demopw` |
   | Database | `DENTAL_RPT` |

4. **Snowsight.** Open a new SQL worksheet, paste in the contents of
   [demo/snowsight_demo.sql](snowsight_demo.sql), and set the worksheet role and warehouse
   to match `demo/.env.demo`. Each section of the worksheet is labeled `[S1]` to `[S7]`.
   The worksheet references objects created by the reset step, so run the reset once
   before using it.

5. **Screen layout.** Arrange four windows: a terminal in the repository root, the code
   editor, Snowsight, and TablePlus.

## Reset before each presentation

In the terminal, from the repository root:

```bash
./demo/reset_demo.sh
```

The reset script:

- starts MySQL, creates any missing target tables, and empties `DENTAL_CLAIMS`,
  `DENTAL_CLAIMS_STAGED`, and `DENTAL_CLAIMS_CDC`
- recreates the Snowflake source with 20 rows, recreates the stream `CLAIMS_STREAM` and the
  outbox table `CLAIMS_OUTBOX`, and creates and empties the stage `UNLOAD_STAGE`
- removes the local watermark files and any previously retrieved files

Then prepare the terminal session that you will present from:

```bash
set -a; source demo/.env.demo; set +a
source demo/.venv/bin/activate
export SRC=SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS
```

**Check:** the script ends with `Reset complete`. In TablePlus, all three tables are empty.

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
| [snowflake_source.py](../snowflake_source.py#L103) `build_hwm_query()` | Selects rows between the last watermark and a ceiling captured before the read, so rows updated during the read are not skipped. |
| [snowflake_source.py](../snowflake_source.py#L76) `read_query_batches()` | Streams the result as Apache Arrow batches with `fetch_pandas_batches()`. Memory use depends on batch size, not result size. |
| [targets.py](../targets.py#L47) `MySQLTarget.write()` | Batched `INSERT ... ON DUPLICATE KEY UPDATE`, which is an upsert on the primary key. |
| [sync.py](../sync.py#L18) module docstring, "High-water-mark (hwm) state" | Where the watermark is stored, its format, and the four-step cycle. |
| [sync.py](../sync.py#L123) `run_hwm()` | The four steps in code: read the saved watermark, read the ceiling, load and commit, and only then save the new watermark. A failed run leaves the watermark unchanged and is retried from the same point. |

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

The watermark is the largest `UPDATED_AT` value already delivered to MySQL. It is stored
in a JSON file on the job host, not in Snowflake or MySQL:

```bash
cat sync_state.json
```

```json
{
  "SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS": {
    "watermark": "2026-10-08 08:17:10.349000"
  }
}
```

**Talking points**

- The file is keyed by source table, so one file can track many tables.
- The value matches `LATEST_UPDATE` from `[S1]` in Snowsight.
- The watermark is written only after the MySQL commit succeeds. If a run fails, the
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
| [unload_sync.py](../unload_sync.py#L72) `run()` | Four steps: unload to the stage, retrieve the files, bulk load, and save the watermark after commit. |
| [snowflake_source.py](../snowflake_source.py#L191) `unload_to_stage()` | `COPY INTO @stage` writes gzip-compressed CSV files. Snowflake does the export work, in parallel for large results. NULLs are written as a sentinel so they can be told apart from empty strings. |
| [snowflake_source.py](../snowflake_source.py#L224) `get_files()` | `GET` downloads the files over outbound HTTPS. With an external stage in production, the job reads the bucket with the cloud provider's SDK instead. |
| [targets.py](../targets.py#L72) `MySQLTarget.bulk_load()` | `LOAD DATA LOCAL INFILE ... REPLACE`, the native MySQL bulk loader, with sentinel values converted back to NULL. |

### Run the initial load

```bash
python unload_sync.py --source $SRC --target DENTAL_CLAIMS_STAGED \
    --change-capture hwm --hwm-col UPDATED_AT \
    --mode upsert --key-cols CLAIM_ID \
    --stage @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE \
    --state-file sync_state_unload.json --local-dir _unload_tmp
```

**Expected output:**

- `Unloaded 20 rows to @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE/dental_claims_staged/`
- `Bulk-loaded 1 file(s) (20 rows) -> DENTAL_CLAIMS_STAGED`

### Show the files on the stage

**Snowsight: run `[S3]`.**

- `LIST @UNLOAD_STAGE` shows `dental_claims_staged/data_0_0_0.csv.gz` with its size and
  MD5.
- The second query reads the compressed file in place and shows its 20 rows.

`UNLOAD_STAGE` is a Snowflake internal stage. In production this is usually an external
stage on Amazon S3, Azure Blob Storage, or Google Cloud Storage, and the files would appear
in that bucket. The firewall implications are described in the
[top-level README](../README.md#transport-b-with-an-external-stage). You can also browse
the files from the `UNLOAD_STAGE` page in the Snowsight object explorer
(`SIMPLE_REVERSE_ETL_DEMO` » `DENTAL` » Stages).

### Show the files retrieved on premises

```bash
ls -l _unload_tmp
gzip -dc _unload_tmp/*.gz | head -5
```

The compressed file is what was transferred. The decompressed CSV is what MySQL loaded.

**TablePlus:** refresh `DENTAL_CLAIMS_STAGED`. It contains the same 20 rows as
`DENTAL_CLAIMS`.

---

## Part 4: Incremental load on both transports

### Change the source

**Snowsight: run `[S2]`.**

- Claims 1 and 2 are set to `PAID` with the amount increased by 100.
- Claims 21, 22, and 23 are inserted.
- The final query shows these five rows with a current `UPDATED_AT`.

### Run both transports again

Use the same commands as before (press the Up arrow in the terminal):

```bash
python sync.py --source $SRC --target DENTAL_CLAIMS \
    --change-capture hwm --hwm-col UPDATED_AT \
    --mode upsert --key-cols CLAIM_ID

python unload_sync.py --source $SRC --target DENTAL_CLAIMS_STAGED \
    --change-capture hwm --hwm-col UPDATED_AT \
    --mode upsert --key-cols CLAIM_ID \
    --stage @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE \
    --state-file sync_state_unload.json --local-dir _unload_tmp
```

**Expected output:**

- Transport A: `HWM load complete: 5 rows`
- Transport B: `Unloaded 5 rows` and `Bulk-loaded 1 file(s) (5 rows)`

Only the five changed rows were transferred, not the whole table.

**Show that the watermark advanced:**

```bash
cat sync_state.json sync_state_unload.json
```

Both watermarks now equal the `UPDATED_AT` of the rows changed in `[S2]`. Each transport
keeps its own state file because each one tracks delivery to a different target table.

**Snowsight: run `[S3]` again.** The stage now contains only the five-row delta file.

**TablePlus:** refresh both tables. Each contains 23 rows. Claims 1 and 2 show `PAID` with
the updated amounts, and claims 21 to 23 are present. Optionally, open a SQL editor in
TablePlus (Cmd+E) and confirm that the two tables are identical:

```sql
SELECT COUNT(*) AS rows_that_differ
FROM DENTAL_CLAIMS a
LEFT JOIN DENTAL_CLAIMS_STAGED b USING (CLAIM_ID)
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
inserts, updates, and deletes without relying on any column. This part loads a third MySQL
table, `DENTAL_CLAIMS_CDC`, from the same source using the stream.

### Show the pending changes in the stream

The reset created `CLAIMS_STREAM` right after seeding the source, so it has been recording
every change since, including the ones made in `[S2]`.

**Snowsight: run `[S5]`.**

- Claims 21, 22, and 23 appear once, as `INSERT` with `METADATA$ISUPDATE = FALSE`.
- Claims 1 and 2 each appear twice, with `METADATA$ISUPDATE = TRUE`: a `DELETE` row with
  the old values and an `INSERT` row with the new values. This is how a stream reports an
  update.
- Selecting from the stream does not consume it. `SYSTEM$STREAM_HAS_DATA` returns `TRUE`.

### Code walkthrough

| File | What to show |
|---|---|
| [sql/01_snowflake_setup.sql](../sql/01_snowflake_setup.sql) | One-time setup: change tracking, the stream, and the outbox table. The demo versions are at the end of [setup_snowflake.sql](setup_snowflake.sql). |
| [snowflake_source.py](../snowflake_source.py#L122) `consume_stream_to_outbox()` | `INSERT INTO outbox SELECT ... FROM stream` in one transaction. Committing it advances the stream offset; the changes are now held in the outbox. |
| [sync.py](../sync.py#L151) `run_stream()` | Reads unexported outbox rows, drops the old-value half of each update, keeps the latest change per key, upserts and deletes in one MySQL transaction, and only then marks the outbox rows exported. |

**Talking points**

- A stream offset advances only when a DML statement that reads the stream commits.
  Snowflake and MySQL cannot share a transaction, so the outbox is the hand-off point: if
  the MySQL write fails, the changes are still in the outbox and the next run applies them.
- Applying the same change twice gives the same result, so delivery is at-least-once with
  a correct final state.

### Run the initial load

A stream carries changes, not the existing data, so the target starts with a full load:

```bash
python sync.py --source $SRC --target DENTAL_CLAIMS_CDC \
    --change-capture none --mode truncate
```

**Expected output:** `Full load complete: 23 rows -> DENTAL_CLAIMS_CDC`

### Run the stream synchronization

```bash
python sync.py --source $SRC --target DENTAL_CLAIMS_CDC \
    --change-capture stream \
    --stream SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_STREAM \
    --outbox SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_OUTBOX \
    --mode upsert --key-cols CLAIM_ID
```

**Expected output:**

- `Consumed 7 change rows from ... CLAIMS_STREAM into ... CLAIMS_OUTBOX`
- `Stream CDC complete: 5 upserts, 0 deletes -> DENTAL_CLAIMS_CDC`

The full load already contained these five changes, because they were made before it ran.
Applying them again leaves the table unchanged, which illustrates why it is safe to
overlap the initial load with the stream.

**Snowsight: run the `[S5]` queries again.** The stream is now empty, and
`SYSTEM$STREAM_HAS_DATA` returns `FALSE`.

### Delete and update in Snowflake

**Snowsight: run `[S6]`.**

- Claim 5 is deleted.
- Claim 3 changes from `DENIED` to `PAID`, and its amount increases by 50.
- The final query shows exactly three stream rows: a `DELETE` for claim 5 with
  `METADATA$ISUPDATE = FALSE`, and an update pair for claim 3.

### Run the stream synchronization again

Run the same stream command (press the Up arrow).

**Expected output:**

- `Consumed 3 change rows`
- `Stream CDC complete: 1 upserts, 1 deletes -> DENTAL_CLAIMS_CDC`

**TablePlus:** refresh `DENTAL_CLAIMS_CDC`.

- It contains 22 rows, matching Snowflake. Claim 5 is gone, and claim 3 shows `PAID`.
- Compare with `DENTAL_CLAIMS`, which was loaded with the watermark: claim 5 is still
  there. A watermark cannot detect deletes, and claim 3 is unchanged there until the next
  watermark run.

### Show the outbox

**Snowsight: run `[S7]`.** The outbox lists every change consumed by the two runs, grouped
by `_CDC_LOADED_AT`, with `_CDC_EXPORTED = TRUE` for all rows. Each update appears as its
`DELETE` and `INSERT` halves.

Optionally, run the stream command once more. It reports `Consumed 0 change rows` and
`0 upserts, 0 deletes`.

---

## Part 6: Summary

| | Transport A: connector pull | Transport B: unload and bulk load |
|---|---|---|
| Best for | Incremental deltas and moderate volumes | Large full or incremental loads |
| Load method | Batched upsert statements | Native bulk loader |
| Recovery | Re-query Snowflake | Reload from retrieved files |
| Change capture | `none`, `hwm`, `stream` | `none`, `hwm` |
| Job host connects to | Snowflake (and stage storage) | Snowflake and stage storage, or only the bucket when Snowflake schedules the unload |

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

## Troubleshooting

| Symptom | Resolution |
|---|---|
| `Cannot connect to the Docker daemon` | Start Docker Desktop, wait until it is running, and run `./demo/reset_demo.sh` again. |
| `No new rows above watermark` | The source has not changed since the last run. Run `[S2]` in Snowsight, or reset the demo. |
| Snowflake authentication prompt or failure | Check `demo/.env.demo`. Test the connection with `snow connection test -c <connection name>`. |
| `$SRC` is empty | Run the three terminal preparation commands from [Reset before each presentation](#reset-before-each-presentation) in the current terminal. |
| TablePlus shows old data | Refresh the table with Cmd+R. |
| Snowsight reports that `UNLOAD_STAGE`, `UNLOAD_CSV`, `CLAIMS_STREAM`, or `CLAIMS_OUTBOX` does not exist | Run `./demo/reset_demo.sh`. It creates these objects. |
| Stream run reports `Consumed 0 change rows` unexpectedly | The changes were already consumed by an earlier run. Run `[S6]` again with a different claim, or reset the demo. |
| Stream run fails because the stream is stale or its source table was replaced | Run `./demo/reset_demo.sh`, which recreates the source table and the stream together. |
| TablePlus cannot connect | Use host `127.0.0.1`, not `localhost`, and confirm the container is running with `docker ps`. |

## Cleanup

After the presentation, stop MySQL and remove its data:

```bash
docker compose -f demo/docker-compose.yml down -v
rm -rf sync_state.json sync_state_unload.json _unload_tmp
```

To remove the Snowflake objects, run the following in Snowsight. This includes the stage
and its files.

```sql
DROP DATABASE IF EXISTS SIMPLE_REVERSE_ETL_DEMO;
```
