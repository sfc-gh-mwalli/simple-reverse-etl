-- ============================================================================
-- Snowflake-side setup for the --change-capture=stream (CDC) path.
--
-- Not needed for --change-capture=none (full) or =hwm (timestamp watermark).
-- Run once as a role that can ALTER the source table and create objects in the
-- export schema. Replace ANALYTICS.DENTAL.CLAIMS with your real source object.
--
-- Why an "outbox" table instead of reading the stream directly from on-prem:
-- a stream's offset only advances when its rows are CONSUMED by a DML statement
-- (a plain SELECT does NOT advance it). We cannot safely advance the offset
-- before the on-prem write to MySQL/SQL Server has succeeded. So the job:
--   1. consumes the stream INTO this outbox (offset advances atomically here),
--   2. reads the net change per key from the outbox (the old-value half of each
--      UPDATE dropped, latest change per key kept) and writes it to the target,
--   3. after the target commit, marks those rows exported and deletes delivered
--      rows (immediately by default; --outbox-retention-days N keeps N days).
-- If the on-prem write fails, the changes are still safely in the outbox and
-- get retried on the next run (at-least-once + idempotent upsert on the key).
-- With the default, the outbox only ever holds changes not yet delivered.
-- ============================================================================

-- 1. Enable change tracking on the source (a stream also enables it implicitly).
ALTER TABLE ANALYTICS.DENTAL.CLAIMS SET CHANGE_TRACKING = TRUE;

-- 2. Standard (delta) stream: tracks INSERT / UPDATE / DELETE.
--    Do not use APPEND_ONLY = TRUE here: an append-only stream does not report
--    updates or deletes, so they would never reach the target.
CREATE STREAM IF NOT EXISTS ANALYTICS.DENTAL.CLAIMS_STREAM
  ON TABLE ANALYTICS.DENTAL.CLAIMS;

-- 3. Outbox: source columns + CDC metadata + an export marker.
--    The consume step inserts BY POSITION, so list the source columns in exactly
--    the same order as the source table, then the four _CDC_* columns last.
--
--    TRANSIENT: the outbox is short-lived working data, not a system of record,
--    so it skips Fail-safe storage. One day of Time Travel still allows UNDROP
--    after an accident. If the outbox is lost, recover by recreating the stream
--    and running a full load. Use a permanent table instead only if delivered
--    rows are kept (--outbox-retention-days) as an audit trail.
--    An existing permanent outbox can be recreated as transient once it has no
--    un-exported rows (SELECT COUNT_IF(NOT _CDC_EXPORTED) FROM <outbox> = 0).
--
--    Privileges for the job's role: SELECT, INSERT, UPDATE, DELETE.
CREATE TRANSIENT TABLE IF NOT EXISTS ANALYTICS.DENTAL.CLAIMS_OUTBOX (
    -- <<< source columns here, same names/types as ANALYTICS.DENTAL.CLAIMS >>>
    -- e.g.  CLAIM_ID       NUMBER,
    --       MEMBER_ID      NUMBER,
    --       CLAIM_STATUS   VARCHAR,
    --       UPDATED_AT     TIMESTAMP_NTZ,
    _CDC_ACTION    STRING,        -- 'INSERT' or 'DELETE'
    _CDC_ISUPDATE  BOOLEAN,       -- TRUE => this row is one half of an UPDATE
    _CDC_LOADED_AT TIMESTAMP_LTZ, -- when the job consumed it into the outbox
    _CDC_EXPORTED  BOOLEAN DEFAULT FALSE
)
DATA_RETENTION_TIME_IN_DAYS = 1;

-- 4. Initial load: after creating the stream, run a full load once:
--      python sync.py ... --change-capture none --mode truncate
--    then switch to --change-capture stream for subsequent runs.

-- The job runs the consume step itself (shown here for reference):
--
--   INSERT INTO ANALYTICS.DENTAL.CLAIMS_OUTBOX
--   SELECT s.* EXCLUDE (METADATA$ROW_ID), CURRENT_TIMESTAMP(), FALSE
--   FROM ANALYTICS.DENTAL.CLAIMS_STREAM AS s;
--
-- (A stream's * includes METADATA$ACTION, METADATA$ISUPDATE and METADATA$ROW_ID;
--  the first two map to the outbox's _CDC_ACTION/_CDC_ISUPDATE, ROW_ID is dropped.)
-- ...committing that INSERT is what advances the stream offset.

-- ----------------------------------------------------------------------------
-- Simpler alternative to a stream when you don't want offset bookkeeping:
-- the CHANGES clause reads change-tracking metadata between two timestamps and
-- does NOT advance any offset (you track the last processed time yourself,
-- same idea as the =hwm path). Requires CHANGE_TRACKING = TRUE (step 1 only).
--
--   SELECT *, METADATA$ACTION, METADATA$ISUPDATE
--   FROM ANALYTICS.DENTAL.CLAIMS
--   CHANGES (INFORMATION => DEFAULT)
--   AT (TIMESTAMP => :last_run_ts)
--   END (TIMESTAMP => :ceiling_ts);
-- ----------------------------------------------------------------------------
