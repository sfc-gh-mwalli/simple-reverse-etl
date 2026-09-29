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
--   2. drains the outbox from on-prem and writes to the target,
--   3. marks the drained rows exported.
-- If the on-prem write fails, the changes are still safely in the outbox and
-- get retried on the next run (at-least-once + idempotent upsert on the key).
-- ============================================================================

-- 1. Enable change tracking on the source (a stream also enables it implicitly).
ALTER TABLE ANALYTICS.DENTAL.CLAIMS SET CHANGE_TRACKING = TRUE;

-- 2. Standard (delta) stream: tracks INSERT / UPDATE / DELETE.
--    Use APPEND_ONLY = TRUE instead if the source is insert-only (faster).
CREATE STREAM IF NOT EXISTS ANALYTICS.DENTAL.CLAIMS_STREAM
  ON TABLE ANALYTICS.DENTAL.CLAIMS;

-- 3. Outbox: source columns + CDC metadata + an export marker.
--    Match the source column list; the three _CDC_* columns are added by the job.
CREATE TABLE IF NOT EXISTS ANALYTICS.DENTAL.CLAIMS_OUTBOX (
    -- <<< source columns here, same names/types as ANALYTICS.DENTAL.CLAIMS >>>
    -- e.g.  CLAIM_ID       NUMBER,
    --       MEMBER_ID      NUMBER,
    --       CLAIM_STATUS   VARCHAR,
    --       UPDATED_AT     TIMESTAMP_NTZ,
    _CDC_ACTION    STRING,        -- 'INSERT' or 'DELETE'
    _CDC_ISUPDATE  BOOLEAN,       -- TRUE => this row is one half of an UPDATE
    _CDC_LOADED_AT TIMESTAMP_LTZ, -- when the job consumed it into the outbox
    _CDC_EXPORTED  BOOLEAN DEFAULT FALSE
);

-- The job runs the consume step itself (shown here for reference):
--
--   INSERT INTO ANALYTICS.DENTAL.CLAIMS_OUTBOX
--   SELECT s.*, METADATA$ACTION, METADATA$ISUPDATE, CURRENT_TIMESTAMP(), FALSE
--   FROM ANALYTICS.DENTAL.CLAIMS_STREAM AS s;
--
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
