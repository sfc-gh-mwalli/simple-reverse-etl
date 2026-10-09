-- ---------------------------------------------------------------------------
-- SimpleReverseETL demonstration: Snowsight worksheet.
-- Run each section when the runbook (demo/README.md) refers to it.
-- ---------------------------------------------------------------------------
USE DATABASE SIMPLE_REVERSE_ETL_DEMO;
USE SCHEMA DENTAL;

-- [S1] Source data (expect 20 rows) ------------------------------------------
SELECT * FROM DENTAL_CLAIMS ORDER BY CLAIM_ID;

SELECT COUNT(*) AS ROW_COUNT, MAX(UPDATED_AT) AS LATEST_UPDATE FROM DENTAL_CLAIMS;

-- [S2] Simulate upstream changes: 2 updates, 3 inserts, 1 delete ------------
UPDATE DENTAL_CLAIMS
   SET CLAIM_STATUS = 'PAID',
       AMOUNT       = AMOUNT + 100.00,
       UPDATED_AT   = CURRENT_TIMESTAMP()
 WHERE CLAIM_ID IN (1, 2);

INSERT INTO DENTAL_CLAIMS (CLAIM_ID, MEMBER_ID, CLAIM_STATUS, AMOUNT, UPDATED_AT)
VALUES
    (21, 55555, 'SUBMITTED', 321.00, CURRENT_TIMESTAMP()),
    (22, 66666, 'SUBMITTED', 654.00, CURRENT_TIMESTAMP()),
    (23, 77777, 'DENIED',    987.00, CURRENT_TIMESTAMP());

-- A watermark cannot see this delete: claim 10 stays in both hwm targets.
DELETE FROM DENTAL_CLAIMS WHERE CLAIM_ID = 10;

-- Rows that the next incremental run will pick up (claim 10 is gone)
SELECT * FROM DENTAL_CLAIMS
 WHERE CLAIM_ID IN (1, 2, 10, 21, 22, 23)
 ORDER BY CLAIM_ID;

SELECT COUNT(*) AS ROW_COUNT FROM DENTAL_CLAIMS;  -- 22

-- [S3] Files unloaded by Transport B -----------------------------------------
LIST @UNLOAD_STAGE;

-- Peek inside the unloaded file without downloading it
SELECT METADATA$FILENAME AS FILE_NAME,
       $1 AS CLAIM_ID, $2 AS MEMBER_ID, $3 AS CLAIM_STATUS, $4 AS AMOUNT, $5 AS UPDATED_AT
  FROM @UNLOAD_STAGE/dental_claims_staged/ (FILE_FORMAT => 'UNLOAD_CSV')
 ORDER BY CLAIM_ID;

-- [S4] Query history for the demo (what the job actually ran) ----------------
SELECT START_TIME, QUERY_TYPE, ROWS_PRODUCED, LEFT(QUERY_TEXT, 120) AS QUERY_TEXT
  FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 200))
 WHERE QUERY_TEXT ILIKE '%DENTAL_CLAIMS%'
   AND START_TIME > DATEADD('hour', -1, CURRENT_TIMESTAMP())
 ORDER BY START_TIME DESC;

-- [S5] One-time setup: change tracking, stream, outbox ------------------------
-- Demo version of sql/01_snowflake_setup.sql, the production template.
-- Create the stream first, then run the initial full load, so no change made
-- in between is missed. A new stream starts empty.
ALTER TABLE DENTAL_CLAIMS SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE STREAM CLAIMS_STREAM ON TABLE DENTAL_CLAIMS;

-- Outbox: source columns in the same order, then four CDC columns. Transient,
-- because it is short-lived working data.
CREATE OR REPLACE TRANSIENT TABLE CLAIMS_OUTBOX (
    CLAIM_ID       NUMBER        NOT NULL,
    MEMBER_ID      NUMBER,
    CLAIM_STATUS   VARCHAR(20),
    AMOUNT         NUMBER(10,2),
    UPDATED_AT     TIMESTAMP_NTZ,
    _CDC_ACTION    STRING,
    _CDC_ISUPDATE  BOOLEAN,
    _CDC_LOADED_AT TIMESTAMP_LTZ,
    _CDC_EXPORTED  BOOLEAN DEFAULT FALSE
)
DATA_RETENTION_TIME_IN_DAYS = 1;

SHOW STREAMS LIKE 'CLAIMS_STREAM';           -- MODE = DEFAULT; note STALE_AFTER

SELECT SYSTEM$STREAM_HAS_DATA('CLAIMS_STREAM') AS STREAM_HAS_DATA;   -- FALSE

-- [S6] Upstream changes: delete claim 5, update claim 3 ----------------------
DELETE FROM DENTAL_CLAIMS WHERE CLAIM_ID = 5;

-- Claim 3 is seeded as DENIED; approve it and adjust the amount.
UPDATE DENTAL_CLAIMS
   SET CLAIM_STATUS = 'PAID',
       AMOUNT       = AMOUNT + 50.00,
       UPDATED_AT   = CURRENT_TIMESTAMP()
 WHERE CLAIM_ID = 3;

-- Pending changes in the stream. Selecting does not consume them. An UPDATE
-- appears as two rows: METADATA$ACTION = 'DELETE' (old values) and 'INSERT'
-- (new values), both with METADATA$ISUPDATE = TRUE.
SELECT METADATA$ACTION, METADATA$ISUPDATE, CLAIM_ID, CLAIM_STATUS, AMOUNT
  FROM CLAIMS_STREAM
 ORDER BY CLAIM_ID, METADATA$ACTION;

SELECT SYSTEM$STREAM_HAS_DATA('CLAIMS_STREAM') AS STREAM_HAS_DATA;   -- TRUE

-- [S7] More upstream changes: insert claim 24, update claim 4 ----------------
INSERT INTO DENTAL_CLAIMS (CLAIM_ID, MEMBER_ID, CLAIM_STATUS, AMOUNT, UPDATED_AT)
VALUES (24, 88888, 'SUBMITTED', 432.10, CURRENT_TIMESTAMP());

UPDATE DENTAL_CLAIMS
   SET CLAIM_STATUS = 'PAID',
       AMOUNT       = AMOUNT + 25.00,
       UPDATED_AT   = CURRENT_TIMESTAMP()
 WHERE CLAIM_ID = 4;

SELECT METADATA$ACTION, METADATA$ISUPDATE, CLAIM_ID, CLAIM_STATUS, AMOUNT
  FROM CLAIMS_STREAM
 ORDER BY CLAIM_ID, METADATA$ACTION;

-- [S8] Outbox: every change the job has consumed, and whether it was delivered
SELECT _CDC_LOADED_AT, _CDC_ACTION, _CDC_ISUPDATE, _CDC_EXPORTED,
       CLAIM_ID, CLAIM_STATUS, AMOUNT
  FROM CLAIMS_OUTBOX
 ORDER BY _CDC_LOADED_AT, CLAIM_ID, _CDC_ACTION;

-- ===========================================================================
-- Part 7: volume test (optional, separate from the 20-row demo)
-- ===========================================================================

-- [S9] Create a wide 10M-row claims table ------------------------------------
-- The generator view produces fictitious claims with 21 columns; [S9] keeps
-- claims 1 to 10,000,000 and [S10] later adds 10,000,001 to 15,000,000.
CREATE OR REPLACE VIEW VOLUME_CLAIMS_GEN AS
WITH g AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ8()) AS N,
           UNIFORM(0, 99, RANDOM())           AS R,
           DATEADD('day', -UNIFORM(30, 1095, RANDOM()), CURRENT_DATE()) AS SVC
      FROM TABLE(GENERATOR(ROWCOUNT => 15000000))
), c AS (
    SELECT g.*,
           ROUND(UNIFORM(2500, 350000, RANDOM()) / 100, 2)               AS BILLED,
           DECODE(MOD(N, 5), 0, 'SUBMITTED', 1, 'PENDING', 2, 'PAID',
                             3, 'DENIED', 'ADJUSTED')                    AS STATUS
      FROM g
)
SELECT
    N::NUMBER(12,0)                                                     AS CLAIM_ID,
    UNIFORM(100000000, 999999999, RANDOM())::NUMBER(12,0)               AS MEMBER_ID,
    UNIFORM(1, 50000, RANDOM())::NUMBER(12,0)                           AS PROVIDER_ID,
    TO_VARCHAR(UNIFORM(1000000000, 1999999999, RANDOM()))::VARCHAR(10)  AS PROVIDER_NPI,
    ('PLN-' || LPAD(UNIFORM(1, 400, RANDOM()), 4, '0'))::VARCHAR(12)    AS PLAN_CODE,
    DECODE(MOD(R, 4), 0, 'PREVENTIVE', 1, 'BASIC', 2, 'MAJOR',
                      'ORTHODONTIC')::VARCHAR(20)                       AS CLAIM_TYPE,
    ('D' || LPAD(UNIFORM(100, 9999, RANDOM()), 4, '0'))::VARCHAR(5)     AS PROCEDURE_CODE,
    IFF(R < 40, NULL, TO_VARCHAR(UNIFORM(1, 32, RANDOM())))::VARCHAR(2) AS TOOTH_NUMBER,
    IFF(R < 40, NULL, ARRAY_CONSTRUCT('M', 'O', 'D', 'B', 'L', 'MO', 'DO', 'MOD')
                      [UNIFORM(0, 7, RANDOM())])::VARCHAR(5)            AS TOOTH_SURFACE,
    ('K0' || UNIFORM(0, 8, RANDOM()) || '.' || UNIFORM(0, 9, RANDOM()))::VARCHAR(8)
                                                                        AS DIAGNOSIS_CODE,
    SVC::DATE                                                           AS SERVICE_DATE,
    DATEADD('second', UNIFORM(3600, 1728000, RANDOM()), SVC)::TIMESTAMP_NTZ
                                                                        AS SUBMITTED_AT,
    STATUS::VARCHAR(20)                                                 AS CLAIM_STATUS,
    BILLED::NUMBER(12,2)                                                AS BILLED_AMOUNT,
    ROUND(BILLED * 0.85, 2)::NUMBER(12,2)                               AS ALLOWED_AMOUNT,
    IFF(STATUS IN ('PAID', 'ADJUSTED'), ROUND(BILLED * 0.85 * 0.8, 2), 0)::NUMBER(12,2)
                                                                        AS PAID_AMOUNT,
    IFF(STATUS IN ('PAID', 'ADJUSTED'), ROUND(BILLED * 0.85, 2) - ROUND(BILLED * 0.85 * 0.8, 2),
        0)::NUMBER(12,2)                                                AS MEMBER_RESP_AMOUNT,
    IFF(STATUS = 'DENIED', ARRAY_CONSTRUCT('Not a covered benefit', 'Frequency limitation',
        'Missing X-ray', 'Member not eligible')[MOD(N, 4)], NULL)::VARCHAR(100)
                                                                        AS DENIAL_REASON,
    -- About 30% NULL, some empty, some with quotes and commas, up to 400 characters.
    CASE WHEN R < 30 THEN NULL
         WHEN R < 33 THEN ''
         WHEN R < 40 THEN 'Reviewed, see "attached" narrative: ' || RANDSTR(UNIFORM(1, 300, RANDOM()), RANDOM())
         ELSE RANDSTR(UNIFORM(1, 400, RANDOM()), RANDOM())
    END::VARCHAR(500)                                                   AS PAYER_NOTE,
    (UNIFORM(0, 19, RANDOM()) = 0)::BOOLEAN                             AS IS_EMERGENCY,
    DATEADD('minute', -UNIFORM(60, 500000, RANDOM()), CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
                                                                        AS UPDATED_AT
FROM c;

CREATE OR REPLACE TABLE VOLUME_CLAIMS AS
SELECT * FROM VOLUME_CLAIMS_GEN WHERE CLAIM_ID <= 10000000;

SELECT COUNT(*) AS ROW_COUNT, MAX(UPDATED_AT) AS LATEST_UPDATE FROM VOLUME_CLAIMS;
SELECT * FROM VOLUME_CLAIMS WHERE CLAIM_ID <= 10 ORDER BY CLAIM_ID;

-- [S10] Simulate a large upstream change: 5M inserts + 1M updates -------------
INSERT INTO VOLUME_CLAIMS
SELECT * REPLACE (CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS UPDATED_AT)
  FROM VOLUME_CLAIMS_GEN
 WHERE CLAIM_ID > 10000000;

-- Every 10th existing claim is adjudicated: 1,000,000 updates.
UPDATE VOLUME_CLAIMS
   SET CLAIM_STATUS       = IFF(MOD(CLAIM_ID, 20) = 0, 'DENIED', 'PAID'),
       PAID_AMOUNT        = IFF(MOD(CLAIM_ID, 20) = 0, 0, ROUND(ALLOWED_AMOUNT * 0.8, 2)),
       MEMBER_RESP_AMOUNT = IFF(MOD(CLAIM_ID, 20) = 0, 0,
                                ALLOWED_AMOUNT - ROUND(ALLOWED_AMOUNT * 0.8, 2)),
       DENIAL_REASON      = IFF(MOD(CLAIM_ID, 20) = 0, 'Frequency limitation', NULL),
       PAYER_NOTE         = COALESCE(PAYER_NOTE, '') || ' | Adjudicated',
       UPDATED_AT         = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
 WHERE CLAIM_ID <= 10000000 AND MOD(CLAIM_ID, 10) = 0;

-- Expect 15,000,000 rows, 6,000,000 of them changed since [S9].
SELECT COUNT(*) AS ROW_COUNT,
       COUNT_IF(UPDATED_AT >= DATEADD('minute', -30, CURRENT_TIMESTAMP())) AS CHANGED_RECENTLY
  FROM VOLUME_CLAIMS;
