-- ---------------------------------------------------------------------------
-- SimpleReverseETL demonstration: Snowsight worksheet.
-- Run each section when the runbook (demo/README.md) refers to it.
-- ---------------------------------------------------------------------------
USE DATABASE SIMPLE_REVERSE_ETL_DEMO;
USE SCHEMA DENTAL;

-- [S1] Source data (expect 20 rows) ------------------------------------------
SELECT * FROM DENTAL_CLAIMS ORDER BY CLAIM_ID;

SELECT COUNT(*) AS ROW_COUNT, MAX(UPDATED_AT) AS LATEST_UPDATE FROM DENTAL_CLAIMS;

-- [S2] Simulate upstream changes: 2 updates + 3 inserts ----------------------
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

-- Rows that the next incremental run will pick up
SELECT * FROM DENTAL_CLAIMS
 WHERE CLAIM_ID IN (1, 2, 21, 22, 23)
 ORDER BY CLAIM_ID;

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
