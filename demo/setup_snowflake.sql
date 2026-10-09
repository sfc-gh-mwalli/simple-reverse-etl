-- Resets the Snowflake demo source to a known state (20 rows). Safe to re-run.
CREATE DATABASE IF NOT EXISTS SIMPLE_REVERSE_ETL_DEMO;
CREATE SCHEMA IF NOT EXISTS SIMPLE_REVERSE_ETL_DEMO.DENTAL;

-- Named internal stage used by Transport B. Stands in for the S3 / Azure / GCS
-- bucket an external stage would use in production.
CREATE STAGE IF NOT EXISTS SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE
    COMMENT = 'SimpleReverseETL demo: Transport B unload location';
REMOVE @SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_STAGE;

-- File format used only to inspect unloaded files in Snowsight.
CREATE FILE FORMAT IF NOT EXISTS SIMPLE_REVERSE_ETL_DEMO.DENTAL.UNLOAD_CSV
    TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE OR REPLACE TABLE SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS (
    CLAIM_ID     NUMBER        NOT NULL,
    MEMBER_ID    NUMBER,
    CLAIM_STATUS VARCHAR(20),
    AMOUNT       NUMBER(10,2),
    UPDATED_AT   TIMESTAMP_NTZ
);

INSERT INTO SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS
    (CLAIM_ID, MEMBER_ID, CLAIM_STATUS, AMOUNT, UPDATED_AT)
SELECT
    SEQ4() + 1                                                            AS CLAIM_ID,
    UNIFORM(10000, 99999, RANDOM())                                       AS MEMBER_ID,
    DECODE(MOD(SEQ4(), 3), 0,'SUBMITTED', 1,'PAID', 'DENIED')             AS CLAIM_STATUS,
    ROUND(UNIFORM(50, 5000, RANDOM()), 2)                                 AS AMOUNT,
    DATEADD('minute', -1 * UNIFORM(0, 20000, RANDOM()), CURRENT_TIMESTAMP()) AS UPDATED_AT
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- The stream and outbox are created live in Part 5 of the runbook
-- (snowsight_demo.sql [S5]), so a reset removes them. Recreating the table
-- above would invalidate the stream anyway.
DROP STREAM IF EXISTS SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_STREAM;
DROP TABLE IF EXISTS SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_OUTBOX;

-- Part 7 volume test objects (created by snowsight_demo.sql [S9]).
DROP TABLE IF EXISTS SIMPLE_REVERSE_ETL_DEMO.DENTAL.VOLUME_CLAIMS;
DROP VIEW IF EXISTS SIMPLE_REVERSE_ETL_DEMO.DENTAL.VOLUME_CLAIMS_GEN;
