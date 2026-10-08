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

-- Stream-based change capture (sync.py --change-capture stream).
-- The stream is created after the seed rows are inserted, so it starts empty and
-- records only later changes. Recreating the table above invalidates any older
-- stream, so both objects are recreated on every reset.
CREATE OR REPLACE STREAM SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_STREAM
    ON TABLE SIMPLE_REVERSE_ETL_DEMO.DENTAL.DENTAL_CLAIMS;

-- Outbox: the source columns in the same order, then four CDC columns.
CREATE OR REPLACE TABLE SIMPLE_REVERSE_ETL_DEMO.DENTAL.CLAIMS_OUTBOX (
    CLAIM_ID       NUMBER        NOT NULL,
    MEMBER_ID      NUMBER,
    CLAIM_STATUS   VARCHAR(20),
    AMOUNT         NUMBER(10,2),
    UPDATED_AT     TIMESTAMP_NTZ,
    _CDC_ACTION    STRING,
    _CDC_ISUPDATE  BOOLEAN,
    _CDC_LOADED_AT TIMESTAMP_LTZ,
    _CDC_EXPORTED  BOOLEAN DEFAULT FALSE
);
