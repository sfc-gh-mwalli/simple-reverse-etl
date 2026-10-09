-- Runs automatically on first container start (mounted into
-- /docker-entrypoint-initdb.d). Target tables for the Snowflake -> MySQL demo.
-- PRIMARY KEY on CLAIM_ID is what makes the upsert (ON DUPLICATE KEY UPDATE /
-- LOAD DATA ... REPLACE) work.
--
-- Three identical tables so the demo can show each path side by side:
--   DENTAL_CLAIMS         <- Transport A, hwm change capture (sync.py)
--   DENTAL_CLAIMS_STAGED  <- Transport B (unload -> stage -> bulk load, sync.py --transport unload)
--   DENTAL_CLAIMS_CDC     <- Transport A, stream change capture (sync.py)
--
-- demo/reset_demo.sh also applies this file, so new tables appear in an
-- existing container without recreating it.
CREATE TABLE IF NOT EXISTS DENTAL_CLAIMS (
    CLAIM_ID     BIGINT PRIMARY KEY,
    MEMBER_ID    BIGINT,
    CLAIM_STATUS VARCHAR(20),
    AMOUNT       DECIMAL(10,2),
    UPDATED_AT   DATETIME(3)
);

CREATE TABLE IF NOT EXISTS DENTAL_CLAIMS_STAGED (
    CLAIM_ID     BIGINT PRIMARY KEY,
    MEMBER_ID    BIGINT,
    CLAIM_STATUS VARCHAR(20),
    AMOUNT       DECIMAL(10,2),
    UPDATED_AT   DATETIME(3)
);

CREATE TABLE IF NOT EXISTS DENTAL_CLAIMS_CDC (
    CLAIM_ID     BIGINT PRIMARY KEY,
    MEMBER_ID    BIGINT,
    CLAIM_STATUS VARCHAR(20),
    AMOUNT       DECIMAL(10,2),
    UPDATED_AT   DATETIME(3)
);

-- Part 7 (volume test): wide claims tables, loaded by demo/run_volume.sh.
--   VOLUME_CLAIMS         <- Transport A
--   VOLUME_CLAIMS_STAGED  <- Transport B
CREATE TABLE IF NOT EXISTS VOLUME_CLAIMS (
    CLAIM_ID            BIGINT PRIMARY KEY,
    MEMBER_ID           BIGINT,
    PROVIDER_ID         BIGINT,
    PROVIDER_NPI        VARCHAR(10),
    PLAN_CODE           VARCHAR(12),
    CLAIM_TYPE          VARCHAR(20),
    PROCEDURE_CODE      VARCHAR(5),
    TOOTH_NUMBER        VARCHAR(2),
    TOOTH_SURFACE       VARCHAR(5),
    DIAGNOSIS_CODE      VARCHAR(8),
    SERVICE_DATE        DATE,
    SUBMITTED_AT        DATETIME(3),
    CLAIM_STATUS        VARCHAR(20),
    BILLED_AMOUNT       DECIMAL(12,2),
    ALLOWED_AMOUNT      DECIMAL(12,2),
    PAID_AMOUNT         DECIMAL(12,2),
    MEMBER_RESP_AMOUNT  DECIMAL(12,2),
    DENIAL_REASON       VARCHAR(100),
    PAYER_NOTE          VARCHAR(500),
    IS_EMERGENCY        TINYINT(1),
    UPDATED_AT          DATETIME(3)
);

CREATE TABLE IF NOT EXISTS VOLUME_CLAIMS_STAGED (
    CLAIM_ID            BIGINT PRIMARY KEY,
    MEMBER_ID           BIGINT,
    PROVIDER_ID         BIGINT,
    PROVIDER_NPI        VARCHAR(10),
    PLAN_CODE           VARCHAR(12),
    CLAIM_TYPE          VARCHAR(20),
    PROCEDURE_CODE      VARCHAR(5),
    TOOTH_NUMBER        VARCHAR(2),
    TOOTH_SURFACE       VARCHAR(5),
    DIAGNOSIS_CODE      VARCHAR(8),
    SERVICE_DATE        DATE,
    SUBMITTED_AT        DATETIME(3),
    CLAIM_STATUS        VARCHAR(20),
    BILLED_AMOUNT       DECIMAL(12,2),
    ALLOWED_AMOUNT      DECIMAL(12,2),
    PAID_AMOUNT         DECIMAL(12,2),
    MEMBER_RESP_AMOUNT  DECIMAL(12,2),
    DENIAL_REASON       VARCHAR(100),
    PAYER_NOTE          VARCHAR(500),
    IS_EMERGENCY        TINYINT(1),
    UPDATED_AT          DATETIME(3)
);
