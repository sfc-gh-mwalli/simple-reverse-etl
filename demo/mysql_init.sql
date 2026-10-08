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
