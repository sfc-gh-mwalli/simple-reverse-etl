-- Runs automatically on first container start (mounted into
-- /docker-entrypoint-initdb.d). Target tables for the Snowflake -> MySQL demo.
-- PRIMARY KEY on CLAIM_ID is what makes the upsert (ON DUPLICATE KEY UPDATE /
-- LOAD DATA ... REPLACE) work.
--
-- Two identical tables so the demo can show both transports side by side:
--   DENTAL_CLAIMS         <- Transport A (live connector pull, sync.py)
--   DENTAL_CLAIMS_STAGED  <- Transport B (unload -> stage -> bulk load, unload_sync.py)
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
