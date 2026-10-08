-- SQL Server target tables for the demo, equivalent to mysql_init.sql.
-- Applied by demo/reset_demo.sh (piped to sqlcmd) when DEMO_TARGET=mssql. Safe to re-run.
-- PRIMARY KEY on CLAIM_ID is what the MERGE-based upsert matches on.
--
--   DENTAL_CLAIMS         <- Transport A, hwm change capture (sync.py)
--   DENTAL_CLAIMS_STAGED  <- Transport B (unload -> stage -> bulk load, sync.py --transport unload)
--   DENTAL_CLAIMS_CDC     <- stream change capture (either transport)
IF DB_ID(N'DENTAL_RPT') IS NULL
    CREATE DATABASE DENTAL_RPT;
GO

USE DENTAL_RPT;
GO

IF OBJECT_ID(N'dbo.DENTAL_CLAIMS', N'U') IS NULL
    CREATE TABLE dbo.DENTAL_CLAIMS (
        CLAIM_ID     BIGINT        NOT NULL PRIMARY KEY,
        MEMBER_ID    BIGINT        NULL,
        CLAIM_STATUS NVARCHAR(20)  NULL,
        AMOUNT       DECIMAL(10,2) NULL,
        UPDATED_AT   DATETIME2(3)  NULL
    );

IF OBJECT_ID(N'dbo.DENTAL_CLAIMS_STAGED', N'U') IS NULL
    CREATE TABLE dbo.DENTAL_CLAIMS_STAGED (
        CLAIM_ID     BIGINT        NOT NULL PRIMARY KEY,
        MEMBER_ID    BIGINT        NULL,
        CLAIM_STATUS NVARCHAR(20)  NULL,
        AMOUNT       DECIMAL(10,2) NULL,
        UPDATED_AT   DATETIME2(3)  NULL
    );

IF OBJECT_ID(N'dbo.DENTAL_CLAIMS_CDC', N'U') IS NULL
    CREATE TABLE dbo.DENTAL_CLAIMS_CDC (
        CLAIM_ID     BIGINT        NOT NULL PRIMARY KEY,
        MEMBER_ID    BIGINT        NULL,
        CLAIM_STATUS NVARCHAR(20)  NULL,
        AMOUNT       DECIMAL(10,2) NULL,
        UPDATED_AT   DATETIME2(3)  NULL
    );
GO
