/*===============================================================================
URL..................: admin/tables/etl_log
Activity.............: ETL - Centralized execution log table
Description..........:
    Stores step- and run-level audit entries produced by ETL components
    (Python loaders, stored procedures). Each row is an immutable record with
    timestamps, status, message, and optional row count + duration.

Owner................: Olga Pankova
Retention............: As per platform policy (no automatic purge here)
Change History.......:
    2025-10-29  OP  Initial create with duration_sec and identity PK
===============================================================================*/
-- -- База/схема (если уже есть — можно пропустить)
-- CREATE DATABASE IF NOT EXISTS AIRFLIGHTS;
-- USE DATABASE AIRFLIGHTS;

CREATE SCHEMA IF NOT EXISTS ADMIN;

-- Таблица лога
CREATE TABLE IF NOT EXISTS ADMIN.ETL_LOG (
    -- Surrogate key
    ORDER_ID        NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1,

    -- Emitter / scope (e.g., 'adventure_works_test')
    SCHEMA_NAME     STRING,

    -- Operation name (procedure/step identifier)
    PROCEDURE_NAME  STRING,

    -- Timing
    START_TS        TIMESTAMP_LTZ,
    END_TS          TIMESTAMP_LTZ,

    -- Outcome details
    MSG             STRING,
    ROWS_INSERTED            NUMBER(38,0),     -- affected rows (nullable)
    STATUS          STRING,           -- e.g., STARTED | OK | ERROR
    DURATION_SEC    NUMBER(18,3)      -- elapsed seconds (with milliseconds)
);

-- В Snowflake PK — не обеспечивает уникальность физически, но можно для документации
ALTER TABLE ADMIN.ETL_LOG ADD CONSTRAINT PK_ETL_LOG PRIMARY KEY (ORDER_ID);

-- Комментарии
COMMENT ON TABLE ADMIN.ETL_LOG IS 'Centralized ETL audit log (immutable append-only).';

COMMENT ON COLUMN ADMIN.ETL_LOG.ORDER_ID       IS 'Surrogate identity primary key.';
COMMENT ON COLUMN ADMIN.ETL_LOG.SCHEMA_NAME    IS 'Logical schema/area that emitted the log entry.';
COMMENT ON COLUMN ADMIN.ETL_LOG.PROCEDURE_NAME IS 'Procedure or step name that produced the log entry.';
COMMENT ON COLUMN ADMIN.ETL_LOG.START_TS       IS 'Operation start timestamp.';
COMMENT ON COLUMN ADMIN.ETL_LOG.END_TS         IS 'Operation end timestamp.';
COMMENT ON COLUMN ADMIN.ETL_LOG.MSG            IS 'Short status message.';
COMMENT ON COLUMN ADMIN.ETL_LOG.ROWS_INSERTED           IS 'Rows affected; NULL when not applicable.';
COMMENT ON COLUMN ADMIN.ETL_LOG.STATUS         IS 'Status label, e.g., STARTED/OK/ERROR.';
COMMENT ON COLUMN ADMIN.ETL_LOG.DURATION_SEC   IS 'Elapsed time in seconds.';
