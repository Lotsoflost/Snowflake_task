create or replace procedure ADMIN.SP_LOG_ETL
    (IN_SCHEMA_NAME STRING, IN_PROCEDURE_NAME STRING, IN_START_TS TIMESTAMP_LTZ,
     IN_END_TS TIMESTAMP_LTZ, IN_ROWS_INSERTED NUMBER, IN_STATUS STRING,
     IN_MESSAGE STRING,
     IN_DURATION_SEC NUMBER)
    returns STRING
    language SQL
as
$$
BEGIN
    INSERT INTO ADMIN.ETL_LOG (SCHEMA_NAME,
                               PROCEDURE_NAME,
                               START_TS,
                               END_TS,
                               ROWS_INSERTED,
                               STATUS,
                               MSG,
                               DURATION_SEC)
    VALUES (:IN_SCHEMA_NAME,
            :IN_PROCEDURE_NAME,
            :IN_START_TS,
            :IN_END_TS,
            :IN_ROWS_INSERTED,
            :IN_STATUS,
            :IN_MESSAGE,
            :IN_DURATION_SEC);
    COMMIT;
    RETURN 'OK';
END;
$$;