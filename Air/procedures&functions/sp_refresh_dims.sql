CREATE OR REPLACE PROCEDURE AIR_TEST.SP_REFRESH_DIMS(p_tabname VARCHAR(250))
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_proc_name        STRING DEFAULT 'SP_REFRESH_DIMS';
    v_schema_name      STRING DEFAULT 'AIR_TEST';

    -- Per-branch runtime metadata
    v_step_start_ts    TIMESTAMP_LTZ;
    v_step_end_ts      TIMESTAMP_LTZ;
    v_rows_loaded      NUMBER(38,0) DEFAULT 0;
    v_status           STRING DEFAULT 'OK';
    v_msg              STRING DEFAULT 'upload completed';
    v_duration_sec     NUMBER(18,3);

    -- Overall procedure tracking
    v_proc_start_ts    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP();
    v_proc_end_ts      TIMESTAMP_LTZ;
    v_proc_duration    NUMBER(18,3);
    v_proc_status      STRING DEFAULT 'OK';
    v_proc_msg         STRING DEFAULT 'procedure completed successfully';

    updated_tables     VARIANT DEFAULT OBJECT_CONSTRUCT();
BEGIN
    ----------------------------------------------------------------
    -- GLOBAL START LOG
    ----------------------------------------------------------------
    CALL ADMIN.SP_LOG_ETL(
        :v_schema_name,
        :v_proc_name,
        :v_proc_start_ts,
        :v_proc_start_ts,
        NULL,
        'STARTED',
        'sp_refresh_dims execution started',
        0
    );

    ----------------------------------------------------------------
    -- BRANCH 1: DIM_CUSTOMER (full rebuild with SCD history-like ranges)
    ----------------------------------------------------------------
    IF (p_tabname = 'dim_customer') THEN
        BEGIN
            v_step_start_ts := CURRENT_TIMESTAMP();
            v_rows_loaded   := 0;
            v_status        := 'OK';
            v_msg           := 'upload completed';

            TRUNCATE TABLE AIR_TEST.DIM_CUSTOMER;
            COMMIT;

            DROP TABLE IF EXISTS TMP_DIM_CUSTOMER;

            CREATE OR REPLACE TEMP TABLE TMP_DIM_CUSTOMER AS
            WITH CTE_ORDERED AS (
                SELECT
                    passenger_id,
                    first_name,
                    last_name,
                    gender,
                    age,
                    nationality,
                    update_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY passenger_id
                        ORDER BY update_ts ASC
                    ) AS rn
                FROM AIR_TEST.SRC_AIRLINE_DATASET
                group by passenger_id,
                    first_name,
                    last_name,
                    gender,
                    age,
                    nationality,
                    update_ts
            ),
            CTE_VALIDATED AS (
                SELECT
                    passenger_id,
                    first_name,
                    last_name,
                    gender,
                    age,
                    nationality,
                    update_ts,
                    rn,
                    (LEAD(update_ts) OVER (
                        PARTITION BY passenger_id
                        ORDER BY rn
                    )::DATE - 1) AS effective_to_dt,
                    LEAD(update_ts) OVER (
                        PARTITION BY passenger_id
                        ORDER BY rn
                    )::DATE AS next_valid_from_dt
                FROM CTE_ORDERED
            ),
            CTE_ALL_VALIDATED AS (
                SELECT
                    passenger_id,
                    first_name,
                    last_name,
                    gender,
                    age,
                    nationality,
                    update_ts,
                    COALESCE(
                        next_valid_from_dt,
                        '2000-01-01'::DATE
                    ) AS effective_from_dt,
                    COALESCE(effective_to_dt, '3000-01-01'::DATE) AS effective_to_dt
                FROM CTE_VALIDATED
            )
            SELECT
                passenger_id,
                first_name,
                last_name,
                gender,
                age,
                nationality,
                'AIRLINE' AS record_source,
                TO_TIMESTAMP_LTZ(effective_from_dt) AS effective_from_ts,
                TO_TIMESTAMP_LTZ(effective_to_dt)   AS effective_to_ts,
                IFF(
                    CURRENT_DATE() BETWEEN effective_from_dt AND effective_to_dt,
                    TRUE,
                    FALSE
                ) AS is_current
            FROM CTE_ALL_VALIDATED
            ORDER BY passenger_id DESC, update_ts DESC;

            COMMIT;

            v_rows_loaded := (SELECT COUNT(*) FROM TMP_DIM_CUSTOMER);

            INSERT INTO AIR_TEST.DIM_CUSTOMER (
                passenger_id,
                first_name,
                last_name,
                gender,
                age,
                nationality,
                record_source,
                effective_from_ts,
                effective_to_ts,
                is_current
            )
            SELECT
                passenger_id,
                first_name,
                last_name,
                gender,
                age,
                nationality,
                record_source,
                effective_from_ts,
                effective_to_ts,
                is_current
            FROM TMP_DIM_CUSTOMER;

            COMMIT;

            updated_tables := OBJECT_INSERT(updated_tables, 'DIM_CUSTOMER', v_rows_loaded, TRUE);
            v_msg := 'DIM_CUSTOMER uploaded successfully';

        EXCEPTION
            WHEN OTHER THEN
                v_status      := 'ERROR';
                v_msg         := 'upload failed: ' || SQLERRM;
                v_proc_status := 'ERROR';
                v_proc_msg    := 'branch failed';
        END;

        v_step_end_ts := CURRENT_TIMESTAMP();
        v_duration_sec := DATEDIFF('millisecond', v_step_start_ts, v_step_end_ts) / 1000.0;

        CALL ADMIN.SP_LOG_ETL(
            :v_schema_name,
            :v_proc_name,
            :v_step_start_ts,
            :v_step_end_ts,
            :v_rows_loaded,
            :v_status,
            LEFT(:v_msg, 250),
            :v_duration_sec
        );
        COMMIT;

    ----------------------------------------------------------------
    -- BRANCH 2: DIM_AIRPORT (full rebuild + SCD-like ranges)
    ----------------------------------------------------------------
    ELSEIF (p_tabname = 'dim_airport') THEN
        BEGIN
            v_step_start_ts := CURRENT_TIMESTAMP();
            v_rows_loaded   := 0;
            v_status        := 'OK';
            v_msg           := 'upload completed';

            TRUNCATE TABLE AIR_TEST.DIM_AIRPORT;
            COMMIT;

            DROP TABLE IF EXISTS TMP_DIM_AIRPORT;

            CREATE OR REPLACE TEMP TABLE TMP_DIM_AIRPORT AS
            WITH CTE_BASE AS (
                SELECT
                    airport_name,
                    airport_country_code,
                    country_name,
                    airport_continent,
                    continents,
                    update_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY airport_name, airport_country_code, country_name, airport_continent, continents
                        ORDER BY update_ts ASC
                    ) AS rn
                FROM AIR_TEST.SRC_AIRLINE_DATASET
                group by airport_name,
                    airport_country_code,
                    country_name,
                    airport_continent,
                    continents,
                    update_ts --make group by
            ),
            CTE_VALIDATED AS (
                SELECT
                    airport_name,
                    airport_country_code,
                    country_name,
                    airport_continent,
                    continents,
                    update_ts,
                    rn,
                    (LEAD(update_ts) OVER (
                        PARTITION BY airport_name, airport_country_code, country_name, airport_continent, continents
                        ORDER BY rn
                    )::DATE - 1) AS effective_to_dt,
                    LEAD(update_ts) OVER (
                        PARTITION BY airport_name, airport_country_code, country_name, airport_continent, continents
                        ORDER BY rn
                    )::DATE AS next_valid_from_dt
                FROM CTE_BASE
            ),
            CTE_ALL_VALIDATED AS (
                SELECT
                    airport_name,
                    airport_country_code,
                    country_name,
                    airport_continent,
                    continents,
                    update_ts,
                    COALESCE(
                        next_valid_from_dt,
                        '2000-01-01'::DATE
                    ) AS effective_from_dt,
                    COALESCE(effective_to_dt, '3000-01-01'::DATE) AS effective_to_dt
                FROM CTE_VALIDATED
            )
            SELECT
                airport_name,
                airport_country_code,
                country_name,
                airport_continent,
                continents,
                'AIRLINE' AS record_source,
                TO_TIMESTAMP_LTZ(effective_from_dt) AS effective_from_ts,
                TO_TIMESTAMP_LTZ(effective_to_dt)   AS effective_to_ts,
                IFF(
                    CURRENT_DATE() BETWEEN effective_from_dt AND effective_to_dt,
                    TRUE,
                    FALSE
                ) AS is_current
            FROM CTE_ALL_VALIDATED
            ORDER BY airport_name DESC, update_ts DESC;

            COMMIT;

            v_rows_loaded := (SELECT COUNT(*) FROM TMP_DIM_AIRPORT);

            INSERT INTO AIR_TEST.DIM_AIRPORT (
                airport_name,
                airport_country_code,
                country_name,
                airport_continent,
                continents,
                record_source,
                effective_from_ts,
                effective_to_ts,
                is_current
            )
            SELECT
                airport_name,
                airport_country_code,
                country_name,
                airport_continent,
                continents,
                record_source,
                effective_from_ts,
                effective_to_ts,
                is_current
            FROM TMP_DIM_AIRPORT;

            COMMIT;

            updated_tables := OBJECT_INSERT(updated_tables, 'DIM_AIRPORT', v_rows_loaded, TRUE);
            v_msg := 'DIM_AIRPORT uploaded successfully';

        EXCEPTION
            WHEN OTHER THEN
                v_status      := 'ERROR';
                v_msg         := 'upload failed: ' || SQLERRM;
                v_proc_status := 'ERROR';
                v_proc_msg    := 'branch failed';
        END;

        v_step_end_ts := CURRENT_TIMESTAMP();
        v_duration_sec := DATEDIFF('millisecond', v_step_start_ts, v_step_end_ts) / 1000.0;

        CALL ADMIN.SP_LOG_ETL(
            :v_schema_name,
            :v_proc_name,
            :v_step_start_ts,
            :v_step_end_ts,
            :v_rows_loaded,
            :v_status,
            LEFT(:v_msg, 250),
            :v_duration_sec
        );
        COMMIT;

    ----------------------------------------------------------------
    -- UNKNOWN BRANCH
    ----------------------------------------------------------------
    ELSE
        v_proc_status := 'ERROR';
        v_proc_msg    := 'unsupported table name: ' || COALESCE(p_tabname, '<NULL>');

        v_step_start_ts := CURRENT_TIMESTAMP();
        v_step_end_ts   := CURRENT_TIMESTAMP();
        v_rows_loaded   := 0;
        v_status        := 'ERROR';
        v_msg           := v_proc_msg;
        v_duration_sec  := 0;

        CALL ADMIN.SP_LOG_ETL(
            :v_schema_name,
            :v_proc_name,
            :v_step_start_ts,
            :v_step_end_ts,
            :v_rows_loaded,
            :v_status,
            LEFT(:v_msg, 250),
            :v_duration_sec
        );
        COMMIT;
    END IF;

    ----------------------------------------------------------------
    -- GLOBAL END LOG
    ----------------------------------------------------------------
    v_proc_end_ts := CURRENT_TIMESTAMP();
    v_proc_duration := DATEDIFF('millisecond', v_proc_start_ts, v_proc_end_ts) / 1000.0;

    CALL ADMIN.SP_LOG_ETL(
        :v_schema_name,
        :v_proc_name,
        :v_proc_start_ts,
        :v_proc_end_ts,
        NULL,
        :v_proc_status,
        LEFT(:v_proc_msg, 250),
        :v_proc_duration
    );
    COMMIT;

    RETURN updated_tables;
END;
$$;

call SP_REFRESH_DIMS('dim_airport');
call SP_REFRESH_DIMS('dim_customer');


select * from DIM_AIRPORT order by AIRPORT_NAME;
select * from DIM_CUSTOMER order by PASSENGER_ID;

select count( PASSENGER_ID) from DIM_CUSTOMER;



select C.CUSTOMER_SK, A.AIRPORT_SK, DEPARTURE_DATE,
       ARRIVAL_AIRPORT, PILOT_NAME, FLIGHT_STATUS, TICKET_TYPE,
       PASSENGER_STATUS, UPDATE_TS, 'AIRLINE' as record_source
from SRC_AIRLINE_DATASET S
         join DIM_CUSTOMER C on s.PASSENGER_ID = c.PASSENGER_ID
         and S.DEPARTURE_DATE between C.EFFECTIVE_FROM_TS and C.EFFECTIVE_TO_TS
         join DIM_AIRPORT A on A.AIRPORT_NAME = S.AIRPORT_NAME
         and S.DEPARTURE_DATE between A.EFFECTIVE_FROM_TS and A.EFFECTIVE_TO_TS
