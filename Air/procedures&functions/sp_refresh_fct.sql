CREATE OR REPLACE PROCEDURE AIR_TEST.SP_REFRESH_FCT()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_proc_name        STRING DEFAULT 'SP_REFRESH_FCT';
    v_schema_name      STRING DEFAULT 'AIR_TEST';

    v_proc_start_ts    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP();
    v_proc_end_ts      TIMESTAMP_LTZ;
    v_proc_status      STRING DEFAULT 'OK';
    v_proc_msg         STRING DEFAULT 'procedure completed successfully';
    v_proc_duration    NUMBER(18,3);

    v_step_start_ts    TIMESTAMP_LTZ;
    v_step_end_ts      TIMESTAMP_LTZ;
    v_step_duration    NUMBER(18,3);

    v_rows_loaded      NUMBER(38,0) DEFAULT 0;
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
        'SP_REFRESH_FCT execution started',
        0
    );

    ----------------------------------------------------------------
    -- PHASE: Refresh FACT_FLIGHT (full rebuild)
    ----------------------------------------------------------------
    BEGIN
        v_step_start_ts := CURRENT_TIMESTAMP();
        v_rows_loaded   := 0;

        -- 1) truncate fact
        TRUNCATE TABLE AIR_TEST.FACT_FLIGHT;
        COMMIT;

        -- 2) reload fact
        INSERT INTO AIR_TEST.FACT_FLIGHT (
            customer_sk,
            airport_sk,
            departure_date,
            arrival_airport,
            pilot_name,
            flight_status,
            ticket_type,
            passenger_status,
            update_ts,
            record_source
        )
        SELECT
            c.customer_sk,
            a.airport_sk,
            s.departure_date,
            s.arrival_airport,
            s.pilot_name,
            s.flight_status,
            s.ticket_type,
            s.passenger_status,
            s.update_ts,
            'AIRLINE' AS record_source
        FROM AIR_TEST.SRC_AIRLINE_DATASET s
        JOIN AIR_TEST.DIM_CUSTOMER c
          ON s.passenger_id = c.passenger_id
         AND s.departure_date BETWEEN c.effective_from_ts AND c.effective_to_ts
        JOIN AIR_TEST.DIM_AIRPORT a
          ON a.airport_name = s.airport_name
         AND s.departure_date BETWEEN a.effective_from_ts AND a.effective_to_ts;

        COMMIT;

        v_rows_loaded := (SELECT COUNT(*) FROM AIR_TEST.FACT_FLIGHT);

        updated_tables := OBJECT_INSERT(updated_tables, 'FACT_FLIGHT', v_rows_loaded, TRUE);

        v_step_end_ts := CURRENT_TIMESTAMP();
        v_step_duration := DATEDIFF('millisecond', v_step_start_ts, v_step_end_ts) / 1000.0;

        CALL ADMIN.SP_LOG_ETL(
            :v_schema_name,
            :v_proc_name,
            :v_step_start_ts,
            :v_step_end_ts,
            :v_rows_loaded,
            'OK',
            'FACT_FLIGHT refreshed successfully',
            :v_step_duration
        );
        COMMIT;

    EXCEPTION
        WHEN OTHER THEN
            v_proc_status := 'ERROR';
            v_proc_msg    := 'FACT_FLIGHT refresh failed: ' || SQLERRM;

            v_step_end_ts := CURRENT_TIMESTAMP();
            v_step_duration := DATEDIFF('millisecond', v_step_start_ts, v_step_end_ts) / 1000.0;

            CALL ADMIN.SP_LOG_ETL(
                :v_schema_name,
                :v_proc_name,
                :v_step_start_ts,
                :v_step_end_ts,
                :v_rows_loaded,
                'ERROR',
                LEFT(:v_proc_msg, 250),
                :v_step_duration
            );
            COMMIT;
    END;

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
        :v_rows_loaded,
        :v_proc_status,
        LEFT(:v_proc_msg, 250),
        :v_proc_duration
    );
    COMMIT;

    RETURN updated_tables;
END;
$$;

-- call SP_REFRESH_FCT()