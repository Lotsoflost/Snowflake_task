CREATE OR REPLACE PROCEDURE AIR_TEST.SP_UPLOAD_SRC()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_proc_name        STRING DEFAULT 'SP_UPLOAD_SRC';
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
    CALL ADMIN.SP_LOG_ETL(:v_schema_name, :v_proc_name, :v_proc_start_ts, :v_proc_start_ts,
      NULL, 'STARTED', 'SP_UPLOAD_SRC execution started', 0);

    v_step_start_ts := CURRENT_TIMESTAMP();

    CREATE OR REPLACE TEMP TABLE TMP_AIR_DATA AS
    SELECT
        passenger_id, first_name, last_name, gender, age, nationality,
        airport_name, airport_country_code, country_name, airport_continent,
        continents, departure_date, arrival_airport, pilot_name, flight_status,
        ticket_type, passenger_status,
        MAX(update_ts) AS update_ts
    FROM AIR_TEST.AIRLINE_DATASET_TEMP t
    WHERE NOT EXISTS (
        SELECT 1
        FROM AIR_TEST.SRC_AIRLINE_DATASET d
        WHERE t.passenger_id = d.passenger_id
          AND t.first_name = d.first_name
          AND t.last_name = d.last_name
          AND t.gender = d.gender
          AND t.age = d.age
          AND t.nationality = d.nationality
          AND t.airport_name = d.airport_name
          AND t.airport_country_code = d.airport_country_code
          AND t.country_name = d.country_name
          AND t.airport_continent = d.airport_continent
          AND t.continents = d.continents
          AND t.departure_date = d.departure_date
          AND t.arrival_airport = d.arrival_airport
          AND t.pilot_name = d.pilot_name
          AND t.flight_status = d.flight_status
          AND t.ticket_type = d.ticket_type
          AND t.passenger_status = d.passenger_status
    )
    GROUP BY
        passenger_id, first_name, last_name, gender, age, nationality,
        airport_name, airport_country_code, country_name, airport_continent,
        continents, departure_date, arrival_airport, pilot_name, flight_status,
        ticket_type, passenger_status;
    COMMIT;
    v_rows_loaded := (SELECT COUNT(*) FROM TMP_AIR_DATA);

    INSERT INTO AIR_TEST.SRC_AIRLINE_DATASET (
        passenger_id, first_name, last_name, gender, age, nationality,
        airport_name, airport_country_code, country_name, airport_continent,
        continents, departure_date, arrival_airport, pilot_name, flight_status,
        ticket_type, passenger_status, update_ts
    )
    SELECT
        passenger_id, first_name, last_name, gender, age, nationality,
        airport_name, airport_country_code, country_name, airport_continent,
        continents, departure_date, arrival_airport, pilot_name, flight_status,
        ticket_type, passenger_status, update_ts
    FROM TMP_AIR_DATA;
    COMMIT;
    updated_tables := OBJECT_INSERT(updated_tables, 'SRC_AIRLINE_DATASET', v_rows_loaded, TRUE);

    v_step_end_ts := CURRENT_TIMESTAMP();
    v_step_duration := DATEDIFF('millisecond', v_step_start_ts, v_step_end_ts) / 1000.0;

    CALL ADMIN.SP_LOG_ETL(:v_schema_name, :v_proc_name, :v_step_start_ts, :v_step_end_ts,
       :v_rows_loaded, 'OK', 'SRC_AIRLINE_DATASET uploaded successfully', :v_step_duration);

    v_proc_end_ts := CURRENT_TIMESTAMP();
    v_proc_duration := DATEDIFF('millisecond', v_proc_start_ts, v_proc_end_ts) / 1000.0;

    CALL ADMIN.SP_LOG_ETL(:v_schema_name, :v_proc_name, :v_proc_start_ts, :v_proc_end_ts,
      NULL, :v_proc_status, :v_proc_msg, :v_proc_duration);
    COMMIT;
    RETURN updated_tables;
END;
$$;
