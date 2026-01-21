from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="01_air_flight_core_refresh",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["air_flight", "core", "dims", "fact"],
) as dag:
    time_travel_dml = SQLExecuteQueryOperator(
        task_id="time_travel_dml_checks",
        conn_id="Snowflake_conn",
        sql="""
            -- DML #1: state of fact 5 minutes ago
            SELECT COUNT(*) AS cnt_fact_5min_ago
            FROM AIR_TEST.FACT_FLIGHT AT (OFFSET => -60*5);

            -- DML #2: state of dim 10 minutes ago
            SELECT COUNT(*) AS cnt_dim_customer_10min_ago
            FROM AIR_TEST.DIM_CUSTOMER AT (OFFSET => -60*10);
            """,
    )

    time_travel_ddl = SQLExecuteQueryOperator(
        task_id="time_travel_ddl_operations",
        conn_id="Snowflake_conn",
        sql="""
            -- DDL #1: clone DIM_CUSTOMER as it was 10 minutes ago
            CREATE OR REPLACE TABLE AIR_TEST.DIM_CUSTOMER_TT_CLONE
            CLONE AIR_TEST.DIM_CUSTOMER
            AT (OFFSET => -60*10);

            -- DDL #2: restore FACT_FLIGHT snapshot into a new table
            CREATE OR REPLACE TABLE AIR_TEST.FACT_FLIGHT_TT_RESTORE
            AS
            SELECT *
            FROM AIR_TEST.FACT_FLIGHT
            AT (OFFSET => -60*5);
        """,
    )


    refresh_dim_customer = SQLExecuteQueryOperator(
        task_id="refresh_dim_customer",
        conn_id="Snowflake_conn",
        sql="CALL AIR_TEST.SP_REFRESH_DIMS('dim_customer');",
    )

    refresh_dim_airport = SQLExecuteQueryOperator(
        task_id="refresh_dim_airport",
        conn_id="Snowflake_conn",
        sql="CALL AIR_TEST.SP_REFRESH_DIMS('dim_airport');",
    )

    refresh_fact = SQLExecuteQueryOperator(
        task_id="refresh_fact_flight",
        conn_id="Snowflake_conn",
        sql="CALL AIR_TEST.SP_REFRESH_FCT();",
    )
    setup_secure_view_and_rls = SQLExecuteQueryOperator(
        task_id="setup_secure_view_and_rls",
        conn_id="Snowflake_conn",
        sql="""
            ------------------------------------------------------------
            -- 1) SECURE VIEW
            ------------------------------------------------------------
            CREATE OR REPLACE SECURE VIEW AIR_TEST.V_FACT_FLIGHT_SECURE
            AS
            SELECT
                C.passenger_id,
                C.gender,
                C.age,
                C.nationality,
                A.airport_name,
                A.airport_country_code,
                A.country_name,
                A.airport_continent,
                A.continents,
                F.departure_date,
                F.arrival_airport,
                F.pilot_name,
                F.flight_status,
                F.ticket_type
            FROM AIR_TEST.FACT_FLIGHT F
            JOIN AIR_TEST.DIM_CUSTOMER C ON C.CUSTOMER_SK = F.CUSTOMER_SK
            JOIN AIR_TEST.DIM_AIRPORT  A ON A.AIRPORT_SK  = F.AIRPORT_SK;

            ------------------------------------------------------------
            -- 2) ROW ACCESS POLICY (filter rows by continent)
            ------------------------------------------------------------
            CREATE OR REPLACE ROW ACCESS POLICY AIR_TEST.RLP_FACT_FLIGHT_CONTINENT
            AS (continent STRING) RETURNS BOOLEAN ->
                EXISTS (
                    SELECT 1
                    FROM AIR_TEST.RLS_USER_CONTINENT m
                    WHERE m.user_name = CURRENT_USER()
                      AND m.continent = continent
                )
                OR CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN');

            ------------------------------------------------------------
            -- 3) ATTACH POLICY TO SECURE VIEW
            ------------------------------------------------------------
            ALTER VIEW AIR_TEST.V_FACT_FLIGHT_SECURE
            ADD ROW ACCESS POLICY AIR_TEST.RLP_FACT_FLIGHT_CONTINENT
            ON (continents);
        """,
    )

    done = EmptyOperator(task_id="done")

    time_travel_dml >> time_travel_ddl \
    >> refresh_dim_customer >> refresh_dim_airport \
    >> refresh_fact >> setup_secure_view_and_rls >> done


