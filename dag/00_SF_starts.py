# dags/air_flight_00_fetch_gdrive_csv.py

from __future__ import annotations

import re
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import json
import pandas as pd

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# ---------- helpers ----------
def normalize_columns(cols):
    return [re.sub(r"[^a-z0-9]+", "_", str(c).lower()).strip("_") for c in cols]


def fetch_and_prepare_csv(
    http_conn_id: str,
    output_path: str,
):
    """
    1) GET CSV via Airflow Http connection
    2) pandas read_csv
    3) normalize columns
    4) rename unnamed_0 -> n
    5) save to output_path
    """
    hook = HttpHook(method="GET", http_conn_id=http_conn_id)

    # Getting host
    host = hook.get_connection(http_conn_id).host

    # Using host
    resp = hook.run(host)
    resp.raise_for_status()
    content = resp.content

    # Read CSV
    df = pd.read_csv(BytesIO(content))

    # Normalize columns
    df.columns = normalize_columns(df.columns)
    df["update_ts"] = datetime.now(timezone.utc)

    # Rename first unnamed column if present
    if "unnamed_0" in df.columns:
        df = df.rename(columns={"unnamed_0": "n"})

    # Ensure output dir exists
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Save cleaned CSV
    df.to_csv(out, index=False)

    print(f"Saved cleaned CSV to: {out}")
    print(f"Rows: {len(df)}, Cols: {len(df.columns)}")
    print("Columns:", list(df.columns))


def print_previous_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='get')
    print(data)



def load_clean_csv_to_snowflake(
    snowflake_conn_id: str,
    table_fqn: str,          # "AIR_TEST.AIRLINE_DATASET_TEMP"
    local_csv_path: str,     # "/tmp/airline_dataset_clean.csv"
):
    """
    Replace-load:
      - ensure table exists (fixed schema)
      - truncate
      - PUT file -> table stage
      - COPY INTO table
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_fqn} (
        N                      NUMBER,
        passenger_id           STRING,
        first_name             STRING,
        last_name              STRING,
        gender                 STRING,
        age                    NUMBER,
        nationality            STRING,
        airport_name           STRING,
        airport_country_code   STRING,
        country_name           STRING,
        airport_continent      STRING,
        continents             STRING,
        departure_date         DATE,
        arrival_airport        STRING,
        pilot_name             STRING,
        flight_status          STRING,
        ticket_type            STRING,
        passenger_status       STRING,
        update_ts              TIMESTAMP_LTZ
    );
    """

    truncate_sql = f"TRUNCATE TABLE {table_fqn};"

    # internal stage of the table: @%<table_name>
    table_name_only = table_fqn.split(".")[-1]
    put_sql = f"PUT file://{local_csv_path} @%{table_name_only} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"

    copy_sql = f"""
    COPY INTO {table_fqn}
    FROM @%{table_name_only}
    FILE_FORMAT = (TYPE=CSV FIELD_DELIMITER=',' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"')
    ON_ERROR = 'ABORT_STATEMENT'
    PURGE = TRUE;
    """

    with hook.get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute(create_sql)
            cur.execute(truncate_sql)

            cur.execute(put_sql)

            cur.execute(copy_sql)

        finally:
            cur.close()

    print(f"Loaded (replaced) data into {table_fqn} from {local_csv_path}")

def call_sp_upload_src_and_push_xcom(
    snowflake_conn_id: str,
):
    """
    Calls SP_UPLOAD_SRC and returns number of inserted rows into SRC.
    This return value will be stored in XCom.
    """
    hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    with hook.get_conn() as conn:
        cur = conn.cursor()
        try:
            cur.execute("CALL AIR_TEST.SP_UPLOAD_SRC();")
            row = cur.fetchone()

            if not row:
                return 0

            result = row[0]

            # Snowflake may return VARIANT as string
            if isinstance(result, str):
                result = json.loads(result)

            rows_inserted = int(result.get("SRC_AIRLINE_DATASET", 0))
            print(f"SRC rows inserted: {rows_inserted}")

            return rows_inserted   #  XCOM

        finally:
            cur.close()

def branch_trigger_on_rows(**context):
    rows = context["ti"].xcom_pull(task_ids="sp_upload_src")
    try:
        rows = int(rows or 0)
    except Exception:
        rows = 0

    print(f"rows_inserted from sp_upload_src = {rows}")

    return "trigger_core_refresh_dag" if rows > 0 else "skip_trigger"


# ---------- DAG ----------
with DAG(
    dag_id="00_air_flight_fetch_gdrive_csv",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # запусти вручную
    catchup=False,
    tags=["air_flight", "gdrive", "stage"],
) as dag:

    fetch_prepare_save = PythonOperator(
        task_id="fetch_prepare_save_csv",
        python_callable=fetch_and_prepare_csv,
        op_kwargs={
            "http_conn_id": "air_flight_gdrive",
            # store inside Airflow container
            "output_path": "/tmp/airline_dataset_clean.csv",
        },
    )
    write_log_to_snowflake = SQLExecuteQueryOperator(
        task_id="write_log_to_snowflake",
        conn_id="Snowflake_conn",
        sql="""
            CALL ADMIN.SP_LOG_ETL(
                'ADMIN',
                'save csv',
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(),
                NULL,
                'STARTED',
                'fetch_prepare_save_csv completed',
                0
            );
        """,
    )

    load_to_snowflake = PythonOperator(
        task_id="load_clean_csv_to_snowflake_temp_replace",
        python_callable=load_clean_csv_to_snowflake,
        op_kwargs={
            "snowflake_conn_id": "Snowflake_conn",
            "table_fqn": "AIR_TEST.AIRLINE_DATASET_TEMP",
            "local_csv_path": "/tmp/airline_dataset_clean.csv",
        },
    )

    sp_upload_src = PythonOperator(
        task_id="sp_upload_src",
        python_callable=call_sp_upload_src_and_push_xcom,
        op_kwargs={
            "snowflake_conn_id": "Snowflake_conn",
        },
    )

    branch_trigger = BranchPythonOperator(
        task_id="branch_trigger_on_rows",
        python_callable=branch_trigger_on_rows,
    )

    skip_trigger = EmptyOperator(task_id="skip_trigger")
    done = EmptyOperator(task_id="done")

    trigger_core = TriggerDagRunOperator(
        task_id="trigger_core_refresh_dag",
        trigger_dag_id="01_air_flight_core_refresh",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    fetch_prepare_save \
    >> write_log_to_snowflake \
    >> load_to_snowflake \
    >> sp_upload_src \
    >> branch_trigger

    branch_trigger >> trigger_core >> done
    branch_trigger >> skip_trigger >> done
