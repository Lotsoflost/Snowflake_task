from sqlalchemy import text
import pandas as pd
import re
from datetime import datetime, timezone

import utils


def normalize_columns(cols):
    return [re.sub(r"[^a-z0-9]+", "_", str(c).lower()).strip("_") for c in cols]


def record_log(proc_name, start_ts, end_ts, rows_loaded, status, message, duration_sec):
    # твоя процедура теперь ожидает rows_inserted (не rows)
    with utils.ENGINE.begin() as conn:
        conn.execute(
            text("""
                CALL ADMIN.SP_LOG_ETL(
                    :schema_name,
                    :proc_name,
                    :start_ts,
                    :end_ts,
                    :rows_inserted,
                    :status,
                    :msg,
                    :duration_sec
                )
            """),
            {
                "schema_name": utils.DB_SCHEMA,
                "proc_name": proc_name,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "rows_inserted": rows_loaded,
                "status": status,
                "msg": (message or "")[:250],
                "duration_sec": float(duration_sec) if duration_sec is not None else None,
            }
        )


if __name__ == "__main__":
    # Названия таблиц (одна таблица)
    base_table = "airline_dataset"       # как хочешь назвать логически
    temp_table = f"{base_table}_temp"    # итоговая temp

    start_ts = datetime.now(timezone.utc)
    status = "OK"
    message = f"{temp_table} loaded successfully"
    rows_loaded = 0

    try:
        print(f"Reading CSV: {utils.SRC_FILE}")
        df = pd.read_csv(utils.SRC_FILE)

        # normalize columns
        df.columns = normalize_columns(df.columns)

        # tech колонка
        df["update_ts"] = datetime.now(timezone.utc)

        print(f"Writing to Snowflake: {utils.DB_SCHEMA}.{temp_table} ...")

        # Важно: в Snowflake лучше явно передать schema=..., name=...
        df.to_sql(
            name=temp_table,
            con=utils.ENGINE,
            schema=utils.DB_SCHEMA,
            if_exists="replace",   # для temp ок
            index=False,
            method="multi",
            chunksize=10_000
        )

        rows_loaded = len(df)
        print(f"OK: wrote {rows_loaded} rows into {utils.DB_SCHEMA}.{temp_table}")

    except Exception as e:
        status = "ERROR"
        message = f"Failed to load {utils.DB_SCHEMA}.{temp_table}: {e}"
        print(message)

    finally:
        end_ts = datetime.now(timezone.utc)
        duration_sec = (end_ts - start_ts).total_seconds()
        record_log(
            proc_name=f"load {utils.DB_SCHEMA}.{temp_table}",
            start_ts=start_ts,
            end_ts=end_ts,
            rows_loaded=rows_loaded,
            status=status,
            message=message,
            duration_sec=duration_sec
        )
        print(f"Logged: {status} ({duration_sec:.3f}s)")
# -------------------------------------------------
    # NOW CALL fn_upload_src (only if temp load OK)
    # -------------------------------------------------
    if status == "OK":
        print("Calling fn_upload_src() to push temp -> SRC...")

        upload_start_ts = datetime.now()
        upload_status = "OK"
        upload_msg = "sp_upload_src completed successfully"

        updated_tables = ''
        try:
            with utils.ENGINE.begin() as conn:
                result = conn.execute(
                    text(f"CALL {utils.DB_SCHEMA}.sp_upload_src();")
                ).fetchone()
                updated_tables = result[0] if result else None
            print("sp_upload_src() finished.")

        except Exception as e:
            upload_status = "ERROR"
            upload_msg = f"sp_upload_src failed: {e}"
            global_status = "ERROR"
            global_msg = "temp load ok, but fn_upload_src failed"
            print(upload_msg)

        finally:
            upload_end_ts = datetime.now()
            upload_duration_sec = (upload_end_ts - upload_start_ts).total_seconds()
            record_log(
                f'fn_upload_src',
                upload_start_ts,
                upload_end_ts,
                None,
                upload_status,
                upload_msg[:250],
                upload_duration_sec
            )

        print(f"finished src layer {updated_tables=}")

        # # Trigger downstream refreshes based on fn_upload_src result
        # if updated_tables:
        #     tables = [table for table, value in updated_tables.items() if value > 1]
        #     with ENGINE.begin() as conn:
        #         for upd_table in tables:
        #             if upd_table == "fct_sales_data":
        #                 conn.execute(text(f"CALL {utils.DB_SCHEMA}.sp_refresh_fct();"))
        #             else:
        #                 conn.execute(text(f"CALL {utils.DB_SCHEMA}.sp_refresh_dims('{upd_table}');"))
        #             print(f"{upd_table} is refreshed.")