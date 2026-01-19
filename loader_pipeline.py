from sqlalchemy import text
import pandas as pd
import re
import json
from datetime import datetime, timezone

import utils


def normalize_columns(cols):
    return [re.sub(r"[^a-z0-9]+", "_", str(c).lower()).strip("_") for c in cols]


def record_log(proc_name, start_ts, end_ts, rows_loaded, status, message, duration_sec):
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


def call_proc(conn, sql: str):
    """Exec CALL and return first column (Snowflake procs return 1 col when RETURNS ...)."""
    row = conn.execute(text(sql)).fetchone()
    return row[0] if row else None


if __name__ == "__main__":
    base_table = "airline_dataset"
    temp_table = f"{base_table}_temp"

    start_ts = datetime.now(timezone.utc)
    status = "OK"
    message = f"{temp_table} loaded successfully"
    rows_loaded = 0

    try:
        print(f"Reading CSV: {utils.SRC_FILE}")
        df = pd.read_csv(utils.SRC_FILE)

        df.columns = normalize_columns(df.columns)
        df["update_ts"] = datetime.now(timezone.utc)

        print(f"Writing to Snowflake: {utils.DB_SCHEMA}.{temp_table} ...")
        df.to_sql(
            name=temp_table,
            con=utils.ENGINE,
            schema=utils.DB_SCHEMA,
            if_exists="replace",
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
    # SRC load: temp -> SRC
    # -------------------------------------------------
    if status == "OK":
        print("Calling SP_UPLOAD_SRC() to push temp -> SRC...")

        upload_start_ts = datetime.now(timezone.utc)
        upload_status = "OK"
        upload_msg = "SP_UPLOAD_SRC completed successfully"
        updated_tables = None

        try:
            with utils.ENGINE.begin() as conn:
                updated_tables = call_proc(conn, f"CALL {utils.DB_SCHEMA}.SP_UPLOAD_SRC()")

            # может прийти строкой -> в dict
            if isinstance(updated_tables, str):
                updated_tables = json.loads(updated_tables)

            print("SP_UPLOAD_SRC() finished.")

        except Exception as e:
            upload_status = "ERROR"
            upload_msg = f"SP_UPLOAD_SRC failed: {e}"
            print(upload_msg)

        finally:
            upload_end_ts = datetime.now(timezone.utc)
            upload_duration_sec = (upload_end_ts - upload_start_ts).total_seconds()
            record_log(
                proc_name="SP_UPLOAD_SRC",
                start_ts=upload_start_ts,
                end_ts=upload_end_ts,
                rows_loaded=None,
                status=upload_status,
                message=upload_msg[:250],
                duration_sec=upload_duration_sec
            )

        print(f"finished src layer: {updated_tables=}")

        # -------------------------------------------------
        # Downstream refresh: DIMS -> FCT (only if SRC changed)
        # -------------------------------------------------
        src_rows = 0
        if isinstance(updated_tables, dict):
            src_rows = int(updated_tables.get("SRC_AIRLINE_DATASET", 0) or 0)

        if upload_status == "OK" and src_rows > 0:
            print(f"SRC changed ({src_rows} new rows). Refreshing DIMS and FACT...")

            # 1) refresh dims
            dims_start_ts = datetime.now(timezone.utc)
            dims_status = "OK"
            dims_msg = "dims refreshed successfully"

            try:
                with utils.ENGINE.begin() as conn:
                    call_proc(conn, f"CALL {utils.DB_SCHEMA}.SP_REFRESH_DIMS('dim_customer')")
                    call_proc(conn, f"CALL {utils.DB_SCHEMA}.SP_REFRESH_DIMS('dim_airport')")
                print("DIMS refreshed.")

            except Exception as e:
                dims_status = "ERROR"
                dims_msg = f"SP_REFRESH_DIMS failed: {e}"
                print(dims_msg)

            finally:
                dims_end_ts = datetime.now(timezone.utc)
                dims_duration = (dims_end_ts - dims_start_ts).total_seconds()
                record_log(
                    proc_name="SP_REFRESH_DIMS",
                    start_ts=dims_start_ts,
                    end_ts=dims_end_ts,
                    rows_loaded=None,
                    status=dims_status,
                    message=dims_msg[:250],
                    duration_sec=dims_duration
                )

            # 2) refresh fact only if dims ok
            if dims_status == "OK":
                fct_start_ts = datetime.now(timezone.utc)
                fct_status = "OK"
                fct_msg = "fact refreshed successfully"
                try:
                    with utils.ENGINE.begin() as conn:
                        fct_out = call_proc(conn, f"CALL {utils.DB_SCHEMA}.SP_REFRESH_FCT()")
                    # fct_out может быть VARIANT с rows
                    if isinstance(fct_out, str):
                        try:
                            fct_out = json.loads(fct_out)
                        except Exception:
                            pass
                    print("FACT refreshed. Output:", fct_out)

                except Exception as e:
                    fct_status = "ERROR"
                    fct_msg = f"SP_REFRESH_FCT failed: {e}"
                    print(fct_msg)

                finally:
                    fct_end_ts = datetime.now(timezone.utc)
                    fct_duration = (fct_end_ts - fct_start_ts).total_seconds()
                    record_log(
                        proc_name="SP_REFRESH_FCT",
                        start_ts=fct_start_ts,
                        end_ts=fct_end_ts,
                        rows_loaded=None,
                        status=fct_status,
                        message=fct_msg[:250],
                        duration_sec=fct_duration
                    )

        else:
            print(f"No SRC changes detected (src_rows={src_rows}, upload_status={upload_status}). Skipping DIMS/FCT.")
