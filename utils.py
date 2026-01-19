from enum import Enum
import os
import pandas as pd
from sqlalchemy import create_engine, text

# --- Snowflake connection parameters ---
sf_config = {
    # из твоего Snowflake UI:
    # account locator (как в Host): KFQFYQ-GV16804.snowflakecomputing.com
    # В SQLAlchemy нужно БЕЗ домена:
    'account': 'KFQFYQK-SR25378.eu-west-1.aws',  # <-- важно: только locator, без .snowflakecomputing.com

    'user': 'henrymatisson@gmail.com',
    'password': "WRKJZT89p8FAKvN",

    'role': 'ACCOUNTADMIN',  # ты сама выяснила, что role обязателен ✅
    'warehouse': 'COMPUTE_WH',
    'database': 'BASE_SCHEMA',
    'schema': 'AIR_TEST',  # аналог твоего DB_SCHEMA
}

# --- SQLAlchemy connection string for Snowflake ---
connection_string = (
    f"snowflake://henrymatisson:WRKJZT89p8FAKvN@KFQFYQK-GV16804/BASE_SCHEMA/AIR_TEST?warehouse=COMPUTE_WH&role=ACCOUNTADMIN"
)

ENGINE = create_engine(connection_string)

# --- Files / schema ---
SRC_FILE = r"C:\Users\henry\PycharmProjects\Snowflake_task\Airline_Dataset.csv"
DB_SCHEMA = sf_config["schema"]

if __name__ == '__main__':
    try:
        print("--- Попытка подключения к Snowflake ---")

        # Используем ENGINE из вашего файла utils.py
        with ENGINE.connect() as conn:
            # 1. Простейший запрос на версию
            result = conn.execute(text("SELECT CURRENT_VERSION()")).fetchone()
            print(f"✅ Успешное подключение!")
            print(f"Версия Snowflake: {result[0]}")

            # 2. Проверяем контекст (базу и схему)
            context = conn.execute(text("""
                SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE(), CURRENT_ROLE()
            """)).fetchone()

            print(f"\nКонтекст сессии:")
            print(f"  База данных: {context[0]}")
            print(f"  Схема:       {context[1]}")
            print(f"  Warehouse:   {context[2]}")
            print(f"  Роль:        {context[3]}")

    except Exception as e:
        print(f"❌ Ошибка подключения!")
        print(f"Детали: {e}")
