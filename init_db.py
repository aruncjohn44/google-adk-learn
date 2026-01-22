import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

def run_sql_file(cursor, file_path):
    with open(file_path, "r") as f:
        sql = f.read()
        cursor.execute(sql)

def main():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False

    try:
        with conn.cursor() as cursor:
            print("Creating tables...")
            run_sql_file(cursor, "sql/create_tables.sql")

            print("Inserting sample data...")
            run_sql_file(cursor, "sql/insert_sample_data.sql")

        conn.commit()
        print("✅ Database initialized successfully!")

    except Exception as e:
        conn.rollback()
        print("❌ Error occurred:", e)
        raise

    finally:
        conn.close()

if __name__ == "__main__":
    main()
