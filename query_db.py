import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "dbname": os.getenv("DB_NAME", "mydb"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

QUERIES = [
    "SELECT COUNT(*) FROM sales_fact;",
    "SELECT * FROM products;"
]

def main():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            for query in QUERIES:
                print(f"\nExecuting: {query}")
                cursor.execute(query)
                rows = cursor.fetchall()
                for row in rows:
                    print(row)
    except Exception as e:
        print("❌ Error occurred:", e)
        raise
    finally:
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()
