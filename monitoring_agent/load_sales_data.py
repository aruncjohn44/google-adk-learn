import os
import csv
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "dbname": os.getenv("DB_NAME", "mydb"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

CSV_FILE = "monitoring_agent/data/chocolate_sales.csv"

def parse_amount(amount_str):
    """Convert '$5,320.00' to 5320.00"""
    return float(amount_str.replace('$', '').replace(',', ''))

def parse_date(date_str):
    """Convert 'DD/MM/YYYY' to 'YYYY-MM-DD'"""
    return datetime.strptime(date_str, '%d/%m/%Y').strftime('%Y-%m-%d')

def create_table(cursor):
    """Create the chocolate_sales table if it doesn't exist"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS chocolate_sales (
        id SERIAL PRIMARY KEY,
        sales_person VARCHAR(100),
        country VARCHAR(50),
        product VARCHAR(100),
        date DATE,
        amount DECIMAL(10, 2),
        boxes_shipped INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_sql)
    print("Table 'chocolate_sales' created or already exists.")

def load_csv_data(cursor):
    """Load CSV data into the database"""
    rows = []
    
    with open(CSV_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (after header)
            try:
                parsed_row = (
                    row['Sales Person'],
                    row['Country'],
                    row['Product'],
                    parse_date(row['Date']),
                    parse_amount(row['Amount']),
                    int(row['Boxes Shipped'])
                )
                rows.append(parsed_row)
            except Exception as e:
                print(f"Error parsing row {row_num}: {e}")
                continue
    
    if not rows:
        print("No data to insert.")
        return
    
    insert_sql = """
    INSERT INTO chocolate_sales (sales_person, country, product, date, amount, boxes_shipped)
    VALUES %s
    """
    
    try:
        execute_values(cursor, insert_sql, rows)
        print(f"Successfully inserted {len(rows)} rows into chocolate_sales table.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        raise

def main():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    
    try:
        with conn.cursor() as cursor:
            create_table(cursor)
            load_csv_data(cursor)
        
        conn.commit()
        print("Data loaded successfully!")
    
    except Exception as e:
        conn.rollback()
        print(f"Error occurred: {e}")
        raise
    
    finally:
        conn.close()
        print("Connection closed.")

if __name__ == "__main__":
    main()
