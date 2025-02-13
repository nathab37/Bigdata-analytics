import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# PostgreSQL Database Connection Details
DB_USER = "postgres"
DB_PASS = "nathab"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "ecommerce_dbs"

# File path for transformed data
transformed_file = "data/transformed_kz.csv"

try:
    # Load the transformed CSV file
    df = pd.read_csv(transformed_file)
    print(f"Successfully loaded transformed data from {transformed_file}")
except Exception as e:
    print(f"Error loading {transformed_file}: {e}")
    exit()

# Establish a PostgreSQL connection using psycopg2
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    print("\nConnected to PostgreSQL successfully!")

    # Create table (modify schema as per your dataset)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS ecommerce_transactions (
        order_id VARCHAR(50) PRIMARY KEY,
        product_id VARCHAR(50),
        category_id TEXT,
        category_code TEXT,
        brand TEXT,
        price NUMERIC,
        user_id VARCHAR(50),
        event_time TIMESTAMP
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Table checked/created successfully!")

    # Load data into PostgreSQL using Pandas
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    df.to_sql("ecommerce_transactions", con=engine, if_exists="append", index=False)
    print(f"Data successfully loaded into PostgreSQL table: ecommerce_transactions")

except Exception as e:
    print(f"Error loading data into PostgreSQL: {e}")

finally:
    cursor.close()
    conn.close()
    print("Database connection closed.")
