# lib for connect DB
import psycopg2
from psycopg2.extras import execute_batch

import os
from dotenv import load_dotenv # import env
load_dotenv()

# lib for extract data
import yfinance as yf 
import pandas as pd
from datetime import datetime
import uuid
# import settings
from config.settings import SYMBOL, YEARS_BACK, DB_NAME, DB_PORT

#1. Extract data from Yahoo Finance
def extract_data():
    print(f"Extracting data for {SYMBOL} from Yahoo Finance...")

    ticker = yf.Ticker(SYMBOL)
    
    df = ticker.history(period=YEARS_BACK) # 1. Get historical market data
    df = df.reset_index() # reset index เพื่อเอา Date ออกมาเป็น column

    batch_id = str(uuid.uuid4()) # 2. Generate batch_id (unique ต่อ run)
    
    # 3. Add metadata columns
    df["symbol"] = SYMBOL
    df["source"] = "yfinance"
    df["ingested_at"] = datetime.utcnow()
    df["batch_id"] = batch_id

    df = df.rename(columns={
        "Date": "datetime",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
        "Dividends": "dividends",
        "Stock Splits": "stock_splits"
    })

    return df #extract raw data + batch infomation

#2. load data to postgresql

def load_data(df):
    print(f"Loading data for {SYMBOL} to PostgreSQL...")

    # 1. ดึงค่าจาก .env
    DB_HOST = os.getenv("POSTGRES_HOST")
    DB_PORT = os.getenv("POSTGRES_PORT")
    DB_NAME = os.getenv("POSTGRES_DB")
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

    # 2. connect database
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    # 3. เลือก column ที่จะ insert
    columns = [
        "symbol", "datetime", "open", "high", "low", "close",
        "volume", "dividends", "stock_splits",
        "source", "ingested_at", "batch_id"
    ]

    # 4. แปลง df → list of tuples
    data = [tuple(row[col] for col in columns) for _, row in df.iterrows()]

    # 5. สร้าง SQL
    insert_query = f"""
        INSERT INTO bronze_yfinance_gold (
            id,
            symbol, 
            datetime, 
            open, 
            high, 
            low, 
            close,
            volume, 
            dividends, 
            stock_splits,
            source, 
            ingested_at, 
            batch_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # 6. execute_batch batch insert
    execute_batch(cursor, insert_query, data, page_size=1000)

    conn.commit() 

    print(f"Inserted : {len(data)} rows")

    cursor.close()
    conn.close()

#3. validate data
def validate_data():
    print(f"Validating data for {SYMBOL}...")
    # ...


def main():
    df = extract_data()
    load_data(df)
    validate_data()

if __name__ == "__main__":
    main()