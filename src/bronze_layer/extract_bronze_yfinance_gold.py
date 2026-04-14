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



def extract_data():
    print(f"Extracting data for {SYMBOL} from Yahoo Finance...")

    # 0. Get the symbol(Gold) form config
    ticker = yf.Ticker(SYMBOL)
    
    # 1. Get historical market data
    df = ticker.history(period=YEARS_BACK) 
    df = df.reset_index() # reset index เพื่อเอา Date ออกมาเป็น column

    # 2. Generate batch_id (unique ต่อ run)
    batch_id = str(uuid.uuid4())
    
    # 3. Add metadata columns
    df["symbol"] = SYMBOL
    df["source"] = "yfinance"
    df["ingested_at"] = datetime.utcnow()
    df["batch_id"] = batch_id

    # 4. rename columns
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



def load_data(df):
    print(f"Loading data for {SYMBOL} to PostgreSQL...")

    # 1. connect database + load env.
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )
    cursor = conn.cursor() # start conn

    # 2. เลือก column ที่จะ insert
    columns = [
        "id",
        "symbol", 
        "datetime", 
        "open", 
        "high", 
        "low", 
        "close",
        "volume", 
        "dividends", 
        "stock_splits",
        "source", 
        "ingested_at", 
        "batch_id"
    ]

    # 3. แปลง df → list of tuples
    data = [tuple(row[col] for col in columns) for _, row in df.iterrows()]

    # 4. สร้าง SQL
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

    # 5. execute_batch batch insert
    execute_batch(cursor, insert_query, data, page_size=1000)
    conn.commit() 

    print(f"Inserted : {len(data)} rows")

    # 6. closing the conn
    cursor.close()
    conn.close()



def validate_data():
    print(f"Validating data for {SYMBOL}...")
    # 1. Validate schema: Data type length
	# 2. Validate error: No null, No duplicate
	# 3. Validate Consistency: syntax,Symantec
    # 4. Validate logic: logic check



def main():
    df = extract_data() #extract gold data from y_finace API into df
    load_data(df) #input df(gold y_finance) into database
    validate_data() #check the data

if __name__ == "__main__":
    main()