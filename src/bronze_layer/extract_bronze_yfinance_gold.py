# lib for connect DB
import psycopg2
from psycopg2.extras import execute_batch

import os
from dotenv import load_dotenv # import env
load_dotenv()

# lib for extract data
import yfinance as yf 
import pandas as pd
# from datetime import datetime
import datetime
# import settings
from config.settings import SYMBOL, YEARS_BACK
from config.discord_alert import *
from config.validate_tool import *
from config.connection_posgresql import *



print(f"Extracting data for {SYMBOL} from Yahoo Finance...")

# 0. Get the symbol(Gold) form config
ticker = yf.Ticker(SYMBOL)

# 1. Get historical market data
df = ticker.history(period=YEARS_BACK) 
df = df.reset_index() # reset index เพื่อเอา Date ออกมาเป็น column

# 2. Generate batch_id (unique ต่อ run)
ingested_batch_id = datetime.now().strftime('%Y%m%d%H%M%S%f')

# 3. Add metadata columns
# df["id"] สร้าง database ด้วย BIGSERIAL -> ไม่ต้องส่ง id เข้า
df["symbol"] = SYMBOL
df["source"] = "yfinance"
df["ingested_at"] = datetime.utcnow()
df["ingested_batch_id"] = ingested_batch_id

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

print(f"Loading data for {SYMBOL} to PostgreSQL...")

# เช็ค pre_validation (ข้อมูล col เหมือนกันทุกการนำเข้า)
req_col = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'dividends','stock_splits', 'symbol', 'source', 'ingested_at', 'ingested_batch_id']
req_datatype = ['object', 'float64', 'float64', 'float64', 'float64', 'int64', 'float64', 'float64', 'object', 'object', 'object', 'object']
not_null_col = ['datetime','symbol']
pk_col = ['datetime']
pre_validate_result = pre_validate(df,req_col,req_datatype,not_null_col,pk_col) #ฟังชั่น pre_validate

if pre_validate_result :
    df = incremental_load(df,"datetime","bronze_layer.bronze_yfinance_gold") #incremental_load ถ้ามี data ไหนใน db แล้ว ไม่ต้องเอาเข้า
    r_u_sure = input("Are u still input it?(Y/N): ") #confirm ว่าจะเอาเข้ามั้ย?
    if len(df) != 0 and r_u_sure.lower() == "y"  :
        db = DatabaseManager()
        db.open_conn()
        db_load(df,"bronze_layer.bronze_yfinance_gold",db) #ฟังชั่นนำเข้า
        post_validate_result = post_validate(df,ingested_batch_id,"bronze_layer.bronze_yfinance_gold",db) #ฟังชั่น post_validate

        if post_validate_result == 0: #ถ้า post validate ไม่ผ่าน ให้ลบ ingested_batch_id นี้มั้ย
            print(f"batch: {ingested_batch_id} not Pass")
            r_u_sure = input("Are u delete it?(Y/N): ")
            if r_u_sure.lower() == "y"  :
                db.execute_query(f"DELETE FROM bronze_layer.bronze_yfinance_gold WHERE ingested_batch_id = %s", (ingested_batch_id,))
                print("Delete complete. Please investigate the issue.")
            else:
                print("ok")
        elif post_validate_result == 1:
            alert_discord()
        
        db.close_conn()
    else : 
        print("No new data or user skipped it.")