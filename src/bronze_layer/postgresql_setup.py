# create postgresql database on docker
import psycopg2
from psycopg2.extras import execute_batch

import os
from dotenv import load_dotenv # import env
load_dotenv()

# --- connect zone ---
print(f"connect to PostgreSQL...")

# 1. connect database + load env.
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)
cursor = conn.cursor() # start conn

table_name = "bronze_yfinance_gold"

command_sql = f"""
create table {table_name}
id BIGSERIAL PRIMARY KEY ไอดี

symbol TEXT สัญลักษณ์ UAXUSD
datetime TIMESTAMPTZ วันเวลาของแท่งเทียน
open DOUBLE PRECISION ราคาเปิดของแท่งเทียน
high DOUBLE PRECISION ราคาสูงสุดของแท่งเทียน
low DOUBLE PRECISION ราคาต่ำสุดของแท่งเทียน
close DOUBLE PRECISION ราคาปิดของแท่งเทียน
volume BIGINT 
dividends DOUBLE PRECISION
stock_splits DOUBLE PRECISION

source TEXT DEFAULT 'yfinance' ดึงข้อมูลมาจากไหน 'yfinacnce'
ingested_at TIMESTAMPTZ DEFAULT NOW() วันเวลาที่ดึงข้อมูลชุดนี้
batch_id TEXT เลขระบุ ครั้งที่ดึงข้อมูล
"""

execute_batch(cursor, command_sql)
conn.commit() 
print(f"Created database")
# --- END: connect zone ---


# --- Validate zone ---
command_sql = f"""select * from {table_name}"""
execute_batch(cursor, command_sql)
conn.commit() 
print(f"Check the table")
# --- END: Validate zone ---

# --- closing ---
cursor.close()
conn.close()
# --- END: closing conn ---