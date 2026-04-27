import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def open_conn(self):
        try:
            self.conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT"),
                dbname=os.getenv("POSTGRES_DATABASE_NAME"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD")
            )
            self.cursor = self.conn.cursor()
            print("--- Database Connected ---")
        except Exception as e:
            print(f"--- Failed to Connect: {e} ---")

    def execute_query(self, sql, params=None):
        """สำหรับรันคำสั่ง SQL และคืนค่าผลลัพธ์ (ถ้ามี)"""
        try:
            self.cursor.execute(sql, params)
            
            # ตรวจสอบว่าคำสั่ง SQL มีการคืนค่าข้อมูลออกมาหรือไม่ (เช่น SELECT)
            if self.cursor.description is not None:
                data = self.cursor.fetchall()
                print("Select complete")
                return data
            else:
                # ถ้าไม่มีข้อมูลคืนมา (INSERT, UPDATE, DELETE) ให้ Commit ข้อมูล
                self.conn.commit()
                print("Query complete")
                return None
                
        except Exception as e:
            if self.conn:
                self.conn.rollback()
            print(f"Error: {e}")
            return None

    def close_conn(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("--- Connection Closed ---\n")

# --- วิธีนำไปใช้งาน ---
# db = DatabaseManager()
# db.open_conn()

# 1. สำหรับการดึงข้อมูล (SELECT)
# data = db.execute_query("SELECT * FROM bronze_yfinance_gold LIMIT 5")
# if data:
#     for row in data:
#         print(row)

# 2. สำหรับการใส่ข้อมูล (INSERT)
# db.execute_query("INSERT INTO table_name (col) VALUES (%s)", ('example_data',))

# db.close_conn()

def db_load(df: pd.DataFrame, schema_table_name: str, db_manager):
    # schema = "bronze_layer"
    if df.empty:
        print("No data to load.")
        return

    # 1. เตรียม SQL (ไม่ต้องใส่คอลัมน์ 'id' เพราะเราให้ Postgres รัน BIGSERIAL เอง)
    # และใช้ %s เป็นตัวแทนของข้อมูล (Placeholder)
    columns = list(df.columns)
    column_names = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    
    sql = f"INSERT INTO {schema_table_name} ({column_names}) VALUES ({placeholders})"

    # 2. แปลง DataFrame เป็น List of Tuples
    # เช่น [('GC=F', '2026-04-18', 2300.5), (...)]
    data_values = [tuple(x) for x in df.values]

    # 3. ส่งข้อมูลเข้า Database
    try:
        # ใช้ cursor จาก DatabaseManager ที่เราทำไว้
        execute_batch(db_manager.cursor, sql, data_values)
        db_manager.conn.commit()
        print(f"Successfully loaded {len(df)} rows to {schema_table_name}")
    except Exception as e:
        db_manager.conn.rollback()
        print(f"Failed to load data: {e}")