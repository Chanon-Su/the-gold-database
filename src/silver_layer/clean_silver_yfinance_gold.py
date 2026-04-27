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
from config.settings import *
from config.discord_alert import *
from config.validate_tool import *
from config.connection_posgresql import *

db = DatabaseManager()



# query from bronze and unselect data from silver -> brand new data into silver
db.open_conn()
df_bronze = db.execute_query(f"select * from {bronze_schema_table}") #get all data from bronze layer
df_bronze_brandnew = incremental_load(df_bronze,"datetime",silver_schema_table) # use incremental_load() to unselect data
db.close_conn()



# Validate & data quality zone
df_silver = df_bronze_brandnew.copy()
df_silver["cleaned_flag"] = True

## logic Deduplication, Nulls/Missing values, datatypes,formanting,
if xxx :
    df_silver["cleaned_flag"] = False




# batch_info and load into silver table
db.open_conn #connect to db
## batch_info
df_silver["cleaned_at"] = datetime.utcnow()
cleaned_batch_id = datetime.now().strftime('%Y%m%d%H%M%S%f')
df_silver["cleaned_batch_id"] = cleaned_batch_id

## load to database, silver table
db_load(df_silver,silver_schema_table,db)

## check the loaded data in table
batch_id_col = "cleaned_batch_id"

post_validate_result = post_validate(df_silver,cleaned_batch_id,silver_schema_table,db) #ฟังชั่น post_validate
if post_validate_result == 0: #ถ้า post validate ไม่ผ่าน ให้ลบ ingested_batch_id นี้มั้ย
    print(f"batch: {batch_id_col} not Pass")
    r_u_sure = input("Are u delete it?(Y/N): ")
    if r_u_sure.lower() == "y"  :
        db.execute_query(f"DELETE FROM {silver_schema_table} WHERE {batch_id_col} = %s", (cleaned_batch_id,))
        print("Delete complete. Please investigate the issue.")
    else:
        print("ok")
elif post_validate_result == 1:
            alert_discord()

db.close_conn() #disconnect to db
# df_silver