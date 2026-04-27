import pandas as pd
from datetime import datetime
from config.connection_posgresql import *

def schema_check (df:pd.DataFrame):
    results_report = []
    total_rows, total_cols = df.shape
    dup_count = df.duplicated().sum()

    for col in df.columns:
        col_name = str(col)

        series = df[col]
        dtype = str(series.dtype)
        row_count = len(series)
        null_count = series.isna().sum()
        unique_count = series.nunique(dropna=True)
        duplicate_count = series.duplicated().sum()

        results_report.append({
            "column": col_name,
            "dtype": dtype,
            "row_count" :row_count,
            "null_count": null_count,
            "unique_count": unique_count,
            "duplicate_count": duplicate_count
        })    
    for i in results_report:
        print(i)



def pre_validate(df: pd.DataFrame,Required_Columns:list,datatype_Columns:list,not_null_column:list,pk_columns: list):
    print("--- pre vaildate: start ---")
    results_validate = 1
    # 1.empty check, row < 0.
    # 2.required_Columns check, the main column must have in ingestion.
    # 3.Data type check. the df have same datatype like previous lot before transform.
    # 4.duplicate check
    # 5.Null check(loop check each column), some column can have null some column must no null.


    # 1.empty check
    total_rows, total_cols = df.shape
    if total_rows <= 0 or total_cols <= 0:
       total_rows = -1
       total_cols = -1
       print("1.empty check: failed❌❌❌")
       print(total_rows,total_cols)
       return 0 #failed
    else: print(f"1.empty check: Col[{total_cols}], row[{total_rows}]✅✅✅")

    # 2.required_Columns
    missing_cols = []
    for col in Required_Columns:
        if col not in df.columns:
            missing_cols.append(col)            
    if len(missing_cols) > 0  or len(Required_Columns) != len(df.columns) :
        print("2.required_Columns check: failed❌❌❌")
        for missing_col in missing_cols :
            print("missing_col",missing_col)
        return 0
    else: print(f"2.required_Columns check: [all column name mactching]✅✅✅")
    
    # 3.Data type check
    missing_cols = []
    for col in Required_Columns:
        if col not in df.dtypes:
            missing_cols.append(col)   
    if len(missing_cols) > 0  :
        results_validate = 0
        print("3.Data type check: failed❌❌❌")
        return 0
    else: print(f"3.Data type check: [all column datatype mactching]✅✅✅")

    # 4.duplicate check
    dup_count = df.duplicated().sum()
    dup_percentage = dup_count *100 / total_rows
    if dup_percentage > 0:
        results_validate = 0
        print("4.duplicate check: failed❌❌❌")
        return 0
    else: print(f"4.duplicate check: [duplicate less = 0]✅✅✅")

    # 5.Null,dup check each column
    print(f"5.Quick report status:")
    for col in df.columns:
        col_name = str(col)
        
        series = df[col]
        total_row_col = len(series)
        null_count = series.isna().sum()

        if null_count == 0:
            null_percentage = 0
        elif null_count > 0 and col_name in not_null_column:
                print(f"""\t- {col_name}*: CRITICAL Null found{null_count}""")
                return 0 #failed
        else:
            null_percentage = null_count *100 / total_row_col
            if null_percentage > 5:
                results_validate=0


        unique_count = series.nunique(dropna=True)
        duplicate_count = series.duplicated().sum()
        if col_name in pk_columns:
            if duplicate_count > 0:
                print(f"""\t- {col_name+"[PK]"}*: CRITICAL dup found{duplicate_count}""")
                return 0
        
        display_name = col_name
        if col_name in pk_columns:
            display_name = display_name + "[PK]"
        if col_name in not_null_column:
            display_name = display_name + "*"

        print(f"""\t- {display_name}: null_count[{null_count}], null_percentage[{null_percentage}%] , unique_count[{unique_count}], duplicate_count[{duplicate_count}]""")
    
    print("--- pre vaildate: end ---\n")
    return results_validate

import pandas as pd



def post_validate(df_loaded: pd.DataFrame, ingested_batch_id: str, table: str, db: DatabaseManager):
    print("--- Starting Post-validation ---")
    # 1.Volume len(df)ที่เข้าไป และ df ที่อยู่บน database เท่ากัน (เช็คจาก ingested_batch_id เดียวกัน)
    # 2.completness ข้อมูลที่เข้าไป มี null เท่ากับก่อนเข้ามัย้้?
    # 3.Match data ข้อมูลที่เข้าไปคือชุดเดียวกับ df ที่ต้องการเข้ามั้ย? หยิบ 10% มาเปรียบเทียบทั้ง row
    
    # ดึงข้อมูลจริงจาก Database ของ Batch นี้ขึ้นมาเป็น DataFrame เพื่อใช้ตรวจสอบ
    cols_to_select = ", ".join(df_loaded.columns) # select * without id
    sql_get_data = f"SELECT {cols_to_select} FROM {table} WHERE ingested_batch_id = %s"
    raw_data = db.execute_query(sql_get_data, (ingested_batch_id,)) #แปลง เป็น dataframe
    
    if not raw_data:
        print("Post-validate: No data found in database for this batch ❌")
        return 0
    # แปลง List of Tuples เป็น DataFrame (ต้องดึงชื่อคอลัมน์มาให้ตรงกับ df_loaded ด้วย)
    df_database = pd.DataFrame(raw_data, columns=df_loaded.columns)

    # 1.Volume len(df)ที่เข้าไป และ df ที่อยู่บน database เท่ากัน (เช็คจาก ingested_batch_id เดียวกัน)
    if len(df_database) == len(df_loaded):
        print("Post validate: Volume Pass✅✅✅")
    else: 
        print(f'both columns count not equl: DB[{len(df_database)}] vs Load[{len(df_loaded)}] ❌')
        return 0
    
    # 2.completness ข้อมูลที่เข้าไป มี null เท่ากับก่อนเข้ามัย้้?
    null_count_totalA = 0
    null_count_totalA = df_database.isna().sum().sum()
    
    null_count_totalB = 0
    null_count_totalB = df_loaded.isna().sum().sum()

    if null_count_totalA == null_count_totalB:
        print("Post validate: null count Pass✅✅✅")
    else: 
        print(f"Null mismatch: DB[{null_count_totalA}] vs Load[{null_count_totalB}] ❌")
        return 0
    
    # 3.Match data ข้อมูลที่เข้าไปคือชุดเดียวกับ df ที่ต้องการเข้ามั้ย? หยิบ 10% มาเปรียบเทียบทั้ง row
    # 10% ปัดขึ้น จาก database มาเช็คกับ df
    random_rows = max(1, round(len(df_database) * 0.1)) # ป้องกันกรณี 10% แล้วได้ 0
    df_database_10percent = df_database.sample(n=random_rows)

    # เพราะ datatype บน python และ database จูนกันยาก -> แปลงเป็น str ให้หมด ดูแต่ value พอ
    N = 5 #เช็คแค่ N ตัวแรกแทน เพราะมีปัญหาเรื่องทศนิยม
    df_db_loose = df_database_10percent.astype(str).apply(lambda x: x.str[:N])
    df_loaded_loose = df_loaded.astype(str).apply(lambda x: x.str[:N])

    # นำมาเปรียบเทียบกัน
    df_sum = df_db_loose.merge(df_loaded_loose, how='inner')
    
    # เช็คผลลัพธ์แบบหลวมๆ
    if len(df_sum) >= random_rows:
        print("Post validate: Match data 10% (Loose Match) PASSED WITH CONDITION ⚠️ ✅")
        return 1
    else: 
        print(f"Data Match Failed: Only {len(df_sum)}/{random_rows} rows matched ❌")
        return 0
        
    return 1 # ทุกด่านผ่านหมด



def incremental_load(df:pd.DataFrame,pk_column:str,table:str):
    existing_pks = []
    # 1.query the pk from database
    # 2.unselect df by pk from database, we will got brand new rows who have pk not in the database
    # 3.retrun it, ready for load

    # 1.query the pk from database
    if pk_column !="" :
        db = DatabaseManager()
        db.open_conn()
        sql_query = f"Select {pk_column} from {table}"
        raw_pks = db.execute_query(sql_query)
        db.close_conn()

        # เช็คก่อนว่ามีข้อมูลไหม ป้องกัน Error
        if raw_pks is not None:
            existing_pks = {row[0] for row in raw_pks}
        else:
            existing_pks = set() # ถ้าว่างให้เป็น set เปล่า = db.execute_query(sql_query)

        existing_pks = {row[0] for row in raw_pks}
    # 2.unselect df by pk from database, we will got brand new rows who have pk not in the database
    if existing_pks:
        ready_df_non_existing_pks = df[~df[pk_column].isin(existing_pks)].copy()
    else:
        # ถ้า database ว่างเปล่า (ไม่มี PK เลย) ให้ใช้ข้อมูลทั้งหมดที่มี
        ready_df_non_existing_pks = df.copy()

    # 3.retrun it, ready for load
    print(f"Incremental Rows: ready to load:[{len(ready_df_non_existing_pks)}], not_select:[{len(df)-len(ready_df_non_existing_pks)}]")
    
    return ready_df_non_existing_pks