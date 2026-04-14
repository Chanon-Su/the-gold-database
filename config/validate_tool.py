import pandas as pd
import numpy as np
from datetime import datetime

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
    results_validate = 1
    # 1.empty check, row < 0.
    # 2.required_Columns check, the main column must have in ingestion.
    # 3.Data type check. the df have same datatype like previous lot before transform.
    # 4.duplicate check
    # 5.Null check(loop check each column), some column can have null some column must no null.


    # 1.empty check
    total_rows, total_cols = df.shape
    if total_rows <= 0 or total_cols <= 0 or len(Required_Columns) != len(df.columns) or len(datatype_Columns) != len(df.dtypes) :
       total_rows = -1
       total_cols = -1
       print("1.empty check: failed❌❌❌")
       return 0 #failed
    else: print(f"1.empty check: Col[{total_cols}], row[{total_rows}]✅✅✅")

    # 2.required_Columns
    missing_cols = []
    for col in Required_Columns:
        if col not in df.columns:
            missing_cols.append(col)            
    if len(missing_cols) > 0  :
        results_validate = 0
        print("2.required_Columns check: failed❌❌❌")
    else: print(f"2.required_Columns check: [all column name mactching]✅✅✅")
    
    # 3.Data type check
    missing_cols = []
    for col in Required_Columns:
        if col not in df.dtypes:
            missing_cols.append(col)   
    if len(missing_cols) > 0  :
        results_validate = 0
        print("3.Data type check: failed❌❌❌")
    else: print(f"3.Data type check: [all column datatype mactching]✅✅✅")

    # 4.duplicate check
    dup_count = df.duplicated().sum()
    dup_percentage = dup_count *100 / total_rows
    if dup_percentage > 0:
        results_validate = 0
        print("4.duplicate check: failed❌❌❌")
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
    
    return results_validate