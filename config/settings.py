# yfinance
# SYMBOL = "XAUUSD=X"
SYMBOL = "GC=F"
YEARS_BACK = '10y'
# https://ranaroussi.github.io/yfinance/

# setup silver table
bronze_schema = "bronze_layer"
bronze_table = "bronze_yfinance_gold"
bronze_schema_table = f"{bronze_schema}.{bronze_table}"

silver_schema = "silver_layer"
silver_table = "silver_yfinance_gold" # silver table on database
silver_schema_table = f"{silver_schema}.{silver_table}"