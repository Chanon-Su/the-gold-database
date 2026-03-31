import yfinance as yf 
import psycopg2
import pandas as pd

# import env
import os
from dotenv import load_dotenv
load_dotenv()

# import settings
from config.settings import SYMBOL, YEARS_BACK, DB_NAME, DB_PORT


#1. Extract data from Yahoo Finance

def extract_data():
    print(f"Extracting data for {SYMBOL} from Yahoo Finance...")
    gold = yf.Ticker(SYMBOL)

    # 1. Get stock info
    # print(gold.info)

    # 2. Get historical market data (defaults to max)
    hist = gold.history(period=f'{YEARS_BACK}')
    print(hist)

#2. load data to postgresql
def load_data():
    print(f"Loading data for {SYMBOL} to PostgreSQL...")
#     DB_HOST = os.getenv("DB_HOST")
#     DB_PORT = os.getenv("DB_PORT")
#     DB_NAME = os.getenv("DB_NAME")
#     DB_USERNAME = os.getenv("DB_USERNAME")
#     DB_PASSWORD = os.getenv("DB_PASSWORD")
    

#3. validate data
def validate_data():
    print(f"Validating data for {SYMBOL}...")


def main():
    extract_data()
    load_data()
    validate_data()

if __name__ == "__main__":
    main()