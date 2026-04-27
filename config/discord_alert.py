import requests
import datetime

import os
from dotenv import load_dotenv # import env
load_dotenv()

from config.settings import *
from config.connection_posgresql import *

def alert_discord():
    # Webhook URL here
    WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK")

    db = DatabaseManager()
    db.open_conn()

    df_bronze = db.execute_query(f"select * from {bronze_schema_table}") #get all data from bronze layer
    df_bronze = pd.DataFrame(df_bronze)
    df_bronze_row= df_bronze.shape[0]

    df_silver = db.execute_query(f"select * from {silver_schema_table}") #get all data from silver layer
    df_silver = pd.DataFrame(df_silver)
    df_silver_row= df_silver.shape[0]

    # current_time = datetime.utcnow()

    content_alert = f"""bronze_layer = Row:{df_bronze_row}\nsilver_layer = Row:{df_silver_row} """

    data = {
        "content": content_alert,
        "username": "Betabase"
    }

    response = requests.post(WEBHOOK_URL, json=data)

    if response.status_code == 204:
        print("Message sent successfully!")
    else:
        print(f"Failed to send message: {response.status_code}")
