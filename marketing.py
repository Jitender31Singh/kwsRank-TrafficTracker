import pandas as pd
from datetime import date, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os

# CONFIG
SITE_URL = "https://piegaming.com/"
KEY_FILE = "electric-signal-491806-e4-6927fa72b120.json"
OUTPUT_FILE = "gsc_data.xlsx"

SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']

# AUTH
credentials = service_account.Credentials.from_service_account_file(
    KEY_FILE, scopes=SCOPES
)

service = build('searchconsole', 'v1', credentials=credentials)

# DATE (2 days back because GSC delay)
target_date = date.today() - timedelta(days=2)

# API CALL
response = service.searchanalytics().query(
    siteUrl=SITE_URL,
    body={
        "startDate": str(target_date),
        "endDate": str(target_date),
        "dimensions": ["query", "page"],
        "rowLimit": 25000
    }
).execute()

# PARSE
rows = []
for row in response.get("rows", []):
    rows.append({
        "date": str(target_date),
        "query": row["keys"][0],
        "page": row["keys"][1],
        "clicks": row["clicks"],
        "impressions": row["impressions"],
        "ctr": row["ctr"],
        "position": row["position"]
    })

df_new = pd.DataFrame(rows)

# APPEND WITHOUT DUPLICATES
if os.path.exists(OUTPUT_FILE):
    df_old = pd.read_excel(OUTPUT_FILE)
    df = pd.concat([df_old, df_new])
    df = df.drop_duplicates(subset=["date", "query", "page"])
else:
    df = df_new

df.to_excel(OUTPUT_FILE, index=False)

print(f"✅ Data saved for {target_date}")