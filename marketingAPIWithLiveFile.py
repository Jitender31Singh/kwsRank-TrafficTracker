import os
import time
import io
import requests
import pandas as pd
import logging
from datetime import date, timedelta
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build

# =========================
# 📝 LOGGING CONFIG
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log", mode="a")
    ]
)

logger = logging.getLogger(__name__)

# =========================
# 🔐 LOAD ENV VARIABLES
# =========================
load_dotenv()

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

SITE_ID = os.getenv("SITE_ID")
DRIVE_ID = os.getenv("DRIVE_ID")
FILE_ID = os.getenv("FILE_ID")

SITE_URL = os.getenv("GSC_SITE_URL")
KEY_FILE = os.getenv("GSC_KEY_FILE")

OUTPUT_FILE = os.getenv("OUTPUT_FILE", "output.xlsx")

# =========================
# 🔑 GRAPH API TOKEN MANAGER
# =========================
class GraphAPIClient:
    def __init__(self):
        self.access_token = None
        # self.access_token="eyJ0eXAiOiJKV1QiLCJub25jZSI6Il83QjM1bXdSbGk2UVRDR2dXSVhDV1hIbW9Fc1BGWDlQLTQyX182MDRDSW8iLCJhbGciOiJSUzI1NiIsIng1dCI6IlFaZ045SHFOa0dORU00R2VLY3pEMDJQY1Z2NCIsImtpZCI6IlFaZ045SHFOa0dORU00R2VLY3pEMDJQY1Z2NCJ9.eyJhdWQiOiJodHRwczovL2dyYXBoLm1pY3Jvc29mdC5jb20vIiwiaXNzIjoiaHR0cHM6Ly9zdHMud2luZG93cy5uZXQvMWE5OTZiZmYtYTRiYS00YzNhLTkxNDItNDIxYjNlMTdiM2ZlLyIsImlhdCI6MTc3NTAxNjYxNiwibmJmIjoxNzc1MDE2NjE2LCJleHAiOjE3NzUwMjA1MTYsImFpbyI6ImsyWmdZRmg3NGI3OTV5aC9KcE1FQmYwWDBhZTVBa3ZUdHBSODdyNHc4K3ZOU3pwYlpOVUEiLCJhcHBfZGlzcGxheW5hbWUiOiJTaGFyZVBvaW50LVhMU1gtT25seS1BcHAgIiwiYXBwaWQiOiI0YjBlNWI3ZC05NTNmLTQ2YjEtOTFmMC0wNzJjOTk2YzljNWUiLCJhcHBpZGFjciI6IjEiLCJpZHAiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC8xYTk5NmJmZi1hNGJhLTRjM2EtOTE0Mi00MjFiM2UxN2IzZmUvIiwiaWR0eXAiOiJhcHAiLCJvaWQiOiIzZmQ0MDJmYi1kOGY4LTRhN2ItOTI2ZS0xNzkxNTdmYjI4YjAiLCJyaCI6IjEuQWNZQV8ydVpHcnFrT2t5UlFrSWJQaGV6X2dNQUFBQUFBQUFBd0FBQUFBQUFBQUFBQUFER0FBLiIsInJvbGVzIjpbIkZpbGVzLlJlYWRXcml0ZS5BbGwiLCJCcm93c2VyU2l0ZUxpc3RzLlJlYWRXcml0ZS5BbGwiLCJTaXRlcy5GdWxsQ29udHJvbC5BbGwiXSwic3ViIjoiM2ZkNDAyZmItZDhmOC00YTdiLTkyNmUtMTc5MTU3ZmIyOGIwIiwidGVuYW50X3JlZ2lvbl9zY29wZSI6IkFTIiwidGlkIjoiMWE5OTZiZmYtYTRiYS00YzNhLTkxNDItNDIxYjNlMTdiM2ZlIiwidXRpIjoiMlUxc0c1YlRpRW0xb0FsOEhyOHNBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiMDk5N2ExZDAtMGQxZC00YWNiLWI0MDgtZDVjYTczMTIxZTkwIl0sInhtc19hY2QiOjE3NzQ5MzYxOTcsInhtc19hY3RfZmN0IjoiOSAzIiwieG1zX2Z0ZCI6ImYwQmF0UWYxeUdydk1PNk9CODFYcGVHam5FZm8yUTV3cnAxdU1JVGpKd1lCYTI5eVpXRnpiM1YwYUMxa2MyMXoiLCJ4bXNfaWRyZWwiOiI3IDI2IiwieG1zX3BmdGV4cCI6MTc3NTEwNjkxNiwieG1zX3JkIjoiMC40MkxqWUJKaU9zWW9KTUxCTGlUUXZ1OXVRWGVFbXZldXltUzF4YnRDNDRDaW5FSUM2VnlMR0N2MnIzZmNsZmV4LXRPeTRrZEFVUTRoQVdZR0NEZ0FwWUdpM0VJQ2doWnpEaDJjWDFfZTZCdVJ3eWNYNWd3QSIsInhtc19zdWJfZmN0IjoiMyA5IiwieG1zX3RjZHQiOjE3MzcxMDkxMDUsInhtc190bnRfZmN0IjoiMyA4In0.UH60sUYh-WwXKw_5mVRx_lJgC3Y3FW-KM2cP2-qF8XTGBX_IVSwGdWr5s7EM_BLC0T_1rEVbnmjPkb_r_nt1vTV6rJnanBxXLU-kP3NS-uVZO1WQ9s6fqLgkd2R6PMaunn3z_-L2Axio4becm5LZ5Oxd6fYaCfyKbN-OpulfVIww3j7DubtxhtIg99tSIqi7_UiOI5D-ceqO1R_-BgR03S0RREgiEVFh5eY17iHWmi-ob6LzbtU5RYOVExO9kll2BSEUQ-BNimKONJuDikVAQu22LOLSCROVCsz-GAFBjUiMIvVqY0Xb86_3Tn6NgVns8oxXOAb_1BXpR2PPt6wvVw"
        self.expiry_time = 0

    def get_token(self):
        if self.access_token and time.time() < self.expiry_time:
            logger.debug("Using cached access token")
            return self.access_token

        logger.info("Fetching new access token...")

        token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"

        data = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "resource": "https://graph.microsoft.com/"
        }

        response = requests.post(token_url, data=data).json()

        if "access_token" not in response:
            logger.error(f"Token fetch failed: {response}")
            raise Exception(f"Token Error: {response}")

        self.access_token = response["access_token"]
        self.expiry_time = time.time() + int(response["expires_in"]) - 60

        logger.info("Access token fetched successfully")

        return self.access_token

    def get_headers(self):
        return {
            "Authorization": f"Bearer {self.get_token()}"
        }
    # def get_headers(self):
    #     return {
    #         "Authorization": f"Bearer {self.access_token}"
    #     }

# =========================
# 📂 FETCH FILE FROM SHAREPOINT
# =========================
def fetch_excel_from_sharepoint(client):
    logger.info("Fetching Excel file from SharePoint...")

    file_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/items/{FILE_ID}/content"

    response = requests.get(file_url, headers=client.get_headers())

    if response.status_code != 200:
        logger.error(f"SharePoint Fetch Error: {response.text}")
        raise Exception(f"SharePoint Fetch Error: {response.text}")

    logger.info("File fetched successfully from SharePoint")

    df = pd.read_excel(io.BytesIO(response.content))
    logger.info(f"Loaded DataFrame with shape: {df.shape}")

    return df

# =========================
# 🔎 GOOGLE SEARCH CONSOLE SETUP
# =========================
def get_gsc_service():
    logger.info("Initializing Google Search Console service...")

    SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']

    credentials = service_account.Credentials.from_service_account_file(
        KEY_FILE, scopes=SCOPES
    )

    logger.info("GSC service initialized successfully")

    return build('searchconsole', 'v1', credentials=credentials)

# =========================
# 🔥 FETCH GSC DATA
# =========================
def fetch_data(service, start_date, end_date, dimensions):
    logger.info(f"Fetching GSC data for dimensions: {dimensions}")

    response = service.searchanalytics().query(
        siteUrl=SITE_URL,
        body={
            "startDate": str(start_date),
            "endDate": str(end_date),
            "dimensions": dimensions,
            "rowLimit": 25000
        }
    ).execute()

    rows = []
    for row in response.get("rows", []):
        record = {
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"]
        }

        for i, dim in enumerate(dimensions):
            record[dim] = row["keys"][i]

        rows.append(record)

    df = pd.DataFrame(rows)
    logger.info(f"GSC data fetched: {len(df)} rows for {dimensions}")

    return df

# =========================
# 🚀 MAIN EXECUTION
# =========================
def main():
    logger.info(" Starting SEO pipeline...")

    try:
        graph_client = GraphAPIClient()

        df_input = fetch_excel_from_sharepoint(graph_client)

        logger.info("Cleaning input data...")
        # df_input.columns = df_input.columns.str.strip()
        # df_input = df_input.dropna(subset=["Blog URL", "Primary keyword"])

        # Normalize column names
        df_input.columns = (
            df_input.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        logger.info(f"Normalized Columns: {list(df_input.columns)}")

        logger.info(f"Input rows after cleaning: {len(df_input)}")

        df_input["Blog URL"] = df_input["Blog URL"].str.strip().str.lower()

        df_input["Primary keyword"] = (
            df_input["Primary keyword"].fillna('') + ' ' +
            df_input["Secondary Keywords"].fillna('')
        ).str.strip().str.lower()

        logger.info("Fetching GSC service...")
        service = get_gsc_service()

        end_date = date.today() - timedelta(days=2)
        start_date = end_date - timedelta(days=7)

        df_query = fetch_data(service, start_date, end_date, ["query"])
        df_page = fetch_data(service, start_date, end_date, ["page"])
        df_country = fetch_data(service, start_date, end_date, ["country"])
        df_device = fetch_data(service, start_date, end_date, ["device"])
        df_query_page = fetch_data(service, start_date, end_date, ["query", "page"])

        # Normalize
        df_query["query"] = df_query["query"].str.lower()
        df_page["page"] = df_page["page"].str.lower()
        df_query_page["query"] = df_query_page["query"].str.lower()
        df_query_page["page"] = df_query_page["page"].str.lower()

        logger.info("Building summary...")
        summary_rows = []

        for _, row in df_input.iterrows():
            page = row["Blog URL"]
            primary = row["Primary keyword"]

            secondary_raw = str(row.get("Secondary Keywords", ""))
            secondary_list = [k.strip().lower() for k in secondary_raw.split(",") if k.strip()]

            all_keywords = [primary] + secondary_list

            matched = df_query_page[
                (df_query_page["page"] == page) &
                (df_query_page["query"].apply(lambda q: any(k in q for k in all_keywords)))
            ]

            if not matched.empty:
                best = matched.sort_values(by="clicks", ascending=False).iloc[0]

                summary_rows.append({
                    **row,
                    "Matched Query": best["query"],
                    "Clicks": best["clicks"],
                    "Impressions": best["impressions"],
                    "Position": best["position"]
                })
            else:
                summary_rows.append({
                    **row,
                    "Matched Query": None,
                    "Clicks": None,
                    "Impressions": None,
                    "Position": None
                })

        df_summary = pd.DataFrame(summary_rows)

        logger.info("Saving Excel output...")
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            df_summary.to_excel(writer, sheet_name="Summary", index=False)
            df_query.to_excel(writer, sheet_name="Query_Data", index=False)
            df_page.to_excel(writer, sheet_name="Page_Data", index=False)
            df_country.to_excel(writer, sheet_name="Country_Data", index=False)
            df_device.to_excel(writer, sheet_name="Device_Data", index=False)
            df_query_page.to_excel(writer, sheet_name="Query_Page", index=False)

        logger.info("SEO report generated successfully!")

    except Exception as e:
        logger.exception(f"❌ Pipeline failed: {str(e)}")

# =========================
# ▶️ RUN
# =========================
if __name__ == "__main__":
    main()