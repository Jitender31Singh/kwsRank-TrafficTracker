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
from openpyxl import load_workbook
import re

# =========================
# 📝 LOGGING CONFIG
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("app.log", mode="a", encoding="utf-8")
    ]
)

logger = logging.getLogger(__name__)

# =========================
# 🔐 ENV
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

LOCAL_FILE = "temp.xlsx"

COUNTRY_MAP = {
    "South Africa": "zaf",
    "Brazil": "bra",
    "Turkey": "tur",
    "Nigeria": "nga",
    "Kenya": "ken",
    "India": "ind",
}

# =========================
# 🔑 GRAPH CLIENT
# =========================
class GraphAPIClient:
    def __init__(self):
        self.access_token = None
        self.expiry_time = 0

    def get_token(self):
        if self.access_token and time.time() < self.expiry_time:
            logger.debug("Using cached token")
            return self.access_token

        logger.info("Fetching new Graph API token...")
        token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token"

        data = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "resource": "https://graph.microsoft.com/"
        }

        response = requests.post(token_url, data=data).json()

        if "access_token" not in response:
            raise Exception(response)

        self.access_token = response["access_token"]
        self.expiry_time = time.time() + int(response["expires_in"]) - 60

        logger.info("Token fetched successfully")
        return self.access_token

    def get_headers(self):
        return {"Authorization": f"Bearer {self.get_token()}"}

# =========================
# 📂 DOWNLOAD FULL FILE
# =========================
def download_file(client):
    logger.info("Downloading full Excel file from SharePoint...")

    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/items/{FILE_ID}/content"
    response = requests.get(url, headers=client.get_headers())

    if response.status_code != 200:
        raise Exception(response.text)

    with open(LOCAL_FILE, "wb") as f:
        f.write(response.content)

    logger.info("File downloaded successfully")

# =========================
# 📤 UPLOAD FILE
# =========================
def upload_file(client):
    logger.info("Uploading updated file to SharePoint...")

    url = f"https://graph.microsoft.com/v1.0/sites/{SITE_ID}/drives/{DRIVE_ID}/items/{FILE_ID}/content"

    with open(LOCAL_FILE, "rb") as f:
        data = f.read()

    headers = client.get_headers()
    headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    response = requests.put(url, headers=headers, data=data)

    if response.status_code not in [200, 201]:
        raise Exception(response.text)

    logger.info("Upload successful ✅")

# =========================
# 🔧 FIX MULTIINDEX
# =========================
def fix_multiindex_columns(df):
    new_cols = []
    last_main = None

    for main, sub in df.columns:
        main = str(main).strip()
        sub = str(sub).strip().lower()

        if "unnamed" in main.lower():
            main = last_main
        else:
            last_main = main

        if "unnamed" in sub:
            new_cols.append(main)
        else:
            new_cols.append(f"{main}_{sub}")

    df.columns = new_cols
    return df

# =========================
# 🔎 COLUMN FINDER
# =========================
def get_column(df, keyword):
    for col in df.columns:
        if keyword in str(col).lower():
            return col
    return None

# =========================
# 🔎 GSC
# =========================
def get_gsc_service():
    logger.info("Initializing GSC service...")
    credentials = service_account.Credentials.from_service_account_file(
        KEY_FILE,
        scopes=['https://www.googleapis.com/auth/webmasters.readonly']
    )
    return build('searchconsole', 'v1', credentials=credentials)

def fetch_keyword_data_by_country(service, site_url, start_date, end_date, country_code, row_limit=25000):
    """
    Fetch keyword-wise clicks and average position for a specific country from GSC.

    Args:
        service: Authorized Google Search Console service object
        site_url: GSC property URL (must exactly match verified property)
        start_date: datetime.date or string YYYY-MM-DD
        end_date: datetime.date or string YYYY-MM-DD
        country_code: ISO country code (e.g. 'ind', 'usa', 'gbr')
        row_limit: max rows per request (max 25000 per request)

    Returns:
        pandas.DataFrame with columns: query, clicks, impressions, ctr, position
    """

    logger.info(f"Fetching GSC keyword data from {start_date} to {end_date} for country={country_code}")

    all_rows = []
    start_row = 0

    while True:
        body = {
            "startDate": str(start_date),
            "endDate": str(end_date),
            "dimensions": ["query"],
            "rowLimit": row_limit,
            "startRow": start_row,
            "type": "web",
            "dimensionFilterGroups": [
                {
                    "filters": [
                        {
                            "dimension": "country",
                            "operator": "equals",
                            "expression": country_code.lower()
                        }
                    ]
                }
            ]
        }

        response = service.searchanalytics().query(
            siteUrl=site_url,
            body=body
        ).execute()

        rows = response.get("rows", [])
        if not rows:
            break

        for row in rows:
            all_rows.append({
                "query": row["keys"][0],
                "clicks": row.get("clicks", 0),
                # "impressions": row.get("impressions", 0),
                # "ctr": row.get("ctr", 0),
                "position": row.get("position", 0)
            })

        logger.info(f"Fetched {len(rows)} rows at startRow={start_row}")

        # If fewer than row_limit returned, no more pages
        if len(rows) < row_limit:
            break

        start_row += row_limit

    df = pd.DataFrame(all_rows)

    if not df.empty:
        df["query"] = df["query"].astype(str).str.lower().str.strip()

    logger.info(f"Total GSC keyword rows fetched: {len(df)}")
    logger.info(f"Columns returned: {df.columns.tolist()}")

    return df

def fetch_data(service, start_date, end_date, dimension):
    logger.info(f"Fetching GSC data from {start_date} to {end_date}")

    response = service.searchanalytics().query(
        siteUrl=SITE_URL,
        body={
            "startDate": str(start_date),
            "endDate": str(end_date),
            "dimensions": dimension,
            "rowLimit": 25000
        }
    ).execute()

    dim_name = dimension[0]  # "query" OR "page"

    rows = []
    for row in response.get("rows", []):
        rows.append({
            dim_name: row["keys"][0],
            "clicks": row["clicks"],
            "position": row["position"]
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df[dim_name] = df[dim_name].astype(str).str.lower().str.strip()

    logger.info(f"GSC rows fetched: {len(df)}")
    logger.info(f"Columns returned: {df.columns.tolist()}")

    return df

# =========================
# 🔄 HELPER METHOD FOR PAGES
# =========================

def build_page_df(df_page_sheet, df_gsc_page, end_date):
    # df_page_sheet = df_page_sheet.copy()
    # df_gsc_page = df_gsc_page.copy()
 
    # ==========================================
    # 1) Flatten old grouped-header columns
    # Example:
    # urls | 2026-03-20 00:00:00 | Unnamed: 2
    # =>
    # urls | 3/20/2026_Rank | 3/20/2026_Traffic
    # ==========================================
    cols = list(df_page_sheet.columns)
    new_cols = []
    i = 0
 
    while i < len(cols):
        col = str(cols[i]).strip()
 
        # Keep URL column as-is
        if col.lower() == "urls":
            new_cols.append("urls")
            i += 1
            continue
 
        # Old grouped-header style:
        # first col = date, second col = Unnamed => treat as Rank + Traffic
        if "Unnamed:" not in col:
            next_col = str(cols[i + 1]).strip() if i + 1 < len(cols) else None
 
            if next_col and "Unnamed:" in next_col:
                # Convert date-like header to m/d/YYYY format
                try:
                    dt = pd.to_datetime(col)
                    date_label = f"{dt.month}/{dt.day}/{dt.year}"
                except Exception:
                    # fallback if parsing fails
                    date_label = col.split(" ")[0]
 
                new_cols.append(f"{date_label}_Rank")
                new_cols.append(f"{date_label}_Traffic")
                i += 2
                continue
 
        # Already flat column -> keep as-is
        new_cols.append(col)
        i += 1
 
    # Apply flattened column names
    if len(new_cols) == len(df_page_sheet.columns):
        df_page_sheet.columns = new_cols
 
    # ==========================================
    # 2) Create normalized temp copy for URL matching only
    # ==========================================
    df_temp = df_page_sheet.copy()
    df_temp.columns = (
        df_temp.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    logger.info("tdf_columns : %s", df_temp.columns)
 
    # Find URL column safely
    url_col = "urls"
    if url_col not in df_temp.columns:
        raise KeyError(f"Expected column '{url_col}' in Page sheet. Found: {list(df_temp.columns)}")
 
    # Normalize URLs in page sheet
    df_temp[url_col] = df_temp[url_col].astype(str).str.lower().str.strip()
 
    # Normalize GSC page URLs
    df_gsc_page["page"] = df_gsc_page["page"].astype(str).str.lower().str.strip()
 
    # ==========================================
    # 3) Build page-level metrics from GSC
    # ==========================================
    page_metrics = (
        df_gsc_page.groupby("page", as_index=False)
        .agg(
            traffic=("clicks", "sum"),
            rank=("position", "min")
        )
    )
 
    metrics_lookup = page_metrics.set_index("page")
 
    # ==========================================
    # 4) New column names in SAME flat format
    # Example:
    # 3/30/2026_Rank
    # 3/30/2026_Traffic
    # ==========================================
    # date_label = f"{end_date.month}/{end_date.day}/{end_date.year}"
    rank_col = f"{end_date}_Rank"
    traffic_col = f"{end_date}_Traffic"
 
    # Optional: overwrite if already exists (safe rerun)
    df_page_sheet[rank_col] = df_temp[url_col].map(metrics_lookup["rank"])
    df_page_sheet[traffic_col] = df_temp[url_col].map(metrics_lookup["traffic"])
 
    return df_page_sheet 

# =========================
# 🔄 UPDATE ONE SHEET
# =========================
def update_kw_sheet(sheet_name, df_query, formatted_date):
    logger.info(f"Updating sheet: {sheet_name}")

    df = pd.read_excel(LOCAL_FILE, sheet_name=sheet_name, header=[0, 1])
    df = fix_multiindex_columns(df)

    primary_col = get_column(df, "primary")
    secondary_col = get_column(df, "secondary")

    rank_col = f"{formatted_date}_rank"
    traffic_col = f"{formatted_date}_traffic"

    if rank_col not in df.columns:
        df[rank_col] = None
    if traffic_col not in df.columns:
        df[traffic_col] = None

    for idx, row in df.iterrows():
        primary = row.get(primary_col, "")
        secondary = row.get(secondary_col, "")

        primary = "" if pd.isna(primary) else str(primary).lower()
        secondary = "" if pd.isna(secondary) else str(secondary).lower()

        keywords = [k.strip() for k in (primary + "," + secondary).split(",") if k.strip()]

        if not keywords:
            continue

        pattern = "|".join(map(re.escape, keywords))

        matched = df_query[df_query["query"].str.contains(pattern, na=False)]

        if not matched.empty:
            best = matched.sort_values("clicks", ascending=False).iloc[0]
            df.at[idx, rank_col] = best["position"]
            df.at[idx, traffic_col] = best["clicks"]

    # ✅ SAFE WRITE (modern pandas way)
    with pd.ExcelWriter(
        LOCAL_FILE,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace"
    ) as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    logger.info(f"Sheet '{sheet_name}' updated successfully")

def update_page_sheet(sheet_name, df_page, formatted_date):
    logger.info(f"Updating sheet: {sheet_name}")

    df = pd.read_excel(LOCAL_FILE, sheet_name=sheet_name, header=[0])
    df = build_page_df(df,df_page,formatted_date)
    
    with pd.ExcelWriter(
        LOCAL_FILE,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace"
    ) as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

    logger.info(f"Sheet '{sheet_name}' updated successfully")
    
def update_country_sheet(sheet_name, start_date, end_date, formatted_date):
    logger.info(f"Updating country sheet: {sheet_name}")

    country_code = COUNTRY_MAP.get(sheet_name)
    if not country_code:
        logger.warning(f"No country code mapping found for sheet '{sheet_name}'. Skipping.")
        return

    # 1) Read existing sheet
    df = pd.read_excel(LOCAL_FILE, sheet_name=sheet_name, header=0)

    keyword_col = "keywords"
    if keyword_col not in df.columns:
        logger.warning(
            f"'{keyword_col}' column not found in sheet '{sheet_name}'. "
            f"Found columns: {df.columns.tolist()}. Skipping."
        )
        return

    # Normalize sheet keywords
    df[keyword_col] = df[keyword_col].astype(str).str.lower().str.strip()

    # 2) Fetch GSC data for this country
    service = get_gsc_service()
    df_country = fetch_keyword_data_by_country(
        service,
        SITE_URL,
        start_date,
        end_date,
        country_code
    )

    # Dynamic output columns
    rank_col = f"{formatted_date}_rank"
    traffic_col = f"{formatted_date}_traffic"

    # Remove old columns for same date if already present (safe re-run)
    df = df.drop(columns=[rank_col, traffic_col], errors="ignore")

    # 3) If no GSC data, still add empty columns and write back
    if df_country.empty:
        logger.warning(f"No GSC data found for country '{sheet_name}' ({country_code})")

        df[rank_col] = None
        df[traffic_col] = None

        with pd.ExcelWriter(
            LOCAL_FILE,
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace"
        ) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)

        logger.info(f"Country sheet '{sheet_name}' updated with empty rank/traffic columns")
        return

    # Normalize GSC query
    df_country["query"] = df_country["query"].astype(str).str.lower().str.strip()

    # 4) Prepare GSC mapping dataframe
    df_country_map = df_country[["query", "position", "clicks"]].copy()
    df_country_map = df_country_map.rename(columns={
        "query": "keywords",
        "position": rank_col,
        "clicks": traffic_col
    })

    # If same keyword appears multiple times, keep the one with highest clicks
    df_country_map = (
        df_country_map.sort_values(traffic_col, ascending=False)
        .drop_duplicates(subset=["keywords"], keep="first")
    )

    # 5) Merge existing sheet with GSC data
    df_updated = df.merge(df_country_map, on="keywords", how="left")

    # 6) Write updated sheet back
    with pd.ExcelWriter(
        LOCAL_FILE,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="replace"
    ) as writer:
        df_updated.to_excel(writer, sheet_name=sheet_name, index=False)

    logger.info(f"Country sheet '{sheet_name}' updated successfully")
# =========================
# 🚀 MAIN
# =========================
def main():
    logger.info("Starting SEO pipeline...")

    try:
        client = GraphAPIClient()

        # Step 1: Download full file
        download_file(client)

        # workbook = load_workbook(LOCAL_FILE)

        # Step 2: GSC
        service = get_gsc_service()

        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=6)
        formatted_date = end_date.strftime("%d %B")

        df_query = fetch_data(service, start_date, end_date,["query"])
        df_page = fetch_data(service,start_date,end_date,["page"])

        logger.info("pages %s", df_page)

        # =========================
        # 🔥 MULTI-SHEET SUPPORT
        # =========================
        sheets_to_update = [
            "demoSheet",
            "Page sheet",
            # "India",
            "South Africa",
            # "Brazil",
            # "Turkey",
            # "Nigeria",
            # "Kenya",
        ]  # add more later

        for sheet in sheets_to_update:
            
            if sheet == 'demoSheet':
                update_kw_sheet( sheet, df_query, formatted_date)
            elif sheet == "Page sheet":
                update_page_sheet(sheet,df_page,formatted_date)
            else:
                update_country_sheet(sheet,start_date,end_date,formatted_date)

        # Step 3: Upload back
        # upload_file(client)

        logger.info("Pipeline completed successfully ✅")

    except Exception as e:
        logger.exception(f"Pipeline failed: {str(e)}")

if __name__ == "__main__":
    main()