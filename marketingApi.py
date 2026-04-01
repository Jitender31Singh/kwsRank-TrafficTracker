import pandas as pd
from datetime import date, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

# CONFIG
SITE_URL = "https://piegaming.com/"
KEY_FILE = "electric-signal-491806-e4-6927fa72b120.json"

INPUT_FILE = "keywordsTracker.csv"
OUTPUT_FILE = "output.xlsx"

SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']

# AUTH
credentials = service_account.Credentials.from_service_account_file(
    KEY_FILE, scopes=SCOPES
)
service = build('searchconsole', 'v1', credentials=credentials)

# DATE RANGE
end_date = date.today() - timedelta(days=2)
start_date = end_date - timedelta(days=7)

# LOAD INPUT
df_input = pd.read_csv(INPUT_FILE)
df_input.columns = df_input.columns.str.strip()
df_input = df_input.dropna(subset=["Blog URL", "Primary keyword"])

df_input["Blog URL"] = df_input["Blog URL"].str.strip().str.lower()
# df_input["Primary keyword"] = df_input["Primary keyword","Secondary keyword"].str.strip().str.lower()
df_input["Primary keyword"] = (
    df_input["Primary keyword"].fillna('') + ' ' +
    df_input["Secondary Keywords"].fillna('')
).str.strip().str.lower()

# 🔥 FUNCTION TO FETCH DATA
def fetch_data(dimensions):
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

    return pd.DataFrame(rows)


# 🔹 FETCH DIFFERENT VIEWS
df_query = fetch_data(["query"])
df_page = fetch_data(["page"])
df_country = fetch_data(["country"])
df_device = fetch_data(["device"])
df_query_page = fetch_data(["query", "page"])

# NORMALIZE
df_query["query"] = df_query["query"].str.lower()
df_page["page"] = df_page["page"].str.lower()
df_query_page["query"] = df_query_page["query"].str.lower()
df_query_page["page"] = df_query_page["page"].str.lower()

# 🔹 SUMMARY BUILD
summary_rows = []

for _, row in df_input.iterrows():
    # page = row["Blog URL"]
    primary = row["Primary keyword"]

    secondary_raw = str(row.get("Secondary Keywords", ""))
    secondary_list = [k.strip().lower() for k in secondary_raw.split(",") if k.strip()]

    all_keywords = [primary] + secondary_list

    # matched = df_query_page[
    #     # (df_query_page["page"] == page) &
    #     (df_query["query"].apply(lambda q: any(k in q for k in all_keywords)))
    # ]
    matched = df_query[
        (df_query["query"].apply(lambda q: any(k in q for k in all_keywords)))
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

# 🔹 WRITE MULTI-SHEET EXCEL
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    df_summary.to_excel(writer, sheet_name="Summary", index=False)
    df_query.to_excel(writer, sheet_name="Query_Data", index=False)
    df_page.to_excel(writer, sheet_name="Page_Data", index=False)
    df_country.to_excel(writer, sheet_name="Country_Data", index=False)
    df_device.to_excel(writer, sheet_name="Device_Data", index=False)
    df_query_page.to_excel(writer, sheet_name="Query_Page", index=False)

print("✅ Full multi-dimensional SEO report created!")