import json
import os
from datetime import datetime, timedelta
import pandas as pd
import requests
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"
TAB_NAME = "InsiderTrading"

# 1. Dynamically calculate dates for the last 12 months
today = datetime.now()
twelve_months_ago = today - timedelta(days=365)

from_date_str = twelve_months_ago.strftime("%d-%m-%Y")
to_date_str = today.strftime("%d-%m-%Y")

API_URL = f"https://www.nseindia.com/api/corporates-pit?index=equities&from_date={from_date_str}&to_date={to_date_str}"
BASE_URL = "https://www.nseindia.com"

print(f"Generated Dynamic URL: {API_URL}")

# CRITICAL: Enhanced headers to look like legitimate browser navigation on the exchange
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "keep-alive",
}


def fetch_nse_data(api_url):
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        # Step 1: Visit home page to generate required session cookies
        print("Visiting NSE home page for session cookies...")
        session.get(BASE_URL, timeout=15)

        # Step 2: Fetch data using the dynamic API URL
        print("Fetching PIT data for the last 12 months...")
        response = session.get(api_url, timeout=20)

        if response.status_code == 200:
            # Check if the response is actually JSON before parsing
            content_type = response.headers.get("Content-Type", "")
            if "application/json" in content_type:
                return response.json()
            else:
                print("\n[BLOCKED] NSE returned an HTML block page instead of data.")
                print("GitHub Actions cloud IP range is likely flagged by NSE's security firewall.")
                return None
        else:
            print(f"Failed. HTTP Status Code: {response.status_code}")
            return None

    except Exception as e:
        print(f"An error occurred during fetch: {e}")
        return None


def process_and_upload_to_gsheet(json_data):
    if not json_data or "data" not in json_data:
        print("No valid data found to save.")
        return

    records = json_data["data"]
    df = pd.DataFrame(records)

    # ------------------ FILTERING ROWS ------------------
    if "acqMode" in df.columns:
        df = df[df["acqMode"].isin(["Market Purchase", "Market Sale"])]

    if "personCategory" in df.columns:
        df = df[df["personCategory"].isin(["Promoters", "Promoter Group", "Director"])]

    if "secType" in df.columns:
        df = df[df["secType"].isin(["Equity Shares"])]

    # ------------------ REMOVING COLUMNS ------------------
    columns_to_drop = [
        "xbrlFileSize", "xbrl", "tkdAcqm", "tdpDerivativeContractType", 
        "sellquantity", "sellValue", "remarks", "pid", "exchange", 
        "did", "derivativeType", "buyValue", "buyQuantity", "anex"
    ]
    df = df.drop(columns=columns_to_drop, errors="ignore")
    df = df.fillna("")

    # =========================
    # GOOGLE SHEETS AUTH & UPLOAD
    # =========================
    print("Connecting to Google Sheets...")
    credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
    if not credentials_json:
        raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable not set!")

    try:
        creds = Credentials.from_service_account_info(
            json.loads(credentials_json),
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        client = gspread.authorize(creds)
        
        spreadsheet = client.open_by_key(SHEET_ID)
        sheet = spreadsheet.worksheet(TAB_NAME)
        
        sheet.clear()
        data_to_upload = [df.columns.values.tolist()] + df.values.tolist()
        
        print(f"Uploading {len(df)} filtered records to sheet tab '{TAB_NAME}'...")
        sheet.update('A1', data_to_upload)
        print("Successfully uploaded data to Google Sheets!")

    except Exception as e:
        print(f"Failed to complete Google Sheet operation: {e}")


if __name__ == "__main__":
    data = fetch_nse_data(API_URL)
    if data:
        process_and_upload_to_gsheet(data)
