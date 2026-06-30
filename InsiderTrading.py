import json
import os
import sys
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

print(f"[DEBUG] System Time: {today.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"[DEBUG] Generated Dynamic URL: {API_URL}")

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
        print("[DEBUG] Step 1: Visiting NSE home page for session cookies...")
        homepage_res = session.get(BASE_URL, timeout=15)
        print(f"[DEBUG] Homepage Response Status: {homepage_res.status_code}")
        print(f"[DEBUG] Cookies acquired: {session.cookies.get_dict()}")

        # Step 2: Fetch data using the dynamic API URL
        print("[DEBUG] Step 2: Fetching PIT data from API...")
        response = session.get(api_url, timeout=20)
        
        print(f"[DEBUG] API Response Status Code: {response.status_code}")
        print(f"[DEBUG] API Response Headers: {dict(response.headers)}")

        # Check if response status is okay
        if response.status_code == 200:
            content_type = response.headers.get("Content-Type", "")
            if "application/json" in content_type:
                print("[DEBUG] Success: Received JSON data from NSE.")
                return response.json()
            else:
                print("\n[CRITICAL ERROR] NSE returned 200 OK but it is HTML/Text, NOT JSON!")
                print(f"[DEBUG] Actual Content-Type received: {content_type}")
                print("-" * 50)
                print(f"[DEBUG] First 500 characters of response body:\n{response.text[:500]}")
                print("-" * 50)
                return None
        else:
            print(f"\n[CRITICAL ERROR] API Request failed with status code {response.status_code}")
            print("-" * 50)
            print(f"[DEBUG] First 500 characters of failure body:\n{response.text[:500]}")
            print("-" * 50)
            return None

    except Exception as e:
        print(f"\n[CRITICAL ERROR] Exception occurred during HTTP request pipeline: {str(e)}")
        import traceback
        traceback.print_exc()
        return None


def process_and_upload_to_gsheet(json_data):
    if not json_data or "data" not in json_data:
        print("[DEBUG] No valid 'data' array inside JSON object to extract.")
        return

    records = json_data["data"]
    df = pd.DataFrame(records)
    print(f"[DEBUG] Total raw records parsed into DataFrame: {len(df)}")

    # ------------------ FILTERING ROWS ------------------
    if "acqMode" in df.columns:
        df = df[df["acqMode"].isin(["Market Purchase", "Market Sale"])]
    if "personCategory" in df.columns:
        df = df[df["personCategory"].isin(["Promoters", "Promoter Group", "Director"])]
    if "secType" in df.columns:
        df = df[df["secType"].isin(["Equity Shares"])]
    
    print(f"[DEBUG] Records remaining after Row Filtering: {len(df)}")

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
    print("[DEBUG] Initializing Google Sheets connection...")
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
        
        print(f"[DEBUG] Target sheet opened. Clearing tab: '{TAB_NAME}'...")
        sheet.clear()
        
        data_to_upload = [df.columns.values.tolist()] + df.values.tolist()
        
        print(f"[DEBUG] Uploading compiled list block to cell A1...")
        sheet.update('A1', data_to_upload)
        print("[DEBUG] Google Sheets upload routine finished successfully!")

    except Exception as e:
        print(f"[CRITICAL ERROR] Google Sheets operation failed: {e}")


if __name__ == "__main__":
    data = fetch_nse_data(API_URL)
    if data:
        process_and_upload_to_gsheet(data)
    else:
        print("[DEBUG] Exiting script safely because fetch execution yielded no data.")
        sys.exit(1)
