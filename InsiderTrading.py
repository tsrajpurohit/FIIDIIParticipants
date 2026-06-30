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

# Format dates to DD-MM-YYYY as required by NSE API
from_date_str = twelve_months_ago.strftime("%d-%m-%Y")
to_date_str = today.strftime("%d-%m-%Y")

# Construct the dynamic API URL
API_URL = f"https://www.nseindia.com/api/corporates-pit?index=equities&from_date={from_date_str}&to_date={to_date_str}"
BASE_URL = "https://www.nseindia.com"

print(f"Generated Dynamic URL: {API_URL}")

# Standard headers to simulate a browser session
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Origin": "https://www.nseindia.com",
    "Connection": "keep-alive",
}


def fetch_nse_data(api_url):
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        # Step 1: Visit home page to generate required session cookies
        print("Visiting NSE home page for session cookies...")
        session.get(BASE_URL, timeout=10)

        # Step 2: Fetch data using the dynamic API URL
        print("Fetching PIT data for the last 12 months...")
        response = session.get(api_url, timeout=15)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed. HTTP Status Code: {response.status_code}")
            return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def process_and_upload_to_gsheet(json_data):
    if not json_data or "data" not in json_data:
        print("No valid data found to save.")
        return

    records = json_data["data"]
    df = pd.DataFrame(records)

    # ------------------ FILTERING ROWS ------------------
    # 1. Keep only "Market Purchase" and "Market Sale"
    if "acqMode" in df.columns:
        df = df[df["acqMode"].isin(["Market Purchase", "Market Sale"])]

    # 2. Keep only specified promoter/director categories
    if "personCategory" in df.columns:
        df = df[df["personCategory"].isin(["Promoters", "Promoter Group", "Director"])]

    # 3. Keep only "Equity Shares"
    if "secType" in df.columns:
        df = df[df["secType"].isin(["Equity Shares"])]

    # ------------------ REMOVING COLUMNS ------------------
    columns_to_drop = [
        "xbrlFileSize", "xbrl", "tkdAcqm", "tdpDerivativeContractType", 
        "sellquantity", "sellValue", "remarks", "pid", "exchange", 
        "did", "derivativeType", "buyValue", "buyQuantity", "anex"
    ]
    df = df.drop(columns=columns_to_drop, errors="ignore")

    # Replace NaN/Null values with empty strings for safe JSON serialization
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
        
        # Open using the Spreadsheet ID and specific Tab Name
        spreadsheet = client.open_by_key(SHEET_ID)
        sheet = spreadsheet.worksheet(TAB_NAME)
        
        # Clear previous data before refreshing
        sheet.clear()
        
        # Convert DataFrame to list format (headers + row values)
        data_to_upload = [df.columns.values.tolist()] + df.values.tolist()
        
        print(f"Uploading {len(df)} filtered records to sheet tab '{TAB_NAME}'...")
        sheet.update('A1', data_to_upload)
        print("Successfully uploaded data to Google Sheets!")

    except Exception as e:
        print(f"Failed to complete Google Sheet operation: {e}")


if __name__ == "__main__":
    # Fetch the dynamic data
    data = fetch_nse_data(API_URL)

    # Process filters and push to Google Sheets
    if data:
        process_and_upload_to_gsheet(data)
