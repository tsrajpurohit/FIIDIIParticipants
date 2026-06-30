import json
from datetime import datetime, timedelta
import pandas as pd
import requests

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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
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


def save_to_csv(json_data, output_filename):
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
    # errors='ignore' ensures the code won't break if any column is missing from the API
    df = df.drop(columns=columns_to_drop, errors="ignore")

    # Save the filtered DataFrame to CSV
    df.to_csv(output_filename, index=False)
    print(f"Successfully filtered and saved {len(df)} records to '{output_filename}'")


if __name__ == "__main__":
    # Fetch the dynamic data
    data = fetch_nse_data(API_URL)

    # Save it with a dynamically named file
    if data:
        filename = f"nse_pit_last_12M_Filtered.csv"
        save_to_csv(data, filename)
