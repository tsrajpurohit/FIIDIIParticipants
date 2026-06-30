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


def save_to_csv(json_data, output_filename):
    if not json_data or "data" not in json_data:
        print("No valid data found to save.")
        return

    records = json_data["data"]
    df = pd.DataFrame(records)

    # Save DataFrame to CSV
    df.to_csv(output_filename, index=False)
    print(f"Successfully saved {len(df)} records to '{output_filename}'")


if __name__ == "__main__":
    # Fetch the dynamic data
    data = fetch_nse_data(API_URL)

    # Save it with a dynamically named file
    if data:
        filename = f"nse_pit_last_12M_{from_date_str}_to_{to_date_str}.csv"
        save_to_csv(data, filename)
