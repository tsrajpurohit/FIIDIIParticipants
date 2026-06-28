import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import json
import os
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

# =========================
# CONFIG
# =========================
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"
TAB_NAME = "FPI_Sectors"

BASE_URL = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120 Safari/537.36"
}

# =========================
# GOOGLE SHEETS AUTH
# =========================
credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")

if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS not set")

creds = Credentials.from_service_account_info(
    json.loads(credentials_json),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

client = gspread.authorize(creds)

# =========================
# DATE GENERATION
# =========================
def generate_dates():
    dates = []
    now = datetime.now()

    for i in range(370):
        d = now - timedelta(days=i)

        is_month_end = (d + timedelta(days=1)).day == 1

        if d.day == 15:
            dates.append(d.strftime("%b15%Y"))
        elif is_month_end:
            dates.append(d.strftime(f"%b{d.day}%Y"))

    return list(dict.fromkeys(dates))[:24]

# =========================
# SAFE REQUEST (IMPORTANT FIX)
# =========================
def safe_get(url):
    for attempt in range(3):
        try:
            res = requests.get(url, headers=HEADERS, timeout=15)

            if res.status_code == 200:
                return res

        except Exception as e:
            print(f"Retry {attempt+1}: {e}")
            time.sleep(random.uniform(3, 6))

    return None

# =========================
# SCRAPE DATA
# =========================
def fetch_data():
    dates = generate_dates()
    all_data = []

    print(f"Total periods: {len(dates)}")

    for token in dates:
        url = f"{BASE_URL}{token}.html"
        print(f"Fetching {token}")

        response = safe_get(url)
        if not response:
            print(f"Skipped {token}")
            continue

        try:
            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table")

            if not table:
                continue

            rows = table.find_all("tr")

            for r in rows:
                cols = [c.text.strip().replace(",", "") for c in r.find_all("td")]

                # Ensure it's a valid data row (starts with a sector number)
                if len(cols) > 30 and cols[0].isdigit():
                    try:
                        # NSDL Layout structure from the end of the row (Latest Date):
                        # Block 4 (Latest AUC): Last 24 columns -> 12 USD Mn columns, preceded by 12 INR Cr columns
                        # Block 3 (Latest Net Investment Fortnight): Previous 24 columns -> 12 USD Mn, preceded by 12 INR Cr
                        
                        # Target columns for the requested date:
                        auc_cr_total = cols[-13]       # Total AUC in INR Cr for the current date
                        net_flow_cr_total = cols[-37]  # Total Net Investment in INR Cr for the current fortnight
                        
                        all_data.append({
                            "Report_Date": token,
                            "Sector": cols[1],
                            "AUC_Cr": float(auc_cr_total) if auc_cr_total.replace("-", "", 1).replace(".", "", 1).isdigit() else 0.0,
                            "Net_Flow_Cr": float(net_flow_cr_total) if net_flow_cr_total.replace("-", "", 1).replace(".", "", 1).isdigit() else 0.0
                        })
                    except Exception as e:
                        print(f"Error processing row in {token}: {e}")
                        pass

        except Exception as e:
            print(f"Error parsing {token}: {e}")

        # Anti-block delay
        time.sleep(random.uniform(3, 5))

    df = pd.DataFrame(all_data)
    return df

# =========================
# CLEAN DATA
# =========================
def clean_df(df):
    df = df.copy()

    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x))

    return df.fillna("")

# =========================
# UPLOAD TO GOOGLE SHEETS
# =========================
def upload(df):
    sheet = client.open_by_key(SHEET_ID)

    try:
        ws = sheet.worksheet(TAB_NAME)
        ws.clear()
    except:
        ws = sheet.add_worksheet(TAB_NAME, rows="5000", cols="20")

    df = clean_df(df)

    values = [df.columns.tolist()] + df.values.tolist()

    ws.update("A1", values, value_input_option="RAW")

    print("Uploaded to Google Sheets")

# =========================
# MAIN
# =========================
def main():
    df = fetch_data()

    if df.empty:
        print("No data fetched")
        return

    print(f"Total rows: {len(df)}")

    df.to_csv("FPI_Sectors.csv", index=False)
    print("CSV saved")

    upload(df)

    print("DONE")

if __name__ == "__main__":
    main()
