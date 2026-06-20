import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import json
import os
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"
TAB_NAME = "FPI_Sectors"

BASE_URL = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# =========================
# GOOGLE SHEETS AUTH
# =========================
credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")

creds = Credentials.from_service_account_info(
    json.loads(credentials_json),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

client = gspread.authorize(creds)

# =========================
# DATE GENERATION
# =========================
def generate_nsdl_date_strings():
    date_strings = []
    current_date = datetime.now()

    for i in range(370):
        d = current_date - timedelta(days=i)
        day = d.day

        is_eom = (d + timedelta(days=1)).day == 1

        if day == 15:
            date_strings.append(d.strftime("%b15%Y"))
        elif is_eom:
            date_strings.append(d.strftime(f"%b{day}%Y"))

    return list(dict.fromkeys(date_strings))[:24]

# =========================
# SCRAPING
# =========================
def download_fpi_data():
    dates = generate_nsdl_date_strings()
    frames = []

    for token in dates:
        url = f"{BASE_URL}{token}.html"

        try:
            res = requests.get(url, headers=HEADERS, timeout=10)

            if res.status_code != 200:
                continue

            soup = BeautifulSoup(res.text, "html.parser")
            table = soup.find("table")

            if not table:
                continue

            rows = table.find_all("tr")

            data = []
            for tr in rows:
                cols = [c.text.strip().replace(",", "") for c in tr.find_all("td")]

                if len(cols) >= 3 and cols[0].isdigit():
                    try:
                        data.append({
                            "Report_Date": token,
                            "Sector": cols[1],
                            "AUC_Cr": float(cols[2]) if cols[2].replace(".", "", 1).isdigit() else 0,
                            "Net_Flow_Cr": float(cols[-1]) if cols[-1].replace(".", "", 1).isdigit() else 0
                        })
                    except:
                        pass

            if data:
                df = pd.DataFrame(data)
                frames.append(df)
                print(f"Downloaded {token}: {len(df)} rows")

            time.sleep(1)

        except Exception as e:
            print(f"Error {token}: {e}")

    if frames:
        return pd.concat(frames, ignore_index=True)

    return pd.DataFrame()

# =========================
# CLEAN DATA
# =========================
def clean_df(df):
    df = df.copy()
    df = df.fillna("")

    # convert everything safe for Google Sheets
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x))

    return df

# =========================
# UPLOAD TO SHEETS
# =========================
def upload_to_gsheet(df):
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
    df = download_fpi_data()

    if df.empty:
        print("No data found")
        return

    df.to_csv("fpi_sectors.csv", index=False)
    print("CSV saved")

    upload_to_gsheet(df)

    print("DONE")

if __name__ == "__main__":
    main()
