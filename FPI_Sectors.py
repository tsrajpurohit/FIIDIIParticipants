import io
import json
import os
import random
import time
from datetime import datetime, timedelta
import gspread
import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

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
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
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
# SAFE REQUEST
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
# SCRAPE & PARSE DATA
# =========================
def fetch_data():
    dates = generate_dates()
    compiled_dfs = []

    print(f"Total periods to process: {len(dates)}")

    for token in dates:
        url = f"{BASE_URL}{token}.html"
        print(f"Fetching {token}...")

        response = safe_get(url)
        if not response:
            print(f"Skipped {token}")
            continue

        try:
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table")
            if not table:
                continue

            # Parse structural raw table
            html_stream = io.StringIO(str(table))
            df = pd.read_html(html_stream, header=None)[0]

            # Find data starting boundary
            data_start_idx = None
            for idx, row in df.iterrows():
                val = str(row.iloc[0]).strip()
                if val in ["1", "1.0"]:
                    data_start_idx = idx
                    break

            if data_start_idx is None:
                continue

            data_rows = df.iloc[data_start_idx:].copy()

            # =========================================================================
            # TAIL-SLICING POSITION EXTRACTOR (TARGETS NEWEST AUC)
            # =========================================================================
            # Total columns variant structure checking
            total_cols = len(df.columns)

            # NSDL structure layouts are wide. The final block spans the last 24 columns:
            # - First 12 columns: Asset breakdown fields calculated in INR Crore.
            # - Last 12 columns: Duplicate asset fields configured in USD Million.
            # By slicing from (total_cols - 24) to (total_cols - 12), we lock onto only INR Crore.
            columns_to_keep = [1] + list(range(total_cols - 24, total_cols - 12))

            # Base normalized metrics mapping names for target column layout alignment
            final_cols = [
                "Sector",
                "AUC_Equity_Cr",
                "AUC_Debt_General_Cr",
                "AUC_Debt_VRR_Cr",
                "AUC_Debt_FAR_Cr",
                "AUC_Hybrid_Cr",
                "AUC_Debt_Solution_Oriented_Cr",
                "AUC_Debt_Other_Cr",
                "AUC_AIF_Cr",
                "AUC_Total_Cr",
            ]

            # Slice localized target segments from index data block
            processed_df = data_rows[columns_to_keep].copy()

            # Assign labels gracefully or use safety index fallbacks if structure shifts
            if len(processed_df.columns) == len(final_cols):
                processed_df.columns = final_cols
            else:
                generated_cols = ["Sector"] + [
                    f"AUC_Metric_Col_{i}"
                    for i in range(len(processed_df.columns) - 1)
                ]
                processed_df.columns = generated_cols

            # Clean rows structural metadata markers
            processed_df = processed_df[processed_df["Sector"].notna()]
            processed_df = processed_df[
                ~processed_df["Sector"].str.contains(
                    "Sectors|Total|Grand Total", case=False, na=False
                )
            ]

            # Inject the standard historical meta tracking date key
            processed_df.insert(0, "Report_Date", token)

            # Clear individual row index collisions entirely
            processed_df.reset_index(drop=True, inplace=True)

            compiled_dfs.append(processed_df)

        except Exception as e:
            print(f"Error parsing date token {token}: {e}")

        # Throttling API limit defense cooldown
        time.sleep(random.uniform(3, 5))

    if compiled_dfs:
        # Concatenate completely unique schemas safely
        return pd.concat(compiled_dfs, ignore_index=True)
    return pd.DataFrame()


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
    except Exception:
        ws = sheet.add_worksheet(TAB_NAME, rows="10000", cols="15")

    df = clean_df(df)
    values = [df.columns.tolist()] + df.values.tolist()
    ws.update("A1", values, value_input_option="RAW")
    print("Uploaded to Google Sheets successfully.")


# =========================
# MAIN EXECUTION ROUTINE
# =========================
def main():
    df = fetch_data()

    if df.empty:
        print("No data fetched.")
        return

    print(f"Total rows aggregated: {len(df)}")
    df.to_csv("FPI_Sectors.csv", index=False)
    print("CSV saved local cache.")

    upload(df)
    print("All tasks completed successfully.")


if __name__ == "__main__":
    main()
