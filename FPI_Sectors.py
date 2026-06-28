import io
import json
import os
import random
import re
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

            header_rows = df.iloc[:data_start_idx]
            data_rows = df.iloc[data_start_idx:].copy()

            # =========================================================================
            # STRICT URL-TO-HEADER MATCHING LOGIC
            # =========================================================================
            # 1. Parse date directly from token (e.g., 'June152026' or 'Jun152026')
            # Handle both long and short month formats gracefully
            match = re.search(r"([A-Za-z]+)(\d{2})(\d{4})", token)
            if not match:
                print(f"Could not parse date token layout for {token}")
                continue
                
            month_str, day_str, year_str = match.groups()
            
            # Clean up known discrepancies (like turning 'June' -> 'Jun' or 'Sept' -> 'Sep')
            cleaned_month = month_str[:3] 
            
            # Form standard target search structures
            # Variant A: "auc as on jun 15, 2026"
            target_auc_phrase_short = f"auc as on {cleaned_month} {day_str}, {year_str}".lower()
            # Variant B: "auc as on june 15, 2026"
            target_auc_phrase_long = f"auc as on {month_str} {day_str}, {year_str}".lower()

            columns_to_keep = [1]  # Sector name column is always index 1
            final_cols = ["Sector"]

            # 2. Extract only columns matching our specific calculated phrases
            for col_idx in range(2, len(df.columns)):
                # Flatten the specific column's multi-row headers into one string
                col_headers_text = " ".join(header_rows[col_idx].dropna().astype(str).tolist()).lower()
                
                # Verify that it matches our target date string, and exclude USD data columns
                is_target_date = (target_auc_phrase_short in col_headers_text) or (target_auc_phrase_long in col_headers_text)
                
                if is_target_date and "usd" not in col_headers_text:
                    columns_to_keep.append(col_idx)
                    
                    # Dynamically name the column based on specific asset sub-headers
                    if "equity" in col_headers_text:
                        final_cols.append("AUC_Equity_Cr")
                    elif "debt general" in col_headers_text:
                        final_cols.append("AUC_Debt_General_Cr")
                    elif "debt vrr" in col_headers_text:
                        final_cols.append("AUC_Debt_VRR_Cr")
                    elif "debt-far" in col_headers_text or "debt far" in col_headers_text:
                        final_cols.append("AUC_Debt_FAR_Cr")
                    elif "hybrid" in col_headers_text:
                        final_cols.append("AUC_Hybrid_Cr")
                    elif "solution oriented" in col_headers_text:
                        final_cols.append("AUC_Debt_Solution_Oriented_Cr")
                    elif "debt other" in col_headers_text:
                        final_cols.append("AUC_Debt_Other_Cr")
                    elif "aif" in col_headers_text:
                        final_cols.append("AUC_AIF_Cr")
                    elif "total" in col_headers_text:
                        final_cols.append("AUC_Total_Cr")
                    else:
                        final_cols.append(f"AUC_Metric_Col_{col_idx}_Cr")

            # Validate that columns matching our calculated criteria were found
            if len(columns_to_keep) <= 1:
                print(f"Warning: No matching columns found for target criteria: '{target_auc_phrase_short}' on {token}")
                continue

            # Slice out the target data columns
            processed_df = data_rows[columns_to_keep].copy()
            processed_df.columns = final_cols

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
