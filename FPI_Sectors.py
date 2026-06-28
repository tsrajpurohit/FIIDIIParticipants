import io
import json
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import calendar
import time
import gspread
from google.oauth2.service_account import Credentials

# =========================
# CONFIG
# =========================
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"
TAB_NAME = "FPI_Sectors"

# =========================
# GOOGLE SHEETS AUTH
# =========================
credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable not set!")

creds = Credentials.from_service_account_info(
    json.loads(credentials_json),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(creds)

# ================== DEBUG EXTRACTION FUNCTION ==================
def extract_latest_auc(url, report_date):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table")
    if not table:
        print(f"No table found on {url}")
        return None

    html_stream = io.StringIO(str(table))
    df = pd.read_html(html_stream, header=None)[0]

    data_start_idx = None
    for idx, row in df.iterrows():
        val = str(row.iloc[0]).strip()
        if val in ["1", "1.0"]:
            data_start_idx = idx
            break

    if data_start_idx is None:
        print("Could not find data rows")
        return None

    header_rows = df.iloc[:data_start_idx]
    data_rows = df.iloc[data_start_idx:].copy()

    target_str = f"AUC as on {report_date.strftime('%B %d, %Y')}".lower()
    print(f"Target AUC: {target_str}")

    auc_keep = [0, 1]
    net_keep = [0, 1]

    for col_idx in range(2, len(df.columns)):
        col_text = " ".join(header_rows[col_idx].dropna().astype(str).tolist()).lower()
        print(f"Col {col_idx}: {col_text[:100]}...")  # DEBUG

        if target_str in col_text and "inr" in col_text and "usd" not in col_text:
            auc_keep.append(col_idx)
            print(f"  → AUC column found at {col_idx}")
        
        if ("net investment" in col_text or "net inv" in col_text) and "inr" in col_text and "usd" not in col_text:
            net_keep.append(col_idx)
            print(f"  → Net column found at {col_idx}")

    print(f"AUC columns count: {len(auc_keep)}")
    print(f"Net columns count: {len(net_keep)}")

    # SAFE CREATION
    auc_df = data_rows.iloc[:, :len(auc_keep)].copy()
    auc_names = ["Sr_No", "Sector"]
    if len(auc_keep) > 2:
        auc_names.append("AUC_Equity_Cr")
    if len(auc_keep) > 3:
        auc_names.append("AUC_Total_Cr")
    auc_df.columns = auc_names[:len(auc_df.columns)]

    net_df = data_rows.iloc[:, :len(net_keep)].copy()
    net_names = ["Sr_No", "Sector"]
    if len(net_keep) > 2:
        net_names.append("Net_Equity_Cr")
    if len(net_keep) > 3:
        net_names.append("Net_Total_Cr")
    net_df.columns = net_names[:len(net_df.columns)]

    # Clean
    for d in [auc_df, net_df]:
        d = d[d["Sector"].notna()]
        d = d[~d["Sector"].str.contains("Sectors|Total|Grand Total", case=False, na=False)]
        d.reset_index(drop=True, inplace=True)

    auc_df["Report_Date"] = report_date.strftime("%Y-%m-%d")
    net_df["Report_Date"] = report_date.strftime("%Y-%m-%d")

    final_df = pd.merge(auc_df, net_df.drop(columns=['Sr_No'], errors='ignore'), 
                       on=['Report_Date', 'Sector'], how='left')

    return final_df


# ================== HELPERS ==================
def get_nsdl_month_name(dt):
    full = dt.strftime("%B")
    return full if full in ["June", "July"] else dt.strftime("%b")


def generate_dates_last_12_months():
    dates = []
    today = datetime.now()
    current = today.replace(day=1)
    for _ in range(14):
        dates.append(current.replace(day=15))
        last_day = calendar.monthrange(current.year, current.month)[1]
        dates.append(current.replace(day=last_day))
       
        if current.month == 1:
            current = current.replace(year=current.year-1, month=12, day=1)
        else:
            current = current.replace(month=current.month-1, day=1)
    return sorted(set(dates), reverse=True)[:26]


# ================== MAIN ==================
if __name__ == "__main__":
    print("Starting FII AUC + Net Investment Downloader...\n")
    report_dates = generate_dates_last_12_months()
   
    all_data = []
    base_url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_{}.html"
    
    for report_date in report_dates[:3]:  # Test only 3 dates
        month_str = get_nsdl_month_name(report_date)
        day_str = f"{report_date.day:02d}"
        filename = f"{month_str}{day_str}{report_date.year}"
        url = base_url.format(filename)
       
        print(f"\nFetching: {report_date.strftime('%Y-%m-%d')} → {filename}")
        df = extract_latest_auc(url, report_date)
       
        if df is not None and not df.empty:
            all_data.append(df)
            print(f" ✓ Success: {len(df)} sectors")
        else:
            print(" ✗ Failed")
        time.sleep(1.2)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        print("\nFinal Columns:", list(final_df.columns))
        print(final_df.head(5))
    else:
        print("No data collected.")
