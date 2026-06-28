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

# ================== EXTRACTION FUNCTION (AUC + NET) ==================
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
    
    columns_to_keep = [0, 1]
    final_cols = ["Sr_No", "Sector"]
    equity_count = 0
    total_count = 0

    # For Net Investment (latest period)
    net_columns_to_keep = [0, 1]
    net_final_cols = ["Sr_No", "Sector"]

    for col_idx in range(2, len(df.columns)):
        col_text = " ".join(header_rows[col_idx].dropna().astype(str).tolist()).lower()
      
        # AUC Processing
        if target_str in col_text and "inr" in col_text and "usd" not in col_text:
            columns_to_keep.append(col_idx)
            if "equity" in col_text:
                equity_count += 1
                final_cols.append("AUC_Equity_Cr" if equity_count == 1 else f"AUC_Equity_Cr_{equity_count}")
            elif "total" in col_text:
                total_count += 1
                final_cols.append("AUC_Total_Cr" if total_count == 1 else f"AUC_Total_Cr_{total_count}")
            else:
                final_cols.append(f"AUC_Col_{col_idx}")

        # Net Investment Processing (Stops extra duplicated columns from being added)
        if ("net investment" in col_text or "net inv" in col_text) and "inr" in col_text and "usd" not in col_text:
            if "equity" in col_text:
                if "Net_Equity_Cr" not in net_final_cols:
                    net_columns_to_keep.append(col_idx)
                    net_final_cols.append("Net_Equity_Cr")
            elif "total" in col_text:
                if "Net_Total_Cr" not in net_final_cols:
                    net_columns_to_keep.append(col_idx)
                    net_final_cols.append("Net_Total_Cr")

    # AUC DataFrame
    auc_df = data_rows[columns_to_keep].copy()
    auc_df.columns = final_cols[:len(auc_df.columns)]

    # Net Investment DataFrame
    net_df = data_rows[net_columns_to_keep].copy()
    net_df.columns = net_final_cols[:len(net_df.columns)]

    # Clean
    for d in [auc_df, net_df]:
        d = d[d["Sector"].notna()]
        d = d[~d["Sector"].str.contains("Sectors|Total|Grand Total", case=False, na=False)]
        d.reset_index(drop=True, inplace=True)

    auc_df["Report_Date"] = report_date.strftime("%Y-%m-%d")
    net_df["Report_Date"] = report_date.strftime("%Y-%m-%d")

    # Merge AUC + Net
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
    print("Starting FII AUC + Net Investment Downloader → Google Sheets...\n")
    report_dates = generate_dates_last_12_months()
  
    all_data = []
    base_url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_{}.html"
   
    for report_date in report_dates:
        month_str = get_nsdl_month_name(report_date)
        day_str = f"{report_date.day:02d}"
        filename = f"{month_str}{day_str}{report_date.year}"
        url = base_url.format(filename)
      
        print(f"Fetching: {report_date.strftime('%Y-%m-%d')} → {filename}")
        df = extract_latest_auc(url, report_date)
      
        if df is not None and not df.empty:
            all_data.append(df)
            print(f" ✓ Success: {len(df)} sectors")
        else:
            print(" ✗ Failed")
        time.sleep(1.2)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
      
        # Keep desired columns
        desired_cols = ["Report_Date", "Sector", "AUC_Equity_Cr", "AUC_Total_Cr",
                        "Net_Equity_Cr", "Net_Total_Cr"]
        final_df = final_df[[col for col in desired_cols if col in final_df.columns]]
      
        # Sort Newest to Oldest
        final_df["Report_Date"] = pd.to_datetime(final_df["Report_Date"])
        final_df = final_df.sort_values(by=["Report_Date", "Sector"], ascending=[False, True])
        final_df["Report_Date"] = final_df["Report_Date"].dt.strftime("%Y-%m-%d")
        final_df = final_df.reset_index(drop=True)

        # Save to Google Sheets
        try:
            sheet = client.open_by_key(SHEET_ID)
            worksheet = sheet.worksheet(TAB_NAME)
            worksheet.clear()
            worksheet.update([final_df.columns.values.tolist()] + final_df.values.tolist())
          
            print(f"\n✅ SUCCESS! Data uploaded to Google Sheet")
            print(f"Sheet ID: {SHEET_ID} | Tab: {TAB_NAME}")
            print(f"Total Rows: {len(final_df)}")
          
        except Exception as e:
            print(f"Google Sheets upload failed: {e}")
            final_df.to_csv("fii_auc_sector_last_12months.csv", index=False)
            print("Saved to CSV backup.")
    else:
        print("No data collected.")
