import io
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import calendar
import time
import gspread
from google.oauth2.service_account import Credentials
import json
import os

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

# ================== ROBUST EXTRACTION ==================
def extract_fpi_data(url, report_date):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed {url}: {e}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table")
    if not table:
        print("No table found")
        return None

    df = pd.read_html(io.StringIO(str(table)), header=None)[0]
    
    # Find data start
    data_start = None
    for i in range(len(df)):
        if str(df.iloc[i,0]).strip() in ["1", "1.0"]:
            data_start = i
            break
    if data_start is None:
        print("Data start not found")
        return None

    header_part = df.iloc[:data_start]
    data_part = df.iloc[data_start:].copy()

    target_date = report_date.strftime("%B %d, %Y").lower()

    auc_cols = [0, 1]
    auc_names = ["Sr_No", "Sector"]
    net_cols = [0, 1]
    net_names = ["Sr_No", "Sector"]

    # Scan columns for latest AUC and latest Net
    for c in range(2, df.shape[1]):
        col_text = " ".join(header_part[c].dropna().astype(str)).lower()
        
        # Latest AUC
        if target_date in col_text and "inr" in col_text and "usd" not in col_text:
            auc_cols.append(c)
            if "equity" in col_text:
                auc_names.append("AUC_Equity_Cr")
            elif "total" in col_text:
                auc_names.append("AUC_Total_Cr")
        
        # Latest Net Investment (the one just before AUC or the rightmost net)
        if ("net investment" in col_text or "net inv" in col_text) and "inr" in col_text and "usd" not in col_text:
            net_cols.append(c)
            if "equity" in col_text:
                net_names.append("Net_Equity_Cr")
            elif "total" in col_text:
                net_names.append("Net_Total_Cr")

    # Create DataFrames safely
    auc_df = data_part.iloc[:, :len(auc_cols)].copy()
    auc_df.columns = auc_names[:len(auc_df.columns)]

    net_df = data_part.iloc[:, :len(net_cols)].copy()
    net_df.columns = net_names[:len(net_df.columns)]

    # Clean
    for d in [auc_df, net_df]:
        d = d[d["Sector"].notna()]
        d = d[~d["Sector"].str.contains("Sectors|Total|Grand Total", case=False, na=False)]
        d.reset_index(drop=True, inplace=True)

    auc_df["Report_Date"] = report_date.strftime("%Y-%m-%d")
    net_df["Report_Date"] = report_date.strftime("%Y-%m-%d")

    final = pd.merge(auc_df, net_df.drop(columns=['Sr_No'], errors='ignore'), 
                    on=['Report_Date', 'Sector'], how='left')
    return final


# Rest of the helpers and main remain the same as previous version...
# (I kept it short here - use the full main from previous response)

# ================== MAIN ==================
if __name__ == "__main__":
    print("Starting FII Sector Data Download...\n")
    report_dates = generate_dates_last_12_months()
    
    all_data = []
    base_url = "https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_{}.html"

    for report_date in report_dates:
        month_str = get_nsdl_month_name(report_date)
        day_str = f"{report_date.day:02d}"
        filename = f"{month_str}{day_str}{report_date.year}"
        url = base_url.format(filename)
        
        print(f"Fetching: {report_date.strftime('%Y-%m-%d')} → {filename}")
        df = extract_fpi_data(url, report_date)
        
        if df is not None and not df.empty:
            all_data.append(df)
            print(f"  ✓ Success: {len(df)} sectors")
        else:
            print("  ✗ Failed")
        time.sleep(1.3)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        desired = ["Report_Date", "Sector", "AUC_Equity_Cr", "AUC_Total_Cr", 
                  "Net_Equity_Cr", "Net_Total_Cr"]
        final_df = final_df[[col for col in desired if col in final_df.columns]]
        
        final_df["Report_Date"] = pd.to_datetime(final_df["Report_Date"])
        final_df = final_df.sort_values(by="Report_Date", ascending=False)
        final_df["Report_Date"] = final_df["Report_Date"].dt.strftime("%Y-%m-%d")
        final_df = final_df.reset_index(drop=True)

        # Upload to Google Sheets
        try:
            sheet = client.open_by_key(SHEET_ID)
            worksheet = sheet.worksheet(TAB_NAME)
            worksheet.clear()
            worksheet.update([final_df.columns.values.tolist()] + final_df.values.tolist())
            
            print(f"\n✅ SUCCESS! Uploaded {len(final_df)} rows to Google Sheet")
            print(f"Tab: {TAB_NAME}")
        except Exception as e:
            print(f"Google Sheets failed: {e}")
            final_df.to_csv("fii_auc_net.csv", index=False)
    else:
        print("No data collected.")
