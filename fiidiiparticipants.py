import aiohttp
import asyncio
import pandas as pd
import io
import os
import json
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import gspread
from google.oauth2.service_account import Credentials

# Get credentials and Sheet ID
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"

if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")

credentials_info = json.loads(credentials_json)
credentials = Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(credentials)

# Headers to mimic browser
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nseindia.com/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}

# Function to download and parse CSV
async def fetch_data(session, date_obj):
    if date_obj.weekday() >= 5:  # Skip weekends
        return None

    date_str = date_obj.strftime("%d%m%Y")
    url = f'https://nsearchives.nseindia.com/content/nsccl/fao_participant_oi_{date_str}.csv'

    try:
        async with session.get(url, headers=HEADERS, timeout=10) as response:
            if response.status == 200:
                content = await response.read()
                df = pd.read_csv(io.StringIO(content.decode('utf-8')), skiprows=1)
                df['Date'] = date_obj.strftime("%d-%m-%Y")
                print(f"✅ Done for {date_obj.strftime('%d-%m-%Y')}")
                return df
            else:
                print(f"❌ Error {response.status} fetching {date_obj.strftime('%d-%m-%Y')}")
                return None
    except Exception as e:
        print(f"❌ Error fetching {date_obj.strftime('%d-%m-%Y')}: {e}")
        return None

async def main():
    end_date = date.today()
    start_date = end_date - relativedelta(months=6)
    delta = timedelta(days=1)

    tasks = []
    async with aiohttp.ClientSession() as session:
        current = start_date
        while current <= end_date:
            tasks.append(fetch_data(session, current))
            current += delta

        results = await asyncio.gather(*tasks)

    # Filter non-empty dataframes
    valid_data = [df for df in results if df is not None]

    if not valid_data:
        print("❌ No data fetched for any date.")
        return

    df_all = pd.concat(valid_data, ignore_index=True)

    upload_to_google_sheets(df_all)
    save_to_csv(df_all)
    print("✅ Data processing completed.")

def upload_to_google_sheets(df):
    try:
        sheet = client.open_by_key(SHEET_ID)
        try:
            worksheet = sheet.worksheet("FiiDii_OI_Row")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet("FiiDii_OI", rows=str(len(df)+1), cols=str(len(df.columns)))

        worksheet.clear()
        df_cleaned = df.replace([float('inf'), float('-inf')], None).fillna('')
        worksheet.update([df_cleaned.columns.values.tolist()] + df_cleaned.values.tolist(), value_input_option='RAW')
        print("✅ Uploaded to Google Sheets.")
    except Exception as e:
        print(f"❌ Google Sheets upload error: {e}")

def save_to_csv(df):
    try:
        df.to_csv('fao_participant_oi_data.csv', index=False)
        print("✅ Saved to fao_participant_oi_data.csv")
    except Exception as e:
        print(f"❌ CSV save error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
