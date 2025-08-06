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

# Fetch credentials and Sheet ID from environment variables
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')  # JSON string
SHEET_ID = "1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY"

if not credentials_json:
    raise ValueError("GOOGLE_SHEETS_CREDENTIALS environment variable is not set.")

# Authenticate using the JSON string from environment
credentials_info = json.loads(credentials_json)
credentials = Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)
client = gspread.authorize(credentials)

# Function to download and parse CSV
async def fetch_data(session, url, date):
    try:
        async with session.get(url, timeout=10) as response:
            if response.status == 200:
                url_content = await response.read()
                df = pd.read_csv(io.StringIO(url_content.decode('utf-8')), skiprows=1)
                df['Date'] = date.strftime("%d-%m-%Y")
                print(f'✅ Done for {date.strftime("%d-%m-%Y")}')
                return df
            else:
                print(f'❌ Error for {date.strftime("%d-%m-%Y")}: HTTP {response.status}')
                return None
    except Exception as e:
        print(f'❌ Error fetching {date.strftime("%d-%m-%Y")}: {e}')
        return None

# Main function to handle the asynchronous process
async def main():
    end_date = date.today()
    start_date = end_date - relativedelta(months=6)
    delta = timedelta(days=1)

    tasks = []

    async with aiohttp.ClientSession() as session:
        while start_date <= end_date:
            if start_date.weekday() < 5:  # Weekday only (Mon-Fri)
                formatted_date = start_date.strftime("%d-%b-%Y")  # Format: 06-Aug-2025
                csv_url = (
                    "https://www.nseindia.com/api/reports?"
                    "archives=%5B%7B%22name%22%3A%22F%26O%20-%20Participant%20wise%20Open%20Interest(csv)%22%2C"
                    "%22type%22%3A%22archives%22%2C%22category%22%3A%22derivatives%22%2C%22section%22%3A%22equity%22%7D%5D"
                    f"&date={formatted_date}&type=equity&mode=single"
                )
                tasks.append(fetch_data(session, csv_url, start_date))
            start_date += delta

        results = await asyncio.gather(*tasks)

    # Filter out failed/empty results
    valid_results = [df for df in results if df is not None and not df.empty]

    if not valid_results:
        print("❌ No data fetched for any date.")
        return

    df = pd.concat(valid_results, ignore_index=True)

    # Save and upload
    save_to_csv(df)
    upload_to_google_sheets(df)
    print("✅ Data processing completed.")

def upload_to_google_sheets(df):
    try:
        sheet = client.open_by_key(SHEET_ID)
        try:
            worksheet = sheet.worksheet("FiiDii_OI")
            print("📄 Tab 'FiiDii_OI' already exists.")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title="FiiDii_OI", rows=str(len(df)+1), cols=str(len(df.columns)))
            print("🆕 Tab 'FiiDii_OI' created.")
        
        worksheet.clear()
        df_cleaned = df.replace([float('inf'), float('-inf')], None).fillna('')
        worksheet.update([df_cleaned.columns.tolist()] + df_cleaned.values.tolist(), value_input_option='RAW')
        print("📤 Data uploaded to Google Sheets.")
    except Exception as e:
        print(f"❌ Error uploading to Google Sheets: {e}")

def save_to_csv(df):
    try:
        output_filename = 'fao_participant_oi_data.csv'
        df.to_csv(output_filename, index=False)
        print(f"💾 Data saved to {output_filename}.")
    except Exception as e:
        print(f"❌ Error saving to CSV: {e}")

# Run the script
asyncio.run(main())
