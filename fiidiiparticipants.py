import aiohttp
import asyncio
import pandas as pd
import io
import os
import json
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import gspread
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google.auth.exceptions import RefreshError

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
                # Parse CSV with explicit handling of whitespace in column names
                df = pd.read_csv(io.StringIO(url_content.decode('utf-8')), skiprows=1)
                # Clean column names: remove tabs, strip whitespace
                df.columns = [col.strip().replace('\t', '') for col in df.columns]
                # Check for duplicate columns
                if df.columns.duplicated().any():
                    print(f"Warning: Duplicate columns found for {date.strftime('%d-%m-%Y')}: {df.columns[df.columns.duplicated()]}")
                    df = df.loc[:, ~df.columns.duplicated()]  # Keep first occurrence of duplicates
                df['Date'] = date.strftime("%d-%m-%Y")
                # Check for NaN or inf values
                if df.isna().any().any():
                    print(f"Warning: NaN values found in CSV for {date.strftime('%d-%m-%Y')}")
                if df.isin([float('inf'), float('-inf')]).any().any():
                    print(f"Warning: inf values found in CSV for {date.strftime('%d-%m-%Y')}")
                print(f'Done for {date.strftime("%d-%m-%Y")}')
                return df
            else:
                print(f'Error for {date.strftime("%d-%m-%Y")}: {response.status}')
                return None
    except Exception as e:
        print(f'Error fetching {date.strftime("%d-%m-%Y")}: {e}')
        return None

# Main function to handle the asynchronous process
async def main():
    end_date = date.today()
    start_date = end_date - relativedelta(months=6)
    delta = timedelta(days=1)

    async with aiohttp.ClientSession() as session:
        tasks = []
        while start_date <= end_date:
            csv_url = f'https://archives.nseindia.com/content/nsccl/fao_participant_oi_{start_date.strftime("%d%m%Y")}.csv'
            tasks.append(fetch_data(session, csv_url, start_date))
            start_date += delta
        results = await asyncio.gather(*tasks)

    df = pd.concat([result for result in results if result is not None], ignore_index=True)
    
    # Clean column names again after concatenation
    df.columns = [col.strip().replace('\t', '') for col in df.columns]
    if df.columns.duplicated().any():
        print(f"Warning: Duplicate columns in final DataFrame: {df.columns[df.columns.duplicated()]}")
        df = df.loc[:, ~df.columns.duplicated()]

    upload_to_google_sheets(df)
    save_to_csv(df)
    print(f"Data processing completed.")

def upload_to_google_sheets(df):
    try:
        # Create a copy of the DataFrame
        df_cleaned = df.copy()

        # Replace inf/-inf with None
        df_cleaned = df_cleaned.replace([float('inf'), float('-inf')], None)

        # Replace NaN with None for all columns
        df_cleaned = df_cleaned.fillna(value=None)

        # Handle object columns to avoid string 'nan'
        for col in df_cleaned.columns:
            if df_cleaned[col].dtype == 'object':
                df_cleaned[col] = df_cleaned[col].astype(str).replace('nan', None)
            elif df_cleaned[col].dtype in ['float64', 'float32']:
                if df_cleaned[col].isna().any() or df_cleaned[col].isin([float('inf'), float('-inf')]).any():
                    print(f"Warning: Column {col} still contains NaN or inf after cleaning.")

        # Debug: Print sample and data types
        print("Sample of cleaned DataFrame before upload:")
        print(df_cleaned.head())
        print("Data types before upload:")
        print(df_cleaned.dtypes)

        # Open the Google Sheet
        sheet = client.open_by_key(SHEET_ID)
        try:
            worksheet = sheet.worksheet("FiiDii_OI")
            print("Tab 'FiiDii_OI' already exists.")
        except gspread.exceptions.WorksheetNotFound:
            rows = len(df_cleaned) + 1
            cols = len(df_cleaned.columns)
            worksheet = sheet.add_worksheet(title="FiiDii_OI", rows=rows, cols=cols)
            print("Tab 'FiiDii_OI' created.")
        
        worksheet.clear()
        worksheet.update([df_cleaned.columns.values.tolist()] + df_cleaned.values.tolist(), value_input_option='RAW')
        print("Data successfully uploaded to Google Sheets.")
    
    except Exception as e:
        print(f"Error uploading to Google Sheets: {e}")
        print("Sample of cleaned DataFrame:")
        print(df_cleaned.head())
        print("Data types:")
        print(df_cleaned.dtypes)

def save_to_csv(df):
    try:
        output_filename = 'fao_participant_oi_data.csv'
        df.to_csv(output_filename, index=False)
        print(f"Data successfully saved to {output_filename}.")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

# Run the asynchronous main function
asyncio.run(main())
