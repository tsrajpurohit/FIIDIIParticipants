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
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                url_content = await response.read()
                df = pd.read_csv(io.StringIO(url_content.decode('utf-8')), skiprows=1)
                df['Date'] = date  # Keep as `datetime.date`
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
    # Calculate the current date (end date) and the start date 6 months ago
    end_date = date.today()
    start_date = end_date - relativedelta(months=6)

    delta = timedelta(days=1)

    # Create a session for making requests
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Create tasks for each day's request between the start and end date
        while start_date <= end_date:
            csv_url = f'https://archives.nseindia.com/content/nsccl/fao_participant_oi_{start_date.strftime("%d%m%Y")}.csv'
            tasks.append(fetch_data(session, csv_url, start_date))
            start_date += delta

        # Await all tasks and gather results
        results = await asyncio.gather(*tasks)

    # Combine all the DataFrames
    df = pd.concat([result for result in results if result is not None], ignore_index=True)
    df['Date'] = pd.to_datetime(df['Date'])

    # Save the data to Google Sheets
    upload_to_google_sheets(df)

    # Save the data to a CSV file
    save_to_csv(df)

    print(f"Data processing completed.")

def upload_to_google_sheets(df):
    try:
        # Open the Google Sheet by ID
        sheet = client.open_by_key(SHEET_ID)
        
        # Check if the "FiiDii_OI" tab exists
        try:
            worksheet = sheet.worksheet("FiiDii_OI")
            print("Tab 'FiiDii_OI' already exists.")
        except gspread.exceptions.WorksheetNotFound:
            # If the tab doesn't exist, create it
            worksheet = sheet.add_worksheet(title="FiiDii_OI", rows="1000", cols="20")
            print("Tab 'FiiDii_OI' created.")
        
        # Clear the existing content in the sheet (if necessary)
        worksheet.clear()
        
        # Update with the new data from DataFrame
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        print("Data successfully uploaded to Google Sheets.")
    
    except Exception as e:
        print(f"Error uploading to Google Sheets: {e}")

def save_to_csv(df):
    try:
        # Save the DataFrame to a CSV file
        output_filename = 'fao_participant_oi_data.csv'
        df.to_csv(output_filename, index=False)
        print(f"Data successfully saved to {output_filename}.")
    except Exception as e:
        print(f"Error saving to CSV: {e}")

# Run the asynchronous main function
asyncio.run(main())
