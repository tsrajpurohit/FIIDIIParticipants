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
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
                df['Date'] = date

                # Standardize column headers
                expected_columns = [
                    'Participant Type', 'Future Index Long', 'Future Index Short',
                    'Future Stock Long', 'Future Stock Short', 'Option Index Call Long',
                    'Option Index Put Long', 'Option Stock Call Long', 'Option Stock Put Long', 'Date'
                ]
                df = df.reindex(columns=expected_columns, fill_value=0)  # Fill missing columns with 0

                logging.info(f"Fetched data for {date.strftime('%d-%m-%Y')}")
                return df
            else:
                logging.error(f"Error for {date.strftime('%d-%m-%Y')}: {response.status}")
                return None
    except Exception as e:
        logging.error(f"Error fetching {date.strftime('%d-%m-%Y')}: {e}")
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
    valid_results = [result for result in results if result is not None]
    if not valid_results:
        logging.warning("No valid data fetched. Exiting.")
        return

    df = pd.concat(valid_results, ignore_index=True)
    df['Date'] = pd.to_datetime(df['Date'])

    # Save the data to Google Sheets
    upload_to_google_sheets(df)

    # Save the data to a CSV file
    save_to_csv(df)

    logging.info("Data processing completed.")

def upload_to_google_sheets(df):
    if df.empty:
        logging.warning("DataFrame is empty. No data to upload.")
        return

    try:
        # Convert the Date column to a string format for Google Sheets compatibility
        if 'Date' in df.columns:
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')  # ISO format: YYYY-MM-DD

        # Open the Google Sheet by ID
        sheet = client.open_by_key(SHEET_ID)

        # Check if the "FiiDii_OI" tab exists
        try:
            worksheet = sheet.worksheet("FiiDii_OI")
            logging.info("Tab 'FiiDii_OI' already exists.")
        except gspread.exceptions.WorksheetNotFound:
            # If the tab doesn't exist, create it
            worksheet = sheet.add_worksheet(title="FiiDii_OI", rows="1000", cols="20")
            logging.info("Tab 'FiiDii_OI' created.")
        
        # Clear the existing content in the sheet (if necessary)
        worksheet.clear()

        # Debugging: Verify data structure
        logging.info("Uploading the following data:")
        logging.info(df.head())

        # Update with the new data from DataFrame
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())
        logging.info("Data successfully uploaded to Google Sheets.")
    except Exception as e:
        logging.error(f"Error uploading to Google Sheets: {e}")


def save_to_csv(df):
    try:
        # Save the DataFrame to a CSV file
        output_filename = 'fao_participant_oi_data.csv'
        df.to_csv(output_filename, index=False)
        logging.info(f"Data successfully saved to {output_filename}.")
    except Exception as e:
        logging.error(f"Error saving to CSV: {e}")

# Run the asynchronous main function
asyncio.run(main())
