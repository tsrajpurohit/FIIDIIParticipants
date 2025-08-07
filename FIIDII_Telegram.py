import os
import json
import requests
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import dateutil.parser
import pytz  

# Load environment variables
load_dotenv()

# Validate environment variables
SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
TELEGRAM_BOT_TOKEN = "5814838708:AAGMVW2amDqFcdmNMEiAetu0cLlgtMl-Kf8"
TELEGRAM_CHAT_ID = "-1002355806500"

if not all([SERVICE_ACCOUNT_JSON, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    print("❌ Missing required environment variables. Ensure GOOGLE_APPLICATION_CREDENTIALS, TELEGRAM_BOT_TOKEN, and TELEGRAM_CHAT_ID are set in .env.")
    exit()

# Parse service account JSON
try:
    service_account_info = json.loads(SERVICE_ACCOUNT_JSON)
except json.JSONDecodeError as e:
    print(f"❌ Failed to parse GOOGLE_APPLICATION_CREDENTIALS JSON: {str(e)}")
    print("Ensure the JSON is a valid single-line string with escaped newlines.")
    exit()

# Google Sheets setup
SHEET_ID = '1IUChF0UFKMqVLxTI69lXBi-g48f-oTYqI1K9miipKgY'
SHEET_NAME = 'Fiiparticipants'
SHEET_RANGE = 'D30:J54'
B32_RANGE = 'B32'  # Range for cell B32

# Authenticate with Google Sheets API
try:
    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    service = build("sheets", "v4", credentials=credentials)
    sheet = service.spreadsheets()
    
    # Fetch data for the table (D30:J54)
    result = sheet.values().get(spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!{SHEET_RANGE}").execute()
    values = result.get("values", [])
    
    # Fetch cell B32
    b32_result = sheet.values().get(spreadsheetId=SHEET_ID, range=f"{SHEET_NAME}!{B32_RANGE}").execute()
    b32_value = b32_result.get("values", [[]])[0][0] if b32_result.get("values") else None
    
except Exception as e:
    print(f"❌ Failed to fetch data from Google Sheets: {str(e)}")
    exit()

# Check if B32 has today's date
send_message = False
if b32_value:
    try:
        # Parse the date in B32 (handles various formats like MM/DD/YYYY, DD-MM-YYYY, etc.)
        sheet_date = dateutil.parser.parse(b32_value).date()
        today_date = datetime.now().date()
        send_message = (sheet_date == today_date)
        print(f"📅 B32 Date: {sheet_date}, Today's Date: {today_date}, Send Message: {send_message}")
    except (ValueError, TypeError) as e:
        print(f"❌ Failed to parse date in B32 ('{b32_value}'): {str(e)}")
        send_message = False
else:
    print("❌ No value found in B32.")
    send_message = False

# Check if data is empty
if not values:
    print("❌ No data found in the specified range.")
    exit()

# Debug: Log raw data for inspection
with open("sheet_data.json", "w") as f:
    json.dump(values, f, indent=2)
print("Raw data saved to sheet_data.json for debugging.")

# Process headers and rows
expected_columns = 7  # Based on range D30:J54 (columns D to J)
headers = values[0] if values else []
if len(headers) < expected_columns:
    headers += [""] * (expected_columns - len(headers))  # Pad headers
elif len(headers) > expected_columns:
    headers = headers[:expected_columns]  # Truncate headers
rows = values[1:] if values else []

# Clean rows to match expected column count
cleaned_rows = [
    row + [""] * (expected_columns - len(row)) if len(row) < expected_columns else row[:expected_columns]
    for row in rows
]

# Create DataFrame
try:
    df = pd.DataFrame(cleaned_rows, columns=headers)
except ValueError as e:
    print(f"❌ DataFrame creation failed: {str(e)}")
    print("Headers:", headers)
    print("First few rows:", cleaned_rows[:3])
    exit()

# Plot DataFrame as a table
fig, ax = plt.subplots(figsize=(min(12, len(headers) * 1.5), len(df) * 0.5 + 0.5))  # Reduced height to minimize top space
ax.axis("off")

# Adjust table bounding box to remove space above and maximize table size
table = ax.table(
    cellText=df.values,
    colLabels=df.columns,
    loc="center",
    cellLoc="center",
    colColours=['#FFA07A'] * len(df.columns),  # Light orange headers
    bbox=[0, 0.05, 1, 0.90]  # Adjusted: 5% bottom margin, 90% height to maximize table size
)
table.auto_set_font_size(False)
table.set_fontsize(10)
table.scale(1, 1.5)

# Style the table to match Google Sheets color scheme
for (i, j), cell in table.get_celld().items():
    cell.set_edgecolor('#D3D3D3')  # Light gray gridlines
    cell.set_linewidth(0.5)  # Thin gridlines
    if i == 0:  # Header row
        cell.set_text_props(weight='bold', color='black')
        cell.set_facecolor('#FFA07A')  # Light orange for headers
    elif j == 0 and i > 0:  # First column (e.g., "Stock Future")
        cell.set_facecolor('#D3D3D3')  # Gray for column D
        cell.set_text_props(weight='bold', color='black')
    else:
        # Apply color based on Interpretation column (column H, index 5)
        interpretation = df.iloc[i-1, 5] if i > 0 and len(df.columns) > 5 and j == 5 else None
        if j == 5:  # Column H (Interpretation)
            if interpretation == "Bearish":
                cell.set_facecolor('#FF9999')  # Light red for Bearish
                cell.set_text_props(color='black')
            elif interpretation == "Bullish":
                cell.set_facecolor('#99FF99')  # Light green for Bullish
                cell.set_text_props(color='black')
            else:
                cell.set_facecolor('#FFFFFF')  # White for other values in Interpretation
                cell.set_text_props(color='black')
        elif j == 1:  # Column E (specific coloring)
            if i-1 in [0, 6, 12, 18]:  # E31, E37, E43, E49 (0-based indices 1, 7, 13, 19)
                cell.set_facecolor('#ADD8E6')  # Light blue
            elif (2 <= i <= 6) or (8 <= i <= 11) or (14 <= i <= 17) or (20 <= i <= 23):  # E32:E36, E38:E41, E44:E47, E50:E53
                cell.set_facecolor('#FFFF99')  # Light yellow
            else:
                cell.set_facecolor('#FFFFFF')  # Default white
        elif j in [2, 3, 4, 5] and i-1 in [0, 6, 12, 18]:  # F31:I31, F37:I37, F43:I43, F49:I49 (0-based indices 1, 7, 13, 19)
            cell.set_facecolor('#D2B48C')  # Light brown
        else:
            cell.set_facecolor('#FFFFFF')  # White for other cells

        # Apply value-based font color for numeric columns (e.g., Net Position, Today Net Change)
        try:
            value = float(df.iloc[i-1, j]) if i > 0 and pd.notna(df.iloc[i-1, j]) else None
            if value is not None and j in [1, 2, 3, 6]:  # Apply to columns E, F, G, J
                reverse_color_rows = {4, 10, 16, 22}
                if i-1 in reverse_color_rows:
                    cell.set_text_props(color='red' if value >= 0 else 'green')
                else:
                    cell.set_text_props(color='green' if value >= 0 else 'red')
            else:
                cell.set_text_props(color='black')
        except (ValueError, TypeError):
            cell.set_text_props(color='black')

# Add footer below the table
ist = pytz.timezone('Asia/Kolkata')  # Use IST for timestamp
current_time = datetime.now(ist)
footer_text = f"Generated by https://t.me/Nifty_BankNifty_Alerts | {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}"
fig.text(0.5, 0.02, footer_text, ha='center', va='bottom', fontsize=8, 
         color='black', bbox=dict(facecolor='#F0F0F0', edgecolor='none', pad=3))

# Adjust layout to prevent clipping
plt.tight_layout()
IMG_FILENAME = "FiiParticipants.png"
plt.savefig(IMG_FILENAME, dpi=300, bbox_inches='tight', facecolor='white')  # High DPI for quality
plt.close()

# Send image to Telegram with caption only if B32 has today's date
if send_message:
    try:
        caption = f"FII Participants Data for {b32_value if b32_value else 'Date not available'}\n | By @Nifty_BankNifty_Alerts"
        with open(IMG_FILENAME, "rb") as img_file:
            response = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption},
                files={"photo": img_file}
            )
        if response.ok:
            print("✅ Image with caption sent to Telegram successfully.")
        else:
            try:
                error_info = response.json()
                print(f"❌ Failed to send image. Error: {error_info.get('description', 'Unknown error')}")
            except ValueError:
                print(f"❌ Failed to send image. HTTP Status: {response.status_code}")
    except Exception as e:
        print(f"❌ Failed to send image to Telegram: {str(e)}")
else:
    print("⏭️ Skipped sending message: B32 date does not match today's date.")

# Clean up the image file
if os.path.exists(IMG_FILENAME):
    os.remove(IMG_FILENAME)
    print(f"🗑️ Deleted temporary file: {IMG_FILENAME}")
