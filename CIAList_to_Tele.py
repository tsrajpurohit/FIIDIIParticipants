import os
import json
import pytz
import requests
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

# ------------------ LOAD ENV ------------------
load_dotenv()

SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
TELEGRAM_BOT_TOKEN = "5814838708:AAGMVW2amDqFcdmNMEiAetu0cLlgtMl-Kf8"
TELEGRAM_CHAT_ID = -1002192022564 #cia

if not all([SERVICE_ACCOUNT_JSON, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    print("❌ Missing required environment variables. Exiting.")
    exit()

# Parse service account JSON
try:
    service_account_info = json.loads(SERVICE_ACCOUNT_JSON)
except json.JSONDecodeError as e:
    print(f"❌ Failed to parse GOOGLE_SHEETS_CREDENTIALS JSON: {str(e)}")
    exit()

# ------------------ GOOGLE SHEETS ------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_dict(service_account_info, scope)
client = gspread.authorize(creds)

GSHEET_ID = "1hKjtvDZJjYLH5G5E3bfe5hqSoeG-XK-u1yPaSPmrkus"
TAB_NAME = "filter"
worksheet = client.open_by_key(GSHEET_ID).worksheet(TAB_NAME)

# Columns to send
COLUMNS_TO_SEND = ['A', 'E', 'G', 'H', 'K', 'BT']
HEADERS = ["Timestamp", "Close", "Symbol", "ST", "Power", "CIA"]  # Custom headers

# Fetch all columns
def get_column_values(worksheet, col_letter):
    col_index = gspread.utils.a1_to_rowcol(f"{col_letter}1")[1]
    return worksheet.col_values(col_index)[1:]  # skip header

columns_data = [get_column_values(worksheet, col) for col in COLUMNS_TO_SEND]
rows = list(zip(*columns_data))

# ------------------ FILTER TODAY & BT=TRUE ------------------
tz = pytz.timezone("Asia/Kolkata")
today_str = datetime.now(tz).strftime("%Y-%m-%d")

filtered_rows = []
for row in rows:
    timestamp, *rest, bt = row
    if timestamp.startswith(today_str) and bt.strip().upper() == "TRUE":
        filtered_rows.append(row)

# ------------------ FORMAT AS TABULAR TEXT ------------------
if filtered_rows:
    # Determine max width for each column
    col_widths = [len(h) for h in HEADERS]
    for row in filtered_rows:
        for i, val in enumerate(row):
            # Round ST and Power to 2 decimals
            if HEADERS[i] in ["ST", "Power"]:
                try:
                    val = f"{float(val):.2f}"
                except:
                    pass
            col_widths[i] = max(col_widths[i], len(str(val)))
    
    # Build header line
    header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(HEADERS))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(HEADERS)))
    
    # Build row lines
    row_lines = []
    for row in filtered_rows:
        line_values = []
        for i, val in enumerate(row):
            # Round ST and Power to 2 decimals
            if HEADERS[i] in ["ST", "Power"]:
                try:
                    val = f"{float(val):.2f}"
                except:
                    pass
            line_values.append(str(val).ljust(col_widths[i]))
        row_lines.append(" | ".join(line_values))
    
    table_text = "\n".join([header_line, separator] + row_lines)
    
    # ------------------ SEND TO TELEGRAM ------------------
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": f"```\n{table_text}\n```", "parse_mode": "MarkdownV2"}
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print("✅ All rows sent in formatted table")
    else:
        print(f"❌ Failed to send, Response: {response.text}")

else:
    print("No rows found for today with BT=TRUE")
