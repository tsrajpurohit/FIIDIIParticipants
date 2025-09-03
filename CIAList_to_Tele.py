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
TELEGRAM_CHAT_ID = -1002192022564

if not all([SERVICE_ACCOUNT_JSON, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
    print("❌ Missing required environment variables. Exiting.")
    exit()

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

try:
    worksheet = client.open_by_key(GSHEET_ID).worksheet(TAB_NAME)
except gspread.exceptions.WorksheetNotFound:
    print(f"❌ Worksheet '{TAB_NAME}' not found in spreadsheet.")
    exit()
except gspread.exceptions.SpreadsheetNotFound:
    print(f"❌ Spreadsheet with ID '{GSHEET_ID}' not found.")
    exit()

COLUMNS_TO_SEND = ['A', 'E', 'G', 'H', 'K', 'BT']
HEADERS = ["Timestamp", "Close", "Symbol", "ST", "Power", "CIA"]

# Fetch column values
def get_column_values(worksheet, col_letter):
    try:
        col_index = gspread.utils.a1_to_rowcol(f"{col_letter}1")[1]
        return worksheet.col_values(col_index)[1:]  # skip header
    except Exception as e:
        print(f"❌ Error fetching column {col_letter}: {str(e)}")
        return []

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
def escape_markdown_v2(text):
    special_chars = r'_*\[]()~`>#+=|{}.!-'
    for char in special_chars:
        text = text.replace(char, f"\\{char}")
    return text

def send_telegram_message(text, chat_id, token):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    max_length = 4000  # Slightly below 4096 to account for Markdown markers
    if len(text) <= max_length:
        payload = {"chat_id": chat_id, "text": f"```\n{text}\n```", "parse_mode": "MarkdownV2"}
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print("✅ Message sent successfully")
        else:
            print(f"❌ Failed to send message: {response.text}")
        return response.status_code == 200
    else:
        # Split table into chunks
        lines = text.split("\n")
        header_lines = lines[:2]  # Header and separator
        data_lines = lines[2:]
        chunk_size = 50  # Adjust based on row length; estimate rows per chunk
        for i in range(0, len(data_lines), chunk_size):
            chunk_lines = header_lines + data_lines[i:i + chunk_size]
            chunk_text = "\n".join(chunk_lines)
            payload = {"chat_id": chat_id, "text": f"```\n{chunk_text}\n```", "parse_mode": "MarkdownV2"}
            response = requests.post(url, data=payload)
            if response.status_code != 200:
                print(f"❌ Failed to send chunk {i//chunk_size + 1}: {response.text}")
                return False
        print(f"✅ Sent {len(data_lines)//chunk_size + 1} message chunks")
        return True

if filtered_rows:
    col_widths = [len(h) for h in HEADERS]
    for row in filtered_rows:
        for i, val in enumerate(row):
            if HEADERS[i] in ["ST", "Power"]:
                try:
                    val = f"{float(val):.2f}"
                except (ValueError, TypeError):
                    print(f"⚠️ Warning: Non-numeric value '{val}' in column {HEADERS[i]}")
            col_widths[i] = max(col_widths[i], len(str(val)))
    
    header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(HEADERS))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(HEADERS)))
    
    row_lines = []
    for row in filtered_rows:
        line_values = []
        for i, val in enumerate(row):
            if HEADERS[i] in ["ST", "Power"]:
                try:
                    val = f"{float(val):.2f}"
                except (ValueError, TypeError):
                    pass
            line_values.append(escape_markdown_v2(str(val)).ljust(col_widths[i]))
        row_lines.append(" | ".join(line_values))
    
    table_text = "\n".join([header_line, separator] + row_lines)
    
    send_telegram_message(table_text, TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN)
else:
    send_telegram_message("No rows found for today with BT=TRUE", TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN)
    print("No rows found for today with BT=TRUE")
