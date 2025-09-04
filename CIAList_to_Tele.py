import os
import json
import pytz
import requests
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from datetime import datetime, timedelta
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

COLUMNS_TO_SEND = ['A', 'E', 'G', 'H', 'K','BT']
HEADERS = ["Timestamp", "Close", "Symbol", "ST", "Power",'CIA']
MAX_COL_WIDTH = 20  # Cap column width to reduce padding

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
#today_str = datetime.now(tz).strftime("%Y-%m-%d")
# Yesterday
today_str = (datetime.now(tz) - timedelta(days=1)).strftime("%Y-%m-%d")


filtered_rows = []
for row in rows:
    *data, bt = row  # unpack: all data except last, then bt separately
    timestamp = data[0]
    if timestamp.startswith(today_str) and bt.strip().upper() == "TRUE":
        filtered_rows.append(data)   # only keep data (exclude BT)

# ------------------ FORMAT AS TABULAR TEXT ------------------
def escape_markdown_v2(text):
    special_chars = r'_*\[]()~`>#+=|{}.!-'
    for char in special_chars:
        text = text.replace(char, f"\\{char}")
    return text

def send_telegram_message(text, chat_id, token):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    max_length = 4000  # Slightly below 4096 to account for Markdown
    if len(text) <= max_length:
        payload = {"chat_id": chat_id, "text": f"```\n{text}\n```", "parse_mode": "MarkdownV2"}
        response = requests.post(url, data=payload)
        if response.status_code == 200:
            print("✅ Message sent successfully")
        else:
            print(f"❌ Failed to send message: {response.text}")
        return response.status_code == 200
    else:
        # Split into chunks based on estimated row length
        lines = text.split("\n")
        header_lines = lines[:2]  # Header and separator
        data_lines = lines[2:]
        # Estimate average row length (including separators)
        avg_row_length = sum(len(line) for line in data_lines[:10]) / max(len(data_lines[:10]), 1) if data_lines else 1
        chunk_size = max(1, int((max_length - len("\n".join(header_lines))) / max(avg_row_length, 1)))
        print(f"📏 Estimated chunk size: {chunk_size} rows (avg row length: {avg_row_length:.1f} chars)")
        
        for i in range(0, len(data_lines), chunk_size):
            chunk_lines = header_lines + data_lines[i:i + chunk_size]
            chunk_text = "\n".join(chunk_lines)
            print(f"📤 Sending chunk {i//chunk_size + 1} with {len(chunk_lines)-2} rows ({len(chunk_text)} chars)")
            payload = {"chat_id": chat_id, "text": f"```\n{chunk_text}\n```", "parse_mode": "MarkdownV2"}
            response = requests.post(url, data=payload)
            if response.status_code != 200:
                print(f"❌ Failed to send chunk {i//chunk_size + 1}: {response.text}")
                return False
        print(f"✅ Sent {len(data_lines)//chunk_size + 1} message chunks")
        return True

# ------------------ FORMAT AS TABULAR TEXT ------------------
if filtered_rows:
    # Auto-adjust column widths based on data length (with safe cap)
    col_widths = []
    for i in range(len(HEADERS)):
        max_len = max((len(str(row[i])) for row in filtered_rows), default=0)
        max_len = min(max_len, 60)  # cap max column width to 60 chars
        col_widths.append(max(max_len, len(HEADERS[i])))

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
            val_str = str(val)  # no truncation, we handle with col_widths
            line_values.append(escape_markdown_v2(val_str).ljust(col_widths[i]))
        row_lines.append(" | ".join(line_values))

    table_text = "\n".join([header_line, separator] + row_lines)
    print(f"📊 Total table size: {len(table_text)} characters, {len(filtered_rows)} rows")

    send_telegram_message(table_text, TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN)
else:
    send_telegram_message("No rows found for today with BT=TRUE", TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN)
    print("No rows found for today with BT=TRUE")

