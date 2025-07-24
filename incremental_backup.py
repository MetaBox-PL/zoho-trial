import os
import json
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# Load environment variables from e.env (not .env)
load_dotenv()

# Read MySQL and Drive credentials from env
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASS")  # Use DB_PASS from your e.env
DB_NAME = os.getenv("DB_NAME")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")

# Backup settings
BACKUP_DIR = "backups"
LAST_BACKUP_FILE = "last_backup_time.json"
TABLES = ["attendance_logs", "raw_device_logs", "raw_zoho_logs"]

os.makedirs(BACKUP_DIR, exist_ok=True)

def load_last_backup_times():
    if os.path.exists(LAST_BACKUP_FILE):
        with open(LAST_BACKUP_FILE, "r") as f:
            return json.load(f)
    return {table: "1970-01-01 00:00:00" for table in TABLES}

def save_last_backup_times(times):
    with open(LAST_BACKUP_FILE, "w") as f:
        json.dump(times, f, indent=2)

def format_value(value):
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"

def generate_insert_statements(table, columns, rows):
    if not rows:
        return ""
    sql = f"INSERT INTO `{table}` ({', '.join(columns)}) VALUES\n"
    values = []
    for row in rows:
        formatted = ", ".join(format_value(v) for v in row)
        values.append(f"({formatted})")
    return sql + ",\n".join(values) + ";\n"

def backup_table(cursor, table, last_time):
    query = f"SELECT * FROM `{table}` WHERE `timestamp` > %s ORDER BY `timestamp` ASC"
    cursor.execute(query, (last_time,))
    rows = cursor.fetchall()
    if not rows:
        print(f"[!] No new rows in `{table}` since {last_time}")
        return None, last_time

    columns = [col[0] for col in cursor.description]
    sql_data = generate_insert_statements(table, columns, rows)
    if not sql_data:
        return None, last_time

    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{table}_increment_{now_str}.sql"
    filepath = os.path.join(BACKUP_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(sql_data)

    new_last_time = str(rows[-1][columns.index("timestamp")])
    print(f"[+] Backed up {len(rows)} rows from `{table}` to {filename}")
    return filepath, new_last_time

def upload_to_gdrive(filepath):
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Only asks for login on first run
    drive = GoogleDrive(gauth)

    meta = {'title': os.path.basename(filepath)}
    if GDRIVE_FOLDER_ID:
        meta['parents'] = [{'id': GDRIVE_FOLDER_ID}]

    file = drive.CreateFile(meta)
    file.SetContentFile(filepath)
    file.Upload()
    print(f"[+] Uploaded {filepath} to Google Drive")

def main():
    last_times = load_last_backup_times()
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
        )
        cursor = conn.cursor()

        updated_times = last_times.copy()
        for table in TABLES:
            print(f"\n=== Processing: {table} ===")
            filepath, new_time = backup_table(cursor, table, last_times.get(table, "1970-01-01 00:00:00"))
            if filepath:
                upload_to_gdrive(filepath)
                updated_times[table] = new_time

        save_last_backup_times(updated_times)
        print("\n✅ Incremental backup complete.")

    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    main()
