import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import mysql.connector
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")

GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")
GDRIVE_CREDENTIALS_JSON = os.getenv("GDRIVE_CREDENTIALS_JSON", "client_secrets.json")
GDRIVE_CREDENTIALS_PICKLE = "gdrive_credentials.json"  # Token storage file

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


def generate_insert_components(table, columns, rows):
    if not rows:
        return None, None
    header = f"INSERT INTO `{table}` ({', '.join(columns)}) VALUES\n"
    values = [f"({', '.join(format_value(v) for v in row)})" for row in rows]
    return header, values


def backup_table(cursor, table, last_time):
    query = f"SELECT * FROM `{table}` WHERE `timestamp` > %s ORDER BY `timestamp` ASC"
    cursor.execute(query, (last_time,))
    rows = cursor.fetchall()
    if not rows:
        logging.info(f"[!] No new rows in `{table}` since {last_time}")
        return None, last_time

    columns = [col[0] for col in cursor.description]
    header, values = generate_insert_components(table, columns, rows)
    if not header or not values:
        return None, last_time

    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{table}_increment_{now_str}.sql"
    filepath = os.path.join(BACKUP_DIR, filename)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(header + ",\n".join(values) + ";\n")

    new_last_time = str(rows[-1][columns.index("timestamp")])
    logging.info(f"[+] Backed up {len(rows)} rows from `{table}` to {filename}")
    return (header, values), new_last_time


def authenticate_gdrive():
    if not os.path.exists(GDRIVE_CREDENTIALS_JSON):
        logging.error(f"[ERROR] Google credentials JSON not found: {GDRIVE_CREDENTIALS_JSON}")
        return None

    try:
        gauth = GoogleAuth()
        gauth.LoadClientConfigFile(GDRIVE_CREDENTIALS_JSON)

        if os.path.exists(GDRIVE_CREDENTIALS_PICKLE):
            gauth.LoadCredentialsFile(GDRIVE_CREDENTIALS_PICKLE)

        if gauth.credentials is None:
            logging.info("No saved credentials, performing command line OAuth...")
            gauth.CommandLineAuth()
        elif gauth.access_token_expired:
            logging.info("Credentials expired, refreshing...")
            gauth.Refresh()
        else:
            logging.info("Using saved Google Drive credentials.")

        gauth.SaveCredentialsFile(GDRIVE_CREDENTIALS_PICKLE)
        return GoogleDrive(gauth)
    except Exception as e:
        logging.error(f"[ERROR] Google Drive authentication failed: {e}")
        return None


def find_drive_file(drive, filename):
    query = f"title = '{filename}' and trashed=false"
    file_list = drive.ListFile({'q': query}).GetList()
    return file_list[0] if file_list else None


def download_drive_file_content(file):
    try:
        file.GetContentFile("temp.sql")
        with open("temp.sql", "r", encoding="utf-8") as f:
            content = f.read()
        os.remove("temp.sql")
        return content
    except Exception as e:
        logging.error(f"[ERROR] Could not download file content: {e}")
        return ""


def upload_to_gdrive(drive, filename, insert_components):
    header, new_rows = insert_components
    file = find_drive_file(drive, filename)

    if file:
        logging.info(f"Appending to existing Drive file: {filename}")
        existing_content = download_drive_file_content(file).strip()

        # Remove trailing semicolon if present
        if existing_content.endswith(";"):
            existing_content = existing_content[:-1].strip()

        new_content = existing_content
        if new_rows:
            if not existing_content.endswith(")"):
                new_content += "\n"
            new_content += ",\n" + ",\n".join(new_rows) + ";\n"

        file.SetContentString(new_content)
        file.Upload()
    else:
        logging.info(f"Creating new Drive file: {filename}")
        content = header + ",\n".join(new_rows) + ";\n"
        file_metadata = {'title': filename}
        if GDRIVE_FOLDER_ID:
            file_metadata['parents'] = [{'id': GDRIVE_FOLDER_ID}]
        file = drive.CreateFile(file_metadata)
        file.SetContentString(content)
        file.Upload()

    logging.info(f"[+] Uploaded backup to Google Drive as {filename}")
    return True


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

        drive = authenticate_gdrive()
        if not drive:
            logging.error("Google Drive auth failed, aborting backup.")
            return

        updated_times = last_times.copy()
        for table in TABLES:
            logging.info(f"\n=== Processing: {table} ===")
            insert_components, new_time = backup_table(cursor, table, last_times.get(table, "1970-01-01 00:00:00"))
            if insert_components:
                filename = f"{table}_incremental_backup.sql"
                if upload_to_gdrive(drive, filename, insert_components):
                    updated_times[table] = new_time

        save_last_backup_times(updated_times)
        logging.info("\n✅ Incremental backup complete.")

    except Exception as e:
        logging.error(f"[ERROR] {e}")


if __name__ == "__main__":
    main()
