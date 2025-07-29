import os
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

# ===== LOAD ENVIRONMENT VARIABLES =====
load_dotenv("e.env")

# ===== CONFIGURATION =====
DOMAIN = os.getenv("ZOHO_DOMAIN", "zoho.com")
CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "database": os.getenv("DB_NAME")
}

# ===== LOGGING SETUP =====
def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("zoho_logs.log"),
            logging.StreamHandler()
        ]
    )

# ===== GET ACCESS TOKEN =====
def get_access_token():
    try:
        res = requests.post(
            f"https://accounts.{DOMAIN}/oauth/v2/token",
            data={
                "refresh_token": REFRESH_TOKEN,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type": "refresh_token"
            },
            timeout=10
        )
        res.raise_for_status()
        token = res.json()["access_token"]
        logging.info("✅ Access token retrieved.")
        return token
    except Exception as e:
        logging.error(f"❌ Failed to get access token: {e}")
        raise

# ===== GET LAST SYNCED TIMESTAMP =====
def get_last_synced_timestamp():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(timestamp) FROM attendance_logs WHERE source = 'zoho'")
        result = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return result or (datetime.now() - timedelta(days=30))
    except Exception as e:
        logging.error(f"❌ Failed to get last synced timestamp: {e}")
        return datetime.now() - timedelta(days=30)

# ===== CHECK IF LOG EXISTS IN attendance_logs =====
def log_exists_in_attendance(user_id, timestamp, punch_type):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM attendance_logs WHERE user_id = %s AND timestamp = %s AND punch_type = %s",
            (user_id, timestamp, punch_type)
        )
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count > 0
    except Exception as e:
        logging.error(f"❌ Error checking attendance_logs: {e}")
        return False

# ===== CHECK IF LOG EXISTS IN raw_zoho_logs =====
def log_exists_in_raw_zoho(user_id, timestamp, punch_type):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM raw_zoho_logs WHERE user_id = %s AND timestamp = %s AND punch_type = %s",
            (user_id, timestamp, punch_type)
        )
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return count > 0
    except Exception as e:
        logging.error(f"❌ Error checking raw_zoho_logs: {e}")
        return False

# ===== INSERT LOG INTO DATABASE =====
def insert_log_to_db(user_id, name, timestamp, status):
    punch_type = 0 if status == "Check-In" else 1
    conn = None
    try:
        if log_exists_in_attendance(user_id, timestamp, punch_type) or \
           log_exists_in_raw_zoho(user_id, timestamp, punch_type):
            return False  # Skip duplicates silently

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO attendance_logs (user_id, name, timestamp, punch_type, synced, source)
            VALUES (%s, %s, %s, %s, 1, 'zoho')
        """, (user_id, name, timestamp, punch_type))

        cursor.execute("""
            INSERT INTO raw_zoho_logs (user_id, name, timestamp, punch_type, source)
            VALUES (%s, %s, %s, %s, 'zoho')
        """, (user_id, name, timestamp, punch_type))

        conn.commit()
        logging.info(f"🟢 Inserted: {name} ({user_id}) - {status} at {timestamp}")
        return True
    except Exception as e:
        logging.error(f"❌ Insert error for {name} at {timestamp}: {e}")
        return False
    finally:
        if conn is not None and conn.is_connected():
            cursor.close()
            conn.close()

# ===== FETCH ZOHO ATTENDANCE =====
def fetch_zoho_attendance(token, from_date):
    url = f"https://people.{DOMAIN}/people/api/attendance/fetchLatestAttEntries"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    params = {
        "duration": "200",
        "fromDate": from_date.strftime("%d-%m-%Y"),
        "dateTimeFormat": "dd-MM-yyyy HH:mm:ss"
    }

    logging.info(f"📡 Fetching Zoho logs from: {from_date.strftime('%d-%m-%Y')}...")

    inserted_count = 0

    try:
        res = requests.get(url, headers=headers, params=params, timeout=20)
        data = res.json()

        if data.get("response", {}).get("status") != 0:
            logging.error("❌ Failed to fetch Zoho data.")
            logging.error(data)
            return

        for emp in data["response"].get("result", []):
            emp_id = emp.get("employeeId")
            name = emp_id  # You can later resolve full name from Zoho if needed
            user_id = emp_id  # Directly use Zoho empId as user_id

            for entry in emp.get("entries", []):
                for day_entry in entry.values():
                    for att in day_entry.get("attEntries", []):
                        if "checkInTime" in att:
                            timestamp = datetime.strptime(att["checkInTime"], "%d-%m-%Y %H:%M:%S")
                            if insert_log_to_db(user_id, name, timestamp, "Check-In"):
                                inserted_count += 1
                        if "checkOutTime" in att:
                            timestamp = datetime.strptime(att["checkOutTime"], "%d-%m-%Y %H:%M:%S")
                            if insert_log_to_db(user_id, name, timestamp, "Check-Out"):
                                inserted_count += 1

        if inserted_count == 0:
            logging.info("🆕 0 new records found.")
        else:
            logging.info(f"🆕 {inserted_count} new records inserted.")

    except Exception as e:
        logging.error(f"❌ Error fetching Zoho attendance: {e}")

# ===== MAIN =====
def main():
    configure_logging()
    token = get_access_token()
    from_date = get_last_synced_timestamp()
    fetch_zoho_attendance(token, from_date)
    logging.info("✅ Zoho sync to database complete.")

if __name__ == "__main__":
    main()
