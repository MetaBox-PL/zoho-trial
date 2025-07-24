import os
import requests
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

# ===== LOAD ENVIRONMENT VARIABLES =====
load_dotenv()

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
            logging.FileHandler('zoho_logs.log'),
            logging.StreamHandler()
        ]
    )

# ===== GET ACCESS TOKEN =====
def get_access_token():
    try:
        response = requests.post(
            f"https://accounts.{DOMAIN}/oauth/v2/token",
            data={
                "refresh_token": REFRESH_TOKEN,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type": "refresh_token"
            },
            timeout=10
        )
        response.raise_for_status()
        token = response.json()["access_token"]
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

# ===== GET DEVICE USER ID MAPPING =====
def get_device_user_id(zoho_emp_id):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT zk_user_id FROM user_mapping WHERE zoho_emp_id = %s", (zoho_emp_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] if result else 0
    except Exception as e:
        logging.error(f"❌ Error fetching user mapping for Zoho ID {zoho_emp_id}: {e}")
        return 0

# ===== INSERT LOG INTO DATABASE =====
def insert_log_to_db(user_id, name, timestamp, status):
    punch_type = 0 if status == "Check-In" else 1
    conn = None
    try:
        # Check duplicates in both tables
        exists_in_attendance = log_exists_in_attendance(user_id, timestamp, punch_type)
        exists_in_raw = log_exists_in_raw_zoho(user_id, timestamp, punch_type)

        if exists_in_attendance or exists_in_raw:
            # Skip silently
            return False  # no insertion

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Insert into attendance_logs
        cursor.execute("""
            INSERT INTO attendance_logs (user_id, name, timestamp, punch_type, synced, source)
            VALUES (%s, %s, %s, %s, 1, 'zoho')
        """, (user_id, name, timestamp, punch_type))

        # Insert into raw_zoho_logs
        cursor.execute("""
            INSERT INTO raw_zoho_logs (user_id, name, timestamp, punch_type, source)
            VALUES (%s, %s, %s, %s, 'zoho')
        """, (user_id, name, timestamp, punch_type))

        conn.commit()
        logging.info(f"🟢 Inserted: {name} ({user_id}) - {status} at {timestamp}")
        return True  # inserted
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
            name = emp_id  # Or replace with actual name if available
            user_id = get_device_user_id(emp_id)

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
            logging.info("🆕 0 new records found")
        else:
            logging.info(f"🆕 {inserted_count} new records found")

    except Exception as e:
        logging.error(f"❌ Error fetching Zoho attendance: {e}")

# ===== MAIN =====
def main():
    configure_logging()
    token = get_access_token()
    last_sync = get_last_synced_timestamp()
    fetch_zoho_attendance(token, last_sync)
    logging.info("✅ Zoho sync to database complete.")

if __name__ == "__main__":
    main()
