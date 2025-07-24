import os
import requests
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv()

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("zoho_sync.log"), logging.StreamHandler()]
)

def get_access_token():
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

def fetch_employee_ids(token):
    url = f"https://people.{DOMAIN}/people/api/forms/employee/getRecords"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    payload = {"page": 1, "per_page": 200}
    emp_ids = set()

    try:
        res = requests.post(url, headers=headers, json=payload)
        res.raise_for_status()
        data = res.json()
        if data["response"]["status"] != 0:
            logging.error("❌ Error in getRecords response.")
            return set()
        for rec in data["response"]["result"]:
            for group in rec.values():
                for emp in group:
                    eid = emp.get("EmployeeID")
                    if eid:
                        emp_ids.add(eid.strip())
        logging.info(f"👥 Fetched {len(emp_ids)} employee IDs.")
        return emp_ids

    except Exception as e:
        logging.error(f"❌ Failed to fetch employees: {e}")
        return set()

def fetch_unsynced_logs():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT id, name, timestamp, punch_type
        FROM attendance_logs
        WHERE synced=0
        ORDER BY timestamp
    """)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def mark_log_synced(log_id):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("UPDATE attendance_logs SET synced=1 WHERE id=%s", (log_id,))
    conn.commit()
    cursor.close()
    conn.close()

def push_attendance(emp_id, check_time, action, token):
    url = f"https://people.{DOMAIN}/people/api/attendance"
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    payload = {"dateFormat": "dd/MM/yyyy HH:mm:ss", "empId": emp_id}
    formatted = check_time.strftime("%d/%m/%Y %H:%M:%S")
    label = "check-in" if action == "in" else "check-out"
    payload["checkIn" if action=="in" else "checkOut"] = formatted
    
    logging.info(f"📤 Sending {label} for {emp_id} at {formatted}")
    res = requests.post(url, headers=headers, data=payload, timeout=10)
    if res.status_code == 200:
        logging.info(f"✅ {label.capitalize()} logged for {emp_id}.")
        return True
    logging.error(f"❌ Failed {label} for {emp_id}: {res.status_code}, {res.text}")
    return False

def main():
    token = get_access_token()
    valid_ids = fetch_employee_ids(token)
    if not valid_ids:
        logging.error("🚫 No employees fetched; aborting sync.")
        return

    logs = fetch_unsynced_logs()
    if not logs:
        logging.info("ℹ️ No unsynced logs found.")
        return

    for log in logs:
        emp = log["name"]
        if emp not in valid_ids:
            logging.warning(f"🚫 Skipped: {emp} not in Zoho.")
            continue

        action = "in" if log["punch_type"] == 0 else "out"
        if push_attendance(emp, log["timestamp"], action, token):
            mark_log_synced(log["id"])

if __name__ == "__main__":
    main()
