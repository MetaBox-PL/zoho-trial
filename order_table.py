import mysql.connector
from datetime import timedelta
import logging
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS"),
    'database': os.getenv("DB_NAME")
}

def get_zoho_logs():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT user_id, timestamp, punch_type FROM attendance_logs WHERE source = 'zoho'")
    logs = cursor.fetchall()
    cursor.close()
    conn.close()
    return logs

def get_device_logs():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, user_id, timestamp, punch_type, name FROM attendance_logs WHERE source = 'device'")
    logs = cursor.fetchall()
    cursor.close()
    conn.close()
    return logs

def delete_device_log(log_id):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM attendance_logs WHERE id = %s", (log_id,))
    conn.commit()
    cursor.close()
    conn.close()

def punch_type_to_str(punch_type):
    return 'Check-In' if punch_type == 0 else 'Check-Out'

def main():
    logging.basicConfig(level=logging.INFO, format='%(message)s')

    logging.info("🔍 Comparing device vs Zoho logs for cleanup...")

    zoho_logs = get_zoho_logs()
    device_logs = get_device_logs()

    deleted_count = 0

    for d_log in device_logs:
        d_time = d_log['timestamp']
        d_user = d_log['user_id']
        d_type = d_log['punch_type']

        for z_log in zoho_logs:
            if (
                z_log['user_id'] == d_user and
                z_log['punch_type'] == d_type and
                abs((z_log['timestamp'] - d_time).total_seconds()) <= 1800  # within 30 minutes
            ):
                punch_type_str = punch_type_to_str(d_type)
                user_name = d_log.get('name', f'User {d_user}')
                logging.info(f"🗑️ Removing device log: {user_name}, time {d_time} ({punch_type_str}) - conflict with Zoho")
                delete_device_log(d_log['id'])
                deleted_count += 1
                break  # move to next device log once matched

    logging.info(f"✅ Cleanup complete. {deleted_count} device logs removed due to conflict with Zoho entries.")

if __name__ == "__main__":
    main()
