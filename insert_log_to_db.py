from zk import ZK
import logging
from datetime import datetime
import mysql.connector
import os
from dotenv import load_dotenv

# ===== LOAD ENVIRONMENT VARIABLES =====
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASS'),
    'database': os.getenv('DB_NAME')
}

DEVICE_CONFIG = {
    'ip': os.getenv('ZK_IP'),
    'port': int(os.getenv('ZK_PORT')) if os.getenv('ZK_PORT') else None,
    'password': int(os.getenv('ZK_PASSWORD')) if os.getenv('ZK_PASSWORD') else 0
}

# ===== SANITY CHECK =====
for var in ['ZK_IP', 'ZK_PORT', 'ZK_PASSWORD', 'DB_HOST', 'DB_USER', 'DB_PASS', 'DB_NAME']:
    if not os.getenv(var):
        logging.error(f"❌ Environment variable '{var}' is missing.")
        exit(1)

# ===== LOGGING SETUP =====
def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('zk_device_logs.log'),
            logging.StreamHandler()
        ]
    )

# ===== GET LATEST TIMESTAMP FOR DEVICE LOGS =====
def get_latest_device_timestamp():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(timestamp) FROM attendance_logs WHERE source = 'device'")
        result = cursor.fetchone()
        return result[0] if result and result[0] else datetime.min
    except mysql.connector.Error as err:
        logging.error(f"❌ MySQL Error while fetching latest device timestamp: {err}")
        return datetime.min
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ===== GET LAST STATUS FROM DATABASE =====
def get_last_status(user_id):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT punch_type FROM attendance_logs WHERE user_id = %s ORDER BY timestamp DESC LIMIT 1",
            (user_id,)
        )
        result = cursor.fetchone()
        return 'Check-In' if result and result['punch_type'] == 0 else 'Check-Out' if result else None
    except mysql.connector.Error as err:
        logging.error(f"❌ MySQL Error while checking last status: {err}")
        return None
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ===== CHECK IF LOG EXISTS IN attendance_logs =====
def log_exists_in_attendance(user_id, timestamp):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM attendance_logs WHERE user_id = %s AND timestamp = %s",
            (user_id, timestamp)
        )
        count = cursor.fetchone()[0]
        return count > 0
    except mysql.connector.Error as err:
        logging.error(f"❌ MySQL Error checking duplicate attendance log: {err}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ===== CHECK IF LOG EXISTS IN raw_device_logs =====
def log_exists_in_raw(user_id, timestamp):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM raw_device_logs WHERE user_id = %s AND timestamp = %s",
            (user_id, timestamp)
        )
        count = cursor.fetchone()[0]
        return count > 0
    except mysql.connector.Error as err:
        logging.error(f"❌ MySQL Error checking duplicate raw device log: {err}")
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ===== INSERT INTO attendance_logs =====
def insert_attendance_to_db(record):
    conn = None
    try:
        if log_exists_in_attendance(record['user_id'], record['timestamp']):
            return
        if log_exists_in_raw(record['user_id'], record['timestamp']):
            return

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO attendance_logs (user_id, name, timestamp, punch_type, synced, source)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        punch_type = 0 if record['status'] == 'Check-In' else 1
        values = (
            record['user_id'],
            record['name'],
            record['timestamp'],
            punch_type,
            False,
            'device'
        )

        cursor.execute(insert_query, values)
        conn.commit()
        logging.info(f"✅ Inserted attendance_logs: User {record['user_id']} ({record['name']}) {record['status']} at {record['timestamp']}")
    except mysql.connector.Error as err:
        logging.error(f"❌ MySQL Error inserting attendance log: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ===== INSERT INTO raw_device_logs =====
def insert_raw_device_log(record):
    conn = None
    try:
        if log_exists_in_raw(record['user_id'], record['timestamp']):
            return

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO raw_device_logs (user_id, name, timestamp, status, device_ip)
            VALUES (%s, %s, %s, %s, %s)
        """
        values = (
            record['user_id'],
            record['name'],
            record['timestamp'],
            record['status'],
            record['device_ip']
        )

        cursor.execute(insert_query, values)
        conn.commit()
        logging.info(f"🟢 Inserted raw_device_logs: User {record['user_id']} ({record['name']}) {record['status']} at {record['timestamp']}")
    except mysql.connector.Error as err:
        logging.error(f"❌ MySQL Error inserting raw device log: {err}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# ===== GET ATTENDANCE FROM DEVICE AND PROCESS =====
def get_attendance_records(ip, port, password):
    zk = ZK(ip=ip, port=port, password=password, force_udp=True, timeout=5, ommit_ping=False)
    conn = None
    try:
        conn = zk.connect()
        conn.disable_device()
        logging.info("✅ Connected to device")

        attendance = conn.get_attendance()
        users = conn.get_users()
        user_map = {u.user_id: u.name for u in users}

        logging.info(f"📥 Fetched {len(attendance)} attendance records")
        logging.info(f"👤 Fetched {len(users)} users from device")

        latest_timestamp = get_latest_device_timestamp()
        logging.info(f"📌 Filtering logs after: {latest_timestamp}")
        
        filtered_logs = [
            log for log in attendance
            if log.timestamp > latest_timestamp and not log_exists_in_raw(log.user_id, log.timestamp)
        ]

        logging.info(f"🆕 {len(filtered_logs)} new records found")

        filtered_logs.sort(key=lambda x: (x.user_id, x.timestamp))
        formatted_records = []
        status_tracker = {}

        for record in filtered_logs:
            user_id = record.user_id
            timestamp = record.timestamp
            name = user_map.get(user_id, "Unknown")

            if user_id not in status_tracker:
                last_status = get_last_status(user_id)
                current_status = 'Check-Out' if last_status == 'Check-In' else 'Check-In'
            else:
                last_status = status_tracker[user_id]
                current_status = 'Check-Out' if last_status == 'Check-In' else 'Check-In'

            status_tracker[user_id] = current_status

            formatted_records.append({
                'user_id': user_id,
                'name': name,
                'timestamp': timestamp,
                'status': current_status,
                'device_ip': ip
            })

        return formatted_records

    except Exception as e:
        logging.error(f"❌ Error fetching attendance from device: {e}")
        return []
    finally:
        if conn:
            conn.enable_device()
            conn.disconnect()
            logging.info("🔌 Disconnected from device")

# ===== MAIN =====
def main():
    configure_logging()
    records = get_attendance_records(**DEVICE_CONFIG)
    for idx, record in enumerate(records, 1):
        logging.info(
            f"📄 Record {idx}: User {record['user_id']} ({record['name']}) "
            f"{record['status']} at {record['timestamp']}"
        )
        insert_attendance_to_db(record)
        insert_raw_device_log(record)

if __name__ == "__main__":
    main()
