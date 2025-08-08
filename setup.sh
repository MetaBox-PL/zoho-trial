#!/bin/bash
set -e

REAL_USER=$(whoami)
REAL_HOME=$(eval echo ~$REAL_USER)

REPO_URL="https://github.com/MetaBox-PL/zoho-trial.git"
REPO_NAME="zoho-trial"
VENV_DIR="zk-env"
ENV_FILE=".env"
SCHEMA_FILE="schema.sql"
LOG_FILE="all_logs.log"
SERVICE_NAME="zoho_sync.service"
PYTHON_BIN="$REAL_HOME/$REPO_NAME/$VENV_DIR/bin/python3"
PROJECT_DIR="$REAL_HOME/$REPO_NAME"

echo "📦 Starting full setup..."

# ---------------------------
# 1) Install dependencies
# ---------------------------
echo "🔍 Installing required system packages..."
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip \
  libusb-1.0-0-dev libhidapi-dev nano cron curl wget unzip \
  mariadb-server mariadb-client

sudo systemctl enable cron
sudo systemctl start cron

# ---------------------------
# 2) MariaDB setup
# ---------------------------
echo "⚙️ Setting up MariaDB..."
sudo mkdir -p /etc/mysql/conf.d
sudo mkdir -p /var/run/mysqld
sudo chown mysql:mysql /var/run/mysqld

if [ ! -d "/var/lib/mysql/mysql" ]; then
  sudo mysql_install_db --user=mysql --basedir=/usr --datadir=/var/lib/mysql
fi

sudo systemctl enable mariadb || sudo systemctl enable mysql
sudo systemctl start mariadb || sudo systemctl start mysql

# ---------------------------
# 3) Clone or update repo
# ---------------------------
if [ ! -d "$REPO_NAME" ]; then
  echo "📥 Cloning repo..."
  git clone "$REPO_URL"
else
  echo "📡 Repo exists, pulling latest..."
  cd "$REPO_NAME"
  git pull
  cd ..
fi

cd "$REPO_NAME"

# ---------------------------
# 4) Create .env file
# ---------------------------
echo "🛠 Writing .env file..."
cat > "$ENV_FILE" <<EOF
###ZOHO API###
ZOHO_CLIENT_ID=<id>
ZOHO_CLIENT_SECRET=<secret>
ZOHO_REDIRECT_URI=http://localhost:8080
ZOHO_REFRESH_TOKEN=<refresh_token>
ZOHO_DOMAIN=zoho.com

##ZKTECO DEVICE##
ZK_IP=<device_ip>
ZK_PORT=4370
ZK_PASSWORD=<password>

###DATABASE###
DB_HOST=localhost
DB_PORT=3306
DB_ROOT_PASSWORD=<password>
DB_NAME=<name>
DB_USER=<user>
DB_PASS=<password>

###GOOGLE DRIVE BACKUP###
GDRIVE_FOLDER_ID=1iaFKuYSaUYrEYoivqAhBjB3dnCHU32Vi
GDRIVE_CREDENTIALS_JSON=../client_secret_456700935096-hltqfjul88qu0kcr6vrtef6v2lqe6gqm.apps.googleusercontent.com.json
EOF

echo "✅ .env created."

# ---------------------------
# 5) Setup virtual environment
# ---------------------------
if [ -d "$VENV_DIR" ]; then
  echo "🧹 Removing old virtualenv..."
  rm -rf "$VENV_DIR"
fi

echo "🐍 Creating virtual environment..."
python3 -m venv "$VENV_DIR"

echo "📦 Installing Python packages..."
(
  source "$VENV_DIR/bin/activate"
  pip install --upgrade pip
  pip install mysql-connector-python requests python-dotenv pydrive pyzk
)

# ---------------------------
# 6) Setup DB and tables
# ---------------------------
echo "📂 Creating database and tables..."
source <(grep -v '^#' .env | xargs -d '\n')

sudo mysql -u root <<MYSQL_SCRIPT
CREATE DATABASE IF NOT EXISTS $DB_NAME;
CREATE USER IF NOT EXISTS '$DB_USER'@'localhost' IDENTIFIED BY '$DB_PASS';
GRANT ALL PRIVILEGES ON $DB_NAME.* TO '$DB_USER'@'localhost';
FLUSH PRIVILEGES;
MYSQL_SCRIPT

if [ -f "$SCHEMA_FILE" ]; then
  mysql -h "$DB_HOST" -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$SCHEMA_FILE"
else
  echo "❌ Missing $SCHEMA_FILE"
  exit 1
fi

# ---------------------------
# 7) Trigger Google OAuth (mandatory first time)
# ---------------------------
if [ -f "$PROJECT_DIR/$GDRIVE_CREDENTIALS_JSON" ]; then
  echo "🔐 Running Google Drive OAuth interactively (please follow instructions)..."
  $PYTHON_BIN incremental_backup.py || true
else
  echo "⚠️ Google Drive JSON not found. OAuth will fail unless added later."
fi

# ---------------------------
# 8) Modify run_all.py to wait 10s before running
# ---------------------------
echo "⏳ Adding startup delay to run_all.py..."
sed -i '1s;^;import time\ntime.sleep(10)\n;' run_all.py

# ---------------------------
# 9) Setup systemd service
# ---------------------------
echo "🧩 Setting up systemd service..."
SERVICE_FILE="/etc/systemd/system/$SERVICE_NAME"

sudo bash -c "cat > $SERVICE_FILE" <<EOF
[Unit]
Description=Zoho-ZKTeco Attendance Sync
After=network.target mariadb.service

[Service]
User=$REAL_USER
WorkingDirectory=$PROJECT_DIR
ExecStart=$PYTHON_BIN $PROJECT_DIR/run_all.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable $SERVICE_NAME
sudo systemctl restart $SERVICE_NAME

# ---------------------------
# 10) Setup cron for backup
# ---------------------------
echo "🕒 Adding cron for incremental backup every 2 minutes..."

CRON_JOB="*/2 * * * * cd $PROJECT_DIR && $PYTHON_BIN incremental_backup.py >> $PROJECT_DIR/cron_backup.log 2>&1"

# Only add if not already present
crontab -l 2>/dev/null | grep -q "incremental_backup.py" || (
  (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
)

# ---------------------------
# ✅ DONE
# ---------------------------
echo ""
echo "✅ Full setup completed."
echo "------------------------------------------"
echo "Service: sudo systemctl status $SERVICE_NAME"
echo "Service logs: sudo journalctl -u $SERVICE_NAME -f"
echo "Crontab jobs: crontab -l"
echo "View cron backup logs: tail -f $PROJECT_DIR/cron_backup.log"
echo "Stop service: sudo systemctl stop $SERVICE_NAME"
echo "Disable service: sudo systemctl disable $SERVICE_NAME"
echo "------------------------------------------"
