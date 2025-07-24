import subprocess
import logging
import sys

# Use the same Python interpreter running this script
PYTHON_EXECUTABLE = sys.executable

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

scripts = [
    ("insert_log_to_db.py", True),   # Must always run
    ("zoholog_to_db.py", True),      # Must always run
    ("order_table.py", False),       # Optional - skip next if it fails
    ("sync_to_zoho.py", False)
]

for script, must_run in scripts:
    try:
        logging.info(f"Running: {script} with {PYTHON_EXECUTABLE}")
        subprocess.run([PYTHON_EXECUTABLE, script], check=True)
        logging.info(f"Completed: {script}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running {script}: {e}")
        if must_run:
            logging.warning(f"{script} failed but is marked critical. Continuing to next script.")
        else:
            logging.warning(f"Stopping sequence due to error in optional script: {script}")
            break

