import schedule
import time
import subprocess
import os

# ================== CONFIG ==================
PROJECT_PATH = r"C:\Users\SelvagowthamSelvaman\PycharmProjects\PythonProject2"

SCRIPT_2_MIN = os.path.join(PROJECT_PATH, "apinv_split_bu_rev06.py")
SCRIPT_3_MIN = os.path.join(PROJECT_PATH, "AP_INVOICE_INBND_MULTIPLE_REV04.py")
SCRIPT_5_MIN = os.path.join(PROJECT_PATH, "invoke_bi_report_callback_rev02.py")

# ================== JOB FUNCTIONS ==================
def run_2_min_script():
    print("⏱ Running apinv_split_bu_rev05.py (every 2 minutes)")
    subprocess.Popen(["python", SCRIPT_2_MIN])

def run_3_min_script():
    print("⏱ Running AP_INVOICE_INBND_MULTIPLE_REV04.py (every 3 minutes)")
    subprocess.Popen(["python", SCRIPT_3_MIN])

def run_5_min_script():
    print("⏱ Running invoke_bi_report_callback_rev02.py (every 5 minutes)")
    subprocess.Popen(["python", SCRIPT_5_MIN])

# ================== SCHEDULER ==================
schedule.every(2).minutes.do(run_2_min_script)
schedule.every(3).minutes.do(run_3_min_script)
schedule.every(5).minutes.do(run_5_min_script)

print("🚀 Scheduler started")

while True:
    schedule.run_pending()
    time.sleep(1)
