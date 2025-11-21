import os
import glob
import base64
import shutil
import requests
import xml.etree.ElementTree as ET
import urllib3
import csv
from io import StringIO
import psycopg2
import zipfile
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================== CONFIGURATION ==================
INPUT_FOLDER = r"D:\APInvoice\FBDI\UCMLOAD\Inbound"
PROCESSED_FOLDER = r"D:\APInvoice\FBDI\UCMLOAD\Archive"
ERROR_FOLDER = r"D:\APInvoice\FBDI\UCMLOAD\Error"

ZIP_PREFIX = "apinvoiceimport"

FUSION_BASE_URL = "https://fa-euth-dev58-saasfademo1.ds-fa.oraclepdemos.com:443"

REST_ENDPOINT = f"{FUSION_BASE_URL}/fscmRestApi/resources/11.13.18.05/erpintegrations"
REPORT_SERVICE = f"{FUSION_BASE_URL}/xmlpserver/services/ExternalReportWSSService"

ESS_STATUS_ENDPOINT = f"{FUSION_BASE_URL}/fscmRestApi/resources/11.13.18.05/erpintegrations"

BI_REPORT_PATH = "/Custom/Master Integration Reports/BU Det/BU Det Rpt.xdo"

USERNAME = "Arun.sp"
PASSWORD = "fusion123"

DOCUMENT_ACCOUNT = "fin$/payables$/import$"
JOB_NAME = "oracle/apps/ess/financials/payables/invoices/transactions,APXIIMPT"
CALLBACK_URL = "Y"
NOTIFICATION_CODE = "10"
JOB_OPTIONS = "InterfaceDetails=1,ImportOption=Y,PurgeOption=N,ExtractFileType=ALL"

# ===== DATABASE CONFIG =====
DB_CONFIG = {
    "host": "4.188.251.57",
    "port": 5432,
    "database": "aidb",
    "user": "aiuser1",
    "password": "Password#12"
}

SCHEMA = "DocAI"

# ================== FUNCTIONS ==================

def get_zip_files():
    pattern = os.path.join(INPUT_FOLDER, f"*{ZIP_PREFIX}*.zip")
    zip_files = glob.glob(pattern)
    zip_files.sort(key=os.path.getmtime)
    return zip_files


def extract_bu_name(zip_file):
    base = os.path.basename(zip_file)
    parts = base.split("_apinvoiceimport", 1)
    bu_raw = parts[0]
    bu_name = bu_raw.replace("_", " ").strip()
    return bu_name


# -------- BI REPORT CALL ----------
def call_bi_report(bu_name):
    print(f"📡 Calling BI Report for BU: {bu_name}")

    soap_body = f"""<?xml version="1.0" encoding="UTF-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
               xmlns:pub="http://xmlns.oracle.com/oxp/service/PublicReportService">
   <soap:Header/>
   <soap:Body>
      <pub:runReport>
         <pub:reportRequest>
            <pub:reportAbsolutePath>{BI_REPORT_PATH}</pub:reportAbsolutePath>
            <pub:parameterNameValues>
               <pub:item>
                  <pub:name>P_BU</pub:name>
                  <pub:values>
                     <pub:item>{bu_name}</pub:item>
                  </pub:values>
               </pub:item>
            </pub:parameterNameValues>
            <pub:sizeOfDataChunkDownload>-1</pub:sizeOfDataChunkDownload>
            <pub:attributeFormat>csv</pub:attributeFormat>
         </pub:reportRequest>
      </pub:runReport>
   </soap:Body>
</soap:Envelope>
"""

    headers = {"Content-Type": "application/soap+xml; charset=utf-8"}
    resp = requests.post(REPORT_SERVICE, data=soap_body, headers=headers,
                         auth=(USERNAME, PASSWORD), verify=False, timeout=120)

    if resp.status_code != 200:
        raise Exception(f"BI Report Error: {resp.text}")

    root = ET.fromstring(resp.content)
    ns = {"ns": "http://xmlns.oracle.com/oxp/service/PublicReportService"}
    report_bytes = root.find(".//ns:reportBytes", ns)

    csv_text = base64.b64decode(report_bytes.text).decode("utf-8-sig")
    rows = list(csv.DictReader(StringIO(csv_text)))

    if not rows:
        raise Exception("BI Report returned no data")

    bu_id = rows[0]["BU_ID"].strip()
    ledger_id = rows[0]["BU_LEDGER_ID"].strip()

    return bu_id, ledger_id


def generate_parameter_list(bu_id, ledger_id):
    return f"#NULL,{bu_id},N,#NULL,#NULL,#NULL,1000,External,#NULL,N,N,{ledger_id},#NULL,1"


def encode_file(file_path):
    with open(file_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def upload_to_fusion(zip_file, parameter_list):
    print(f"🚀 Uploading {os.path.basename(zip_file)}")

    payload = {
        "OperationName": "importBulkData",
        "DocumentContent": encode_file(zip_file),
        "ContentType": "zip",
        "FileName": os.path.basename(zip_file),
        "DocumentAccount": DOCUMENT_ACCOUNT,
        "JobName": JOB_NAME,
        "ParameterList": parameter_list,
        "CallbackURL": CALLBACK_URL,
        "NotificationCode": NOTIFICATION_CODE,
        "JobOptions": JOB_OPTIONS
    }

    resp = requests.post(REST_ENDPOINT, json=payload,
                         auth=(USERNAME, PASSWORD),
                         headers={"Content-Type": "application/json"},
                         verify=False)

    resp_json = resp.json()
    req_id = resp_json.get("ReqstId") or resp_json.get("RequestId") or resp_json.get("requestId")
    return req_id


# -------- ESS JOB STATUS CHECK --------
def wait_for_ess_status(req_id):
    print(f"⏳ Checking ESS Job Status for Request ID: {req_id}")

    while True:
        url = f"{ESS_STATUS_ENDPOINT}?finder=ESSJobStatusRF;requestId={req_id}"
        headers = {"Content-Type": "application/vnd.oracle.adf.resourceitem+json"}

        resp = requests.get(url, headers=headers,
                            auth=(USERNAME, PASSWORD), verify=False)

        if resp.status_code != 200:
            print("⚠ ESS Status API Error:", resp.text)
            time.sleep(10)
            continue

        data = resp.json()
        status = data["items"][0]["RequestStatus"]

        print(f"➡ Current ESS Status: {status}")

        if status == "SUCCEEDED":
            return "SUCCESS"

        if status in ("ERROR", "ERROR_IN_JOB", "ERROR_IN_CHILD_JOB"):
            return "ERROR"

        if status == "WARNING":
            return "ERROR"  # treat warning as failure per requirement

        time.sleep(10)


# -------- Extract Invoice Numbers from ZIP --------
def extract_invoice_numbers(zip_file):
    invoice_numbers = []

    with zipfile.ZipFile(zip_file, "r") as z:
        for file in z.namelist():
            if "ApInvoicesInterface.csv" in file:
                content = z.read(file).decode("utf-8-sig")
                reader = csv.reader(StringIO(content))

                for row in reader:
                    if len(row) > 3:
                        invoice_numbers.append(row[3])

    return invoice_numbers


# -------- BULK UPDATE load_request_id --------
def insert_request_ids(req_id, invoice_numbers):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    update_sql = f'''
        UPDATE "{SCHEMA}"."oracle_ap_invoice_headers"
        SET load_request_id = %s
        WHERE invoice_number = %s;
    '''

    batch_values = [(req_id, inv) for inv in invoice_numbers if inv and inv.strip()]

    cur.executemany(update_sql, batch_values)
    conn.commit()

    cur.close()
    conn.close()


# -------- BULK UPDATE status + erp_interface_status --------
def update_invoice_status(invoice_numbers, status_code, interface_status):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    update_sql = f'''
        UPDATE "{SCHEMA}"."oracle_ap_invoice_headers"
        SET status = %s,
            erp_interface_status = %s
        WHERE invoice_number = %s;
    '''

    batch_values = [(status_code, interface_status, inv)
                    for inv in invoice_numbers if inv and inv.strip()]

    cur.executemany(update_sql, batch_values)
    conn.commit()

    cur.close()
    conn.close()

    print(f"🔄 Updated invoices → {status_code} / {interface_status}")


def move_file(zip_file, status):
    dest = PROCESSED_FOLDER if status == "SUCCESS" else ERROR_FOLDER
    os.makedirs(dest, exist_ok=True)
    shutil.move(zip_file, os.path.join(dest, os.path.basename(zip_file)))
    print(f"📁 File moved to {dest}")


# ================== MAIN LOOP ==================
if __name__ == "__main__":
    zip_files = get_zip_files()
    if not zip_files:
        print("No files to process")
        exit()

    for zip_file in zip_files:
        try:
            print(f"\n========= Processing {zip_file} =========")

            bu_name = extract_bu_name(zip_file)
            bu_id, ledger_id = call_bi_report(bu_name)

            params = generate_parameter_list(bu_id, ledger_id)
            req_id = upload_to_fusion(zip_file, params)

            invoice_numbers = extract_invoice_numbers(zip_file)
            insert_request_ids(req_id, invoice_numbers)

            ess_status = wait_for_ess_status(req_id)

            # ⭐ NEW REQUIRED LOGIC ⭐
            if ess_status == "SUCCESS":
                update_invoice_status(invoice_numbers, "P", "Processed")
            else:
                update_invoice_status(invoice_numbers, "IE", "Interface Loader Error")

            move_file(zip_file, ess_status)

        except Exception as e:
            print(f"❌ ERROR: {e}")
            continue

    print("\n🎉 COMPLETED ALL FILES!")
