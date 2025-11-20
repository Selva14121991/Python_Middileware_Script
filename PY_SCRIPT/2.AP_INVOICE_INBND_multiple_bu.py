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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================== CONFIGURATION ==================
INPUT_FOLDER = r"D:\APInvoice\FBDI\UCMLOAD"
PROCESSED_FOLDER = r"D:\APInvoice\FBDI\UCMLOAD\processed"
ZIP_PREFIX = "apinvoiceimport"

FUSION_BASE_URL = "https://fa-euth-dev58-saasfademo1.ds-fa.oraclepdemos.com:443"

REST_ENDPOINT = f"{FUSION_BASE_URL}/fscmRestApi/resources/11.13.18.05/erpintegrations"
REPORT_SERVICE = f"{FUSION_BASE_URL}/xmlpserver/services/ExternalReportWSSService"

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
    cleaned = base.replace("_apinvoiceimport.zip", "").replace("_", " ")
    return cleaned.strip()


# -------- BI REPORT CALL (CSV Output) ----------
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

    print(f"➡ BU_ID={bu_id}, LEDGER_ID={ledger_id}")
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

    print("Response:", resp.text)

    resp_json = resp.json()
    req_id = resp_json.get("ReqstId") or resp_json.get("RequestId") or resp_json.get("requestId")
    print(f"✅ ESS RequestId: {req_id}")

    return req_id


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

    print(f"🧾 Extracted {len(invoice_numbers)} invoice numbers")
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
    updated_count = cur.rowcount

    cur.close()
    conn.close()

    print(f"📦 Bulk updated load_request_id for {updated_count} invoices")


def move_to_processed(zip_file):
    os.makedirs(PROCESSED_FOLDER, exist_ok=True)
    shutil.move(zip_file, os.path.join(PROCESSED_FOLDER, os.path.basename(zip_file)))
    print("📁 Moved to processed")


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

            move_to_processed(zip_file)

        except Exception as e:
            print(f"❌ ERROR: {e}")
            continue

    print("\n🎉 COMPLETED ALL FILES!")
