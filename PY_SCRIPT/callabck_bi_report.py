import os
import base64
import requests
import urllib3
import xml.etree.ElementTree as ET
import csv
from io import StringIO
import psycopg2
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================== FUSION CONFIG ==================
FUSION_BASE_URL = "https://fa-euth-dev58-saasfademo1.ds-fa.oraclepdemos.com:443"

REPORT_SERVICE = f"{FUSION_BASE_URL}/xmlpserver/services/ExternalReportWSSService"

USERNAME = "Arun.sp"
PASSWORD = "fusion123"

# BI report path
BI_REPORT_PATH = "/Custom/Master Integration Reports/AP_INVOICE_STATUS_REPORT/AP Interface Rej Det Rpt.xdo"

# ================== DB CONFIG ==================
DB_CONFIG = {
    "host": "4.188.251.57",
    "port": 5432,
    "database": "aidb",
    "user": "aiuser1",
    "password": "Password#12"
}

SCHEMA = "DocAI"
TABLE_LOAD_REQ_ID = "oracle_ap_invoice_load_req_id_tbl"
TABLE_HEADERS = "oracle_ap_invoice_headers"


# ================== DB CONNECTION ==================
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


# ================== FETCH LOAD REQUEST IDs ==================
def get_load_request_ids():
    print("🔌 Fetching LOAD_REQUEST_IDs from DB …")

    conn = get_db_connection()
    query = f'SELECT DISTINCT "LOAD_REQUEST_ID" FROM "{SCHEMA}"."{TABLE_LOAD_REQ_ID}" ORDER BY 1;'
    df = pd.read_sql_query(query, conn)
    conn.close()

    load_ids = df["LOAD_REQUEST_ID"].astype(str).tolist()
    print(f"📦 Retrieved {len(load_ids)} Load Request IDs")
    return load_ids


# ================== CALL BI REPORT ==================
def call_bi_report(load_request_id):
    print(f"\n📡 Calling BI Report for Load Request ID: {load_request_id}")

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
                  <pub:name>P_Load_Request_ID</pub:name>
                  <pub:values>
                     <pub:item>{load_request_id}</pub:item>
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

    resp = requests.post(
        REPORT_SERVICE,
        data=soap_body,
        headers={"Content-Type": "application/soap+xml"},
        auth=(USERNAME, PASSWORD),
        verify=False,
        timeout=120
    )

    print("🔎 HTTP Status:", resp.status_code)

    if resp.status_code != 200:
        print("❌ BI Report Failed:", resp.text)
        return None

    root = ET.fromstring(resp.content)
    ns = {"ns": "http://xmlns.oracle.com/oxp/service/PublicReportService"}

    report_bytes = root.find(".//ns:reportBytes", ns)
    if report_bytes is None or not report_bytes.text:
        print("⚠ No BI data found for this Load Request ID")
        return None

    csv_text = base64.b64decode(report_bytes.text).decode("utf-8-sig")

    print("\n📄 ==== RAW BI CSV RETURNED ====")
    print(csv_text)
    print("================================")

    rows = list(csv.DictReader(StringIO(csv_text)))
    if not rows:
        print("⚠ CSV returned but empty!")
        return None

    print("\n📌 ==== PARSED BI ROWS ====")
    for idx, row in enumerate(rows, start=1):
        print(f"Row {idx}: {row}")
    print("============================")

    return rows


# ================== UPDATE HEADERS TABLE ==================
def update_invoice_header(result_row):
    invoice_num = result_row.get("INVOICE_NUM")
    status = result_row.get("DOC_STATUS")
    err = result_row.get("ERROR_DESCRIPTION")

    conn = get_db_connection()
    cur = conn.cursor()

    update_sql = f"""
        UPDATE "{SCHEMA}"."{TABLE_HEADERS}"
        SET erp_interface_status = %s,
            erp_interface_description = %s
        WHERE invoice_number = %s;
    """

    cur.execute(update_sql, (status, err, invoice_num))
    updated = cur.rowcount
    conn.commit()
    conn.close()

    print(f"📝 Updated → invoice_number={invoice_num}, STATUS={status}, ERROR={err}, Rows affected={updated}")
    return updated


# ================== DELETE LOAD REQUEST ID ==================
def delete_load_request_id(load_request_id):
    conn = get_db_connection()
    cur = conn.cursor()

    delete_sql = f'DELETE FROM "{SCHEMA}"."{TABLE_LOAD_REQ_ID}" WHERE "LOAD_REQUEST_ID" = %s;'
    cur.execute(delete_sql, (load_request_id,))
    deleted = cur.rowcount
    conn.commit()
    conn.close()

    print(f"🗑 Deleted LOAD_REQUEST_ID {load_request_id} → Rows removed={deleted}")


# ================== MAIN ==================
if __name__ == "__main__":

    load_ids = get_load_request_ids()

    for load_id in load_ids:

        print("\n=======================================")
        print(f"🔄 Processing Load Request ID: {load_id}")
        print("=======================================\n")

        rows = call_bi_report(load_id)

        if rows:
            update_count = 0

            for r in rows:
                update_count += update_invoice_header(r)

            print(f"\n✅ Total {update_count} invoice records updated for LOAD_REQUEST_ID={load_id}")

            delete_load_request_id(load_id)

        else:
            print(f"⚠ No BI rows found → No update/delete for LOAD_REQUEST_ID={load_id}")

    print("\n🎉 COMPLETED PROCESSING ALL LOAD REQUEST IDs!")
