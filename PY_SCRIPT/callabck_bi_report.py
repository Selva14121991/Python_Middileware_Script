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

# =========================================================
#                   FUSION CONFIG
# =========================================================
FUSION_BASE_URL = "https://fa-euth-dev58-saasfademo1.ds-fa.oraclepdemos.com:443"

REPORT_SERVICE = f"{FUSION_BASE_URL}/xmlpserver/services/ExternalReportWSSService"

USERNAME = "Arun.sp"
PASSWORD = "fusion123"

# BI report path
BI_REPORT_PATH = "/Custom/Master Integration Reports/AP_INVOICE_STATUS_REPORT/AP Interface Rej Det Rpt.xdo"

# =========================================================
#                   DB CONFIG
# =========================================================
DB_CONFIG = {
    "host": "4.188.251.57",
    "port": 5432,
    "database": "aidb",
    "user": "aiuser1",
    "password": "Password#12"
}

SCHEMA = "DocAI"
TABLE_HEADERS = "oracle_ap_invoice_headers"
TABLE_LINES = "oracle_ap_invoice_lines"

# =========================================================
#           Database Connection
# =========================================================
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# =========================================================
#     Fetch all Load Request IDs to update status
# =========================================================
def get_load_request_ids():
    print("🔌 Fetching LOAD_REQUEST_IDs from oracle_ap_invoice_headers …")
    conn = get_db_connection()
    query = f'''
        SELECT DISTINCT load_request_id
        FROM "{SCHEMA}"."{TABLE_HEADERS}"
        WHERE load_request_id IS NOT NULL
          AND (erp_interface_status IS NULL OR erp_interface_status = 'Error' OR  status = 'P')
        ORDER BY 1;
    '''
    df = pd.read_sql_query(query, conn)
    conn.close()
    load_ids = df["load_request_id"].astype(str).tolist()
    print(f"📦 Retrieved {len(load_ids)} Load Request IDs")
    return load_ids

# =========================================================
#               CALL BI REPORT
# =========================================================
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

    if resp.status_code != 200:
        print("❌ BI Report Failed:", resp.text)
        return None

    # Parse XML
    root = ET.fromstring(resp.content)
    ns = {"ns": "http://xmlns.oracle.com/oxp/service/PublicReportService"}

    report_bytes = root.find(".//ns:reportBytes", ns)
    if report_bytes is None or not report_bytes.text:
        print("⚠ No BI data found for this Load Request ID")
        return None

    # Decode CSV
    csv_text = base64.b64decode(report_bytes.text).decode("utf-8-sig")
    rows = list(csv.DictReader(StringIO(csv_text)))

    if not rows:
        print("⚠ CSV returned but empty!")
        return None

    print(f"📄 BI Report returned {len(rows)} rows")
    return rows
# =========================================================
#               BULK UPDATE HEADER AND LINE TABLES
# =========================================================
def update_invoice_bulk(rows):
    conn = get_db_connection()
    cur = conn.cursor()

    # --- Header update (always)
    update_header_sql = f"""
        UPDATE "{SCHEMA}"."{TABLE_HEADERS}"
        SET erp_interface_status = %s,
            status = %s,
            erp_interface_description = %s
        WHERE invoice_number = %s;
    """
    header_values = [
        (r.get("DOC_STATUS"), r.get("STATUS"), r.get("ERROR_DESCRIPTION"), r.get("INVOICE_NUM"))
        for r in rows
    ]
    cur.executemany(update_header_sql, header_values)
    print(f"📝 Bulk Updated Headers: {cur.rowcount}")

    # --- Line update
    for r in rows:
        invoice_id = r.get("INVOICE_ID")
        line_number = r.get("C_LINE_LEVEL")
        if line_number:  # Case 1: line exists
            update_line_sql = f"""
                UPDATE "{SCHEMA}"."{TABLE_LINES}"
                SET erp_interface_status = 'Error',
                    erp_interface_description = %s,
                    erp_interface_desc_details = %s
                WHERE invoice_id = %s
                  AND line_number = %s;
            """
            cur.execute(update_line_sql, (
                r.get("ERROR_DESCRIPTION"),
                r.get("DESCRIPTION"),
                invoice_id,
                line_number
            ))
        else:  # Case 2: no line exists → clear description fields
            update_line_sql = f"""
                UPDATE "{SCHEMA}"."{TABLE_LINES}"
                SET erp_interface_description = NULL,
                    erp_interface_desc_details = NULL,
                    erp_interface_status = NULL
                WHERE invoice_id = %s;
            """
            cur.execute(update_line_sql, (invoice_id,))
    print(f"📝 Lines updated (Error or NULL depending on C_LINE_LEVEL)")

    conn.commit()
    cur.close()
    conn.close()

# =========================================================
#                     MAIN PROCESS
# =========================================================
if __name__ == "__main__":
    load_ids = get_load_request_ids()

    for load_id in load_ids:
        print("\n=======================================")
        print(f"🔄 Processing Load Request ID: {load_id}")
        print("=======================================\n")

        rows = call_bi_report(load_id)
        if rows:
            update_invoice_bulk(rows)
            print(f"\n✅ Updated invoices and lines for LOAD_REQUEST_ID={load_id}")
        else:
            print(f"⚠ No BI rows returned → No updates for LOAD_REQUEST_ID={load_id}")

    print("\n🎉 COMPLETED PROCESSING ALL LOAD REQUEST IDs!")
