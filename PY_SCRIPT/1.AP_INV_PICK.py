import os
import zipfile
import psycopg2
import pandas as pd

# ================== CONFIGURATION ==================
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
SUPPLIER_MASTER = "Oracle_Supplier_Master_Details"

OUTPUT_FOLDER = r"D:\APInvoice\FBDI\UCMLOAD"

# ======================================================================
# ================== REQUIRED COLUMNS (REAL VALUES) ====================
# ======================================================================

# Columns from DB that should retain actual data (headers)
HEADER_REAL_COLS = [
    "invoice_id",
    "business_unit",
    "source",
    "invoice_number",
    "invoice_amount",
    "invoice_date",
    "supplier_name",
    "supplier_number",
    "supplier_site",
    "invoice_currency",
    "payment_currency",
    "description",
    "import_set",
    "invoice_type",
    'NULL AS legal_entity',
    'NULL AS customer_tax_registration_number',
    'NULL AS customer_registration_code',
    'NULL AS first_party_tax_registration_number',
    'NULL AS supplier_tax_registration_number',
    "payment_terms",
    "terms_date",
    'NULL AS goods_received_date',
    'NULL AS invoice_received_date',
    "accounting_date",
    "payment_method",
    "pay_group",
    "pay_alone",
    'NULL AS discountable_amount',
    'NULL AS prepayment_number',
    'NULL AS prepayment_line_number',
    'NULL AS prepayment_application_amount',
    'NULL AS prepayment_accounting_date',
    'NULL AS invoice_includes_prepayment',
    'NULL AS conversion_rate_type',
    'NULL AS conversion_date',
    'NULL AS conversion_rate',
    "liability_combination",
    'NULL AS document_category_code',
    'NULL AS voucher_number',
    'NULL AS delivery_channel_code',
    'NULL AS bank_charge_bearer',
    'NULL AS remit_to_supplier',
    'NULL AS remit_to_supplier_number',
    'NULL AS remit_to_address_name',
    "payment_priority",
    'NULL AS settlement_priority',
    'NULL AS unique_remittance_identifier',
    'NULL AS unique_remittance_identifier_check_digit',
    'NULL AS payment_reason_code',
    'NULL AS payment_reason_comments',
    'NULL AS remittance_message_1',
    'NULL AS remittance_message_2',
    'NULL AS remittance_message_3',
    'NULL AS withholding_tax_group',
    'NULL AS ship_to_location',
    'NULL AS taxation_country',
    'NULL AS document_sub_type',
    'NULL AS tax_invoice_internal_sequence_number',
    'NULL AS supplier_tax_invoice_number',
    'NULL AS tax_invoice_recording_date',
    'NULL AS supplier_tax_invoice_date',
    'NULL AS supplier_tax_invoice_conversion_rate',
    'NULL AS correction_year',
    'NULL AS correction_Period',
    'NULL AS import_document_number',
    'NULL AS import_document_date',
    'NULL AS tax_control_amount',
    "calculate_tax_during_import",
    'NULL AS add_tax_to_invoice_amount',
    'NULL AS attribute_category',
    'NULL AS attribute_1_fbdi',
    'NULL AS attribute_2_fbdi',
    'NULL AS attribute_3_fbdi',
    'NULL AS attribute_4_fbdi',
    'NULL AS attribute_5_fbdi',
    'NULL AS attribute_6_fbdi',
    'NULL AS attribute_7_fbdi',
    'NULL AS attribute_8_fbdi',
    'NULL AS attribute_9_fbdi',
    'NULL AS attribute_10_fbdi',
    'NULL AS attribute_11_fbdi',
    'NULL AS attribute_12_fbdi',
    'NULL AS attribute_13_fbdi',
    'NULL AS attribute_14_fbdi',
    'NULL AS attribute_15_fbdi',
    'NULL AS attribute_number_1',
    'NULL AS attribute_number_2',
    'NULL AS attribute_number_3',
    'NULL AS attribute_number_4',
    'NULL AS attribute_number_5',
    'NULL AS attribute_date_1',
    'NULL AS attribute_date_2',
    'NULL AS attribute_date_3',
    'NULL AS attribute_date_4',
    'NULL AS attribute_date_5',
    'NULL AS global_attribute_category',
    'NULL AS global_attribute_1',
    'NULL AS global_attribute_2',
    'NULL AS global_attribute_3',
    'NULL AS global_attribute_4',
    'NULL AS global_attribute_5',
    'NULL AS global_attribute_6',
    'NULL AS global_attribute_7',
    'NULL AS global_attribute_8',
    'NULL AS global_attribute_9',
    'NULL AS global_attribute_10',
    'NULL AS global_attribute_11',
    'NULL AS global_attribute_12',
    'NULL AS global_attribute_13',
    'NULL AS global_attribute_14',
    'NULL AS global_attribute_15',
    'NULL AS global_attribute_16',
    'NULL AS global_attribute_17',
    'NULL AS global_attribute_18',
    'NULL AS global_attribute_19',
    'NULL AS global_attribute_20',
    'NULL AS global_attribute_number_1',
    'NULL AS global_attribute_number_2',
    'NULL AS global_attribute_number_3',
    'NULL AS global_attribute_number_4',
    'NULL AS global_attribute_number_5',
    'NULL AS global_attribute_date_1',
    'NULL AS global_attribute_date_2',
    'NULL AS global_attribute_date_3',
    'NULL AS global_attribute_date_4',
    'NULL AS global_attribute_date_5',
    'NULL AS url_attachments',
    'NULL AS remit_to_bank_account_number',
    'NULL AS supplier_iban',
    'NULL AS requester_email_address',
    'NULL AS intercompany_crosscharge_flag',
    'NULL AS remit_to_digital_account'
]

# Columns from DB that should retain actual data (lines)
LINES_REAL_COLS = [
    "invoice_id",
    "line_number",
    "line_type",
    "amount",
    "invoice_quantity",
    'NULL AS unit_price',
    'NULL AS uom',
    "description",
    'NULL AS po_number',
    'NULL AS po_line_number',
    'NULL AS po_schedule_number',
    'NULL AS po_distribution_number',
    'NULL AS item_description',
    'NULL AS po_release_number',
    'NULL AS purchasing_category',
    'NULL AS receipt_number',
    'NULL AS receipt_line_number',
    'NULL AS consumption_advice_number',
    'NULL AS consumption_advice_line_number',
    'NULL AS packing_slip',
    'NULL AS final_match',
    'NULL AS distribution_combination',
    'NULL AS distribution_set',
    'NULL AS accounting_date',
    'NULL AS overlay_account_segment',
    'NULL AS overlay_primary_balancing_segment',
    'NULL AS overlay_cost_center_segment',
    "tax_classification_code",
    'NULL AS ship_to_location',
    'NULL AS ship_from_location',
    'NULL AS location_of_final_discharge',
    'NULL AS transaction_business_category',
    'NULL AS product_fiscal_classification',
    'NULL AS intended_use',
    'NULL AS user_defined_fiscal_classification',
    'NULL AS product_type',
    'NULL AS assessable_value',
    'NULL AS product_category',
    "tax_control_amount",
    "tax_regime_code",
    "tax",
    "tax_status_code",
    "tax_jurisdiction_code",
    "tax_rate_code",
    'NULL AS tax_rate',
    'NULL AS withholding_tax_group',
    'NULL AS income_tax_type',
    'NULL AS income_tax_region',
    "prorate_across_all_item_lines",
    "line_group_number",
    'NULL AS cost_factor_name',
    'NULL AS statistical_quantity',
    "track_as_asset",
    'NULL AS asset_book_type_code',
    'NULL AS asset_category_id',
    'NULL AS serial_number',
    'NULL AS manufacturer',
    'NULL AS model_number',
    'NULL AS warranty_number',
    "price_correction_line",
    'NULL AS price_correction_invoice_number',
    'NULL AS price_correction_invoice_line_number',
    'NULL AS attribute_category',
    'NULL AS attribute_1_fbdi',
    'NULL AS attribute_2_fbdi',
    'NULL AS attribute_3_fbdi',
    'NULL AS attribute_4_fbdi',
    'NULL AS attribute_5_fbdi',
    'NULL AS attribute_6_fbdi',
    'NULL AS attribute_7_fbdi',
    'NULL AS attribute_8_fbdi',
    'NULL AS attribute_9_fbdi',
    'NULL AS attribute_10_fbdi',
    'NULL AS attribute_11_fbdi',
    'NULL AS attribute_12_fbdi',
    'NULL AS attribute_13_fbdi',
    'NULL AS attribute_14_fbdi',
    'NULL AS attribute_15_fbdi',
    'NULL AS attribute_number_1',
    'NULL AS attribute_number_2',
    'NULL AS attribute_number_3',
    'NULL AS attribute_number_4',
    'NULL AS attribute_number_5',
    'NULL AS attribute_date_1',
    'NULL AS attribute_date_2',
    'NULL AS attribute_date_3',
    'NULL AS attribute_date_4',
    'NULL AS attribute_date_5',
    'NULL AS global_attribute_category',
    'NULL AS global_attribute_1',
    'NULL AS global_attribute_2',
    'NULL AS global_attribute_3',
    'NULL AS global_attribute_4',
    'NULL AS global_attribute_5',
    'NULL AS global_attribute_6',
    'NULL AS global_attribute_7',
    'NULL AS global_attribute_8',
    'NULL AS global_attribute_9',
    'NULL AS global_attribute_10',
    'NULL AS global_attribute_11',
    'NULL AS global_attribute_12',
    'NULL AS global_attribute_13',
    'NULL AS global_attribute_14',
    'NULL AS global_attribute_15',
    'NULL AS global_attribute_16',
    'NULL AS global_attribute_17',
    'NULL AS global_attribute_18',
    'NULL AS global_attribute_19',
    'NULL AS global_attribute_20',
    'NULL AS global_attribute_number_1',
    'NULL AS global_attribute_number_2',
    'NULL AS global_attribute_number_3',
    'NULL AS global_attribute_number_4',
    'NULL AS global_attribute_number_5',
    'NULL AS global_attribute_date_1',
    'NULL AS global_attribute_date_2',
    'NULL AS global_attribute_date_3',
    'NULL AS global_attribute_date_4',
    'NULL AS global_attribute_date_5',
    'NULL AS expenditure_item_date',
    'NULL AS project_number',
    'NULL AS task_number',
    'NULL AS expenditure_type',
    'NULL AS expenditure_organization',
    'NULL AS fiscal_charge_type',
    'NULL AS project_name',
    'NULL AS task_name'
]
# ======================================================================
# ================== HELPER FUNCTIONS =================================
# ======================================================================

def clean_nulls(df):
    return df.fillna("")

def add_end_column(df):
    df["END"] = "END"
    return df

def export_to_csv(df, path):
    df.to_csv(path, index=False, header=False, encoding="utf-8-sig")
    print(f"📄 Exported CSV → {path}")

def zip_files(file1, file2, zip_path):
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(file1, os.path.basename(file1))
        z.write(file2, os.path.basename(file2))
    print(f"🗜 ZIP Created → {zip_path}")

# ======================================================================
# ================== MAIN PROCESSING ==================================
# ======================================================================

if __name__ == "__main__":
    try:
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # ---------------------------------------------------------
        #  STEP 1 — SUPPLIER MASTER → UPDATE INVOICE HEADERS
        # ---------------------------------------------------------
        print("🔍 Fetching invoices with supplier information...")

        # Fetch distinct suppliers from headers including null supplier_number
        cursor.execute(f"""
            SELECT DISTINCT supplier_number, supplier_name
            FROM "{SCHEMA}"."{TABLE_HEADERS}"
            WHERE status='A'
              AND (erp_interface_status IS NULL OR erp_interface_status = 'Error');
        """)

        supplier_rows = cursor.fetchall()

        print(f"📌 Found {len(supplier_rows)} supplier records to process.\n")

        for sup_num, sup_name in supplier_rows:

            if sup_num and sup_num.strip() != "":
                # ---------------------------------------------------------
                # CASE 1: Supplier number exists → lookup by supplier_number
                # ---------------------------------------------------------
                print(f"👉 Processing supplier_number: {sup_num}")

                cursor.execute(f"""
                    SELECT supplier_name, vendor_site_code, assigned_bu
                    FROM "{SCHEMA}"."{SUPPLIER_MASTER}"
                    WHERE supplier_number = %s
                    LIMIT 1;
                """, (sup_num,))
                data = cursor.fetchone()

            else:
                # ---------------------------------------------------------
                # CASE 2: Supplier number is NULL → lookup by supplier_name
                # ---------------------------------------------------------
                print(f"👉 supplier_number is NULL → using supplier_name: {sup_name}")

                cursor.execute(f"""
                    SELECT supplier_number, supplier_name, vendor_site_code, assigned_bu
                    FROM "{SCHEMA}"."{SUPPLIER_MASTER}"
                    WHERE LOWER(supplier_name) = LOWER(%s)
                    LIMIT 1;
                """, (sup_name,))
                data = cursor.fetchone()

                # Extract the real supplier_number if found
                if data:
                    sup_num, supplier_name, vendor_site_code, assigned_bu = data
                else:
                    print(f"❌ No supplier match found for supplier_name = {sup_name}")
                    continue  # skip this supplier_name

            if data:
                # assign_correct values if the search was by supplier_number
                if len(data) == 3:
                    supplier_name, vendor_site_code, assigned_bu = data

                cursor.execute(f"""
                    UPDATE "{SCHEMA}"."{TABLE_HEADERS}"
                    SET supplier_name = %s,
                        supplier_site = %s,
                        business_unit = %s,
                        supplier_number = %s
                    WHERE (supplier_number = %s OR (supplier_number IS NULL AND supplier_name = %s))
                      AND status = 'A'
                      AND (erp_interface_status IS NULL OR erp_interface_status = 'Error');
                """, (supplier_name, vendor_site_code, assigned_bu, sup_num, sup_num, sup_name))

                conn.commit()
                print(f"✔ Updated header rows for supplier_number={sup_num} supplier_name={sup_name}")

        print("\n✅ Supplier Master → Header Update Completed\n")

        # ---------------------------------------------------------
        # 🔥 STEP 2 — LOAD HEADER DATA
        # ---------------------------------------------------------
        query_headers = f"""
            SELECT {', '.join(HEADER_REAL_COLS)}
            FROM "{SCHEMA}"."{TABLE_HEADERS}"
            WHERE status='A'
              AND business_unit <> ''
              AND (erp_interface_status IS NULL OR erp_interface_status = 'Error');
        """

        df_headers = pd.read_sql(query_headers, conn)
        print(f"✅ Header Records Loaded: {len(df_headers)}")

        # ---------------------------------------------
        # FIX SOURCE COLUMN → Convert to Proper Case
        # ---------------------------------------------
        if "source" in df_headers.columns:
            df_headers["source"] = df_headers["source"].astype(str).str.strip().str.title()
            print("✔ source column normalized to Proper Case (e.g., 'External')")

        # ---------------------------------------------------------
        # FIX: invoice_type → default 'STANDARD' when NULL or empty
        # ---------------------------------------------------------
        if "invoice_type" in df_headers.columns:
            df_headers["invoice_type"] = (
                df_headers["invoice_type"]
                .fillna("STANDARD")  # Replace NULL first
                .replace("", "STANDARD")  # Replace empty string
                .replace("None", "STANDARD")  # Replace string 'None'
                .astype(str)  # Convert AFTER cleaning
                .str.strip()
                .str.upper()  # Oracle FBDI expects uppercase
            )
            print("✔ invoice_type defaulted to STANDARD if NULL/empty")

        # ---------------------------------------------------------
        # FIX: payment_terms → default 'Immediate' when NULL or empty
        # ---------------------------------------------------------
        if "payment_terms" in df_headers.columns:
            df_headers["payment_terms"] = (
                df_headers["payment_terms"]
                .fillna("Immediate")  # FIX 1: replace NULL first
                .replace("", "Immediate")  # FIX 2: replace empty strings
                .replace("None", "Immediate")  # FIX 3: replace Python None as str
                .astype(str)  # convert to string AFTER cleanup
                .str.strip()
            )
            print("✔ payment_terms defaulted to Immediate if NULL/empty")

        # Uppercase invoice_type
        if "invoice_type" in df_headers.columns:
            df_headers["invoice_type"] = df_headers["invoice_type"].astype(str).str.upper()

        # ---------------------------------------------------------
        # 🔥 STEP 3 — LOAD LINE DATA
        # ---------------------------------------------------------
        query_lines = f"""
            SELECT {', '.join(LINES_REAL_COLS)}
            FROM "{SCHEMA}"."{TABLE_LINES}"
            WHERE invoice_id IN (
                SELECT invoice_id FROM "{SCHEMA}"."{TABLE_HEADERS}"
                WHERE status='A'
                  AND business_unit <> ''
                  AND (erp_interface_status IS NULL OR erp_interface_status = 'Error')
            );
        """

        df_lines = pd.read_sql(query_lines, conn)
        print(f"✅ Line Records Loaded: {len(df_lines)}")
        # ---------------------------------------------------------
        # 🔥 REQUIRED FIX: convert line_type → UPPERCASE
        # ---------------------------------------------------------
        if "line_type" in df_lines.columns:
            df_lines["line_type"] = df_lines["line_type"].astype(str).str.upper()
            print("line_type values converted to UPPERCASE")

        # ---------------------------------------------------------
        # 🔥 STEP 4 — SPLIT PER BUSINESS UNIT & GENERATE FBDI
        # ---------------------------------------------------------
        for bu in df_headers['business_unit'].unique():
            print(f"\n🚀 GENERATING FBDI FOR BU: {bu}")

            hdf = df_headers[df_headers['business_unit'] == bu]
            ldf = df_lines[df_lines['invoice_id'].isin(hdf['invoice_id'])]

            hdf = add_end_column(clean_nulls(hdf))
            ldf = add_end_column(clean_nulls(ldf))

            header_csv = os.path.join(OUTPUT_FOLDER, "ApInvoicesInterface.csv")
            lines_csv = os.path.join(OUTPUT_FOLDER, "ApInvoiceLinesInterface.csv")

            export_to_csv(hdf, header_csv)
            export_to_csv(ldf, lines_csv)

            zip_path = os.path.join(OUTPUT_FOLDER, f"{bu.replace(' ', '_')}_apinvoiceimport.zip")
            zip_files(header_csv, lines_csv, zip_path)

            os.remove(header_csv)
            os.remove(lines_csv)

    except Exception as e:
        print("❌ ERROR:", e)

    finally:
        if conn:
            conn.close()
            print("🔒 Database Closed")
