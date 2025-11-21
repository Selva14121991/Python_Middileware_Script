import os
import zipfile
import psycopg2
import pandas as pd
from datetime import datetime
from psycopg2.extras import execute_values

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

OUTPUT_FOLDER = r"D:\APInvoice\FBDI\UCMLOAD\Inbound"

# ======================================================================
# ================== REQUIRED COLUMNS (REAL VALUES) ====================
# ======================================================================

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
    'NULL AS Requester_First_Name',
    'NULL AS Requester_Last_Name',
    'NULL AS Requester_Employee_Number',
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
    'NULL AS Port_Of_Entry_Code',
    'NULL AS correction_year',
    'NULL AS correction_Period',
    'NULL AS import_document_number',
    'NULL AS import_document_date',
    'NULL AS tax_control_amount',
    "calculate_tax_during_import",
    'NULL AS add_tax_to_invoice_amount',
    'NULL AS attribute_category',

    # ⭐ THIS FIELD WILL BE AUTO-FILLED IN THE CSV
    "attribute_1_fbdi",

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

        # ================== NEW LOGIC TO BULK UPDATE HEADER BASED ON SUPPLIER MASTER ==================
        supplier_master_query = """
            SELECT distinct supplier_name, supplier_number, vendor_site_code, assigned_bu
            FROM "DocAI"."Oracle_Supplier_Master_Details";
        """
        df_supplier_master = pd.read_sql(supplier_master_query, conn)

        # Prepare the update query with data from supplier master
        update_query = f"""
            UPDATE "{SCHEMA}"."{TABLE_HEADERS}" AS h
            SET
                supplier_number = data.supplier_number,
                supplier_site = data.vendor_site_code,
                business_unit = data.assigned_bu
            FROM (VALUES %s) AS data(supplier_name, supplier_number, vendor_site_code, assigned_bu)
            WHERE h.supplier_name = data.supplier_name;
        """

        # Prepare data for bulk update from dataframe rows
        update_data = [(row.supplier_name, row.supplier_number, row.vendor_site_code, row.assigned_bu)
                       for row in df_supplier_master.itertuples()]

        execute_values(cursor, update_query, update_data)
        conn.commit()
        print(f"✔ Bulk updated header table supplier info from master data")

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

        # FIX source → Proper Case
        df_headers["source"] = df_headers["source"].astype(str).str.title()

        # FIX invoice_type
        df_headers["invoice_type"] = (
            df_headers["invoice_type"]
            .fillna("STANDARD")
            .replace("", "STANDARD")
            .replace("None", "STANDARD")
            .astype(str)
            .str.upper()
        )

        # FIX payment_terms
        df_headers["payment_terms"] = (
            df_headers["payment_terms"]
            .fillna("Immediate")
            .replace("", "Immediate")
            .replace("None", "Immediate")
            .astype(str)
        )

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
        df_lines["line_type"] = df_lines["line_type"].astype(str).str.upper()

        # ---------------------------------------------------------
        # 🔥 STEP 4 — SPLIT PER BUSINESS UNIT & GENERATE FBDI
        # ---------------------------------------------------------
        for bu in df_headers['business_unit'].unique():
            print(f"\n🚀 GENERATING FBDI FOR BU: {bu}")

            hdf = df_headers[df_headers['business_unit'] == bu].copy()
            ldf = df_lines[df_lines['invoice_id'].isin(hdf['invoice_id'])].copy()

            # ⭐ NEW → AUTO-GENERATE attribute_1_fbdi
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            auto_value = f"BDOC_{bu}_{timestamp}"

            hdf["attribute_1_fbdi"] = auto_value
            print(f"✔ attribute_1_fbdi set to → {auto_value}")

            # CLEAN AND ADD END
            hdf = add_end_column(clean_nulls(hdf))
            ldf = add_end_column(clean_nulls(ldf))

            # FILE PATHS
            header_csv = os.path.join(OUTPUT_FOLDER, "ApInvoicesInterface.csv")
            lines_csv = os.path.join(OUTPUT_FOLDER, "ApInvoiceLinesInterface.csv")

            export_to_csv(hdf, header_csv)
            export_to_csv(ldf, lines_csv)

            # ZIP NAME WITH BU + TIMESTAMP
            zip_path = os.path.join(
                OUTPUT_FOLDER,
                f"{bu.replace(' ', '_')}_apinvoiceimport_{timestamp}.zip"
            )

            zip_files(header_csv, lines_csv, zip_path)

            os.remove(header_csv)
            os.remove(lines_csv)

    except Exception as e:
        print("❌ ERROR:", e)

    finally:
        if conn:
            conn.close()
            print("🔒 Database Closed")
