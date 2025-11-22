import os
import zipfile
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import psycopg2
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
TAX_MASTER = "Oracle_Tax_Master_Details"

OUTPUT_FOLDER = r"D:\APInvoice\FBDI\UCMLOAD\Inbound"

# ================== HELPER FUNCTIONS ==================
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

# ================== MAIN PROCESS ==================
if __name__ == "__main__":
    conn = None
    try:
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)

        # ----------------- DATABASE CONNECTION -----------------
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        conn.autocommit = False

        # SQLAlchemy engine for Pandas read_sql
        engine = create_engine(
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        # ----------------- STEP 1: LOAD SUPPLIER MASTER -----------------
        supplier_master_query = f"""
            SELECT DISTINCT supplier_name, supplier_number, vendor_site_code, assigned_bu
            FROM "{SCHEMA}"."{SUPPLIER_MASTER}";
        """
        df_supplier_master = pd.read_sql(supplier_master_query, engine)

        # Bulk update headers with supplier info
        update_query = f"""
            UPDATE "{SCHEMA}"."{TABLE_HEADERS}" AS h
            SET supplier_number = data.supplier_number,
                supplier_site = data.vendor_site_code,
                business_unit = data.assigned_bu
            FROM (VALUES %s) AS data(supplier_name, supplier_number, vendor_site_code, assigned_bu)
            WHERE h.supplier_name = data.supplier_name;
        """
        update_data = [
            (row.supplier_name, row.supplier_number, row.vendor_site_code, row.assigned_bu)
            for row in df_supplier_master.itertuples()
        ]
        execute_values(cursor, update_query, update_data)
        conn.commit()
        print("✔ Bulk updated header table supplier info from master data")

        # ----------------- STEP 2: LOAD HEADER DATA -----------------
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
        query_headers = f"""
            SELECT {', '.join(HEADER_REAL_COLS)}
            FROM "{SCHEMA}"."{TABLE_HEADERS}"
            WHERE status = 'A' 
              AND business_unit <> ''
              AND (erp_interface_status IS NULL OR erp_interface_status = 'Error');
        """
        df_headers = pd.read_sql(query_headers, engine)
        print(f"✅ Header Records Loaded: {len(df_headers)}")

        df_headers["source"] = df_headers["source"].astype(str).str.title()
        df_headers["invoice_type"] = df_headers["invoice_type"].fillna("STANDARD").replace(["", "None"], "STANDARD").str.upper()
        df_headers["payment_terms"] = df_headers["payment_terms"].fillna("Immediate").replace(["", "None"], "Immediate").astype(str)

        # ----------------- STEP 3: LOAD LINE DATA -----------------
        LINES_REAL_COLS = [
            "invoice_id",
            "line_number",
            "line_type",
            "amount",
            "invoice_quantity",
            "unit_price",
            "uom",
            "description",
            "po_number",
            "po_line_number",
            "po_schedule_number",
            "po_distribution_number",
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
            'NULL AS tax_control_amount',
            'NULL AS  tax_regime_code',
            'NULL AS tax',
            'NULL AS tax_status_code',
            'NULL AS tax_jurisdiction_code',
            "tax_rate_code",
            "tax_rate",
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

        query_lines = f"""
            SELECT {', '.join(LINES_REAL_COLS)}
            FROM "{SCHEMA}"."{TABLE_LINES}"
            WHERE invoice_id IN (
                SELECT invoice_id FROM "{SCHEMA}"."{TABLE_HEADERS}"
                WHERE status = 'A' 
                  AND business_unit <> ''
                  AND (erp_interface_status IS NULL OR erp_interface_status = 'Error')
            );
        """
        df_lines = pd.read_sql(query_lines, engine)
        df_lines["line_type"] = df_lines["line_type"].astype(str).str.upper()

        # ----------------- STEP 4: APPLY TAX MASTER (FIXED) -----------------
        tax_lookup_query = f"""
            SELECT APL.invoice_id, APL.line_number, APL.tax_rate, APH.business_unit
            FROM "{SCHEMA}"."{TABLE_LINES}" APL
            JOIN "{SCHEMA}"."{TABLE_HEADERS}" APH
              ON APH.invoice_id = APL.invoice_id
            WHERE APL.line_type = 'TAX';
        """
        df_tax_lookup = pd.read_sql(tax_lookup_query, engine)

        df_tax_master = pd.read_sql(
            f'SELECT DISTINCT tax_regime_code, tax, tax_status_code, tax_rate_code, tax_rate, bu_name FROM "{SCHEMA}"."{TAX_MASTER}"',
            engine
        )

        def normalize_tax_rate(x):
            try:
                if pd.isna(x):
                    return 0.0
                x = float(x)
                if x < 1:
                    x = x * 100
                return round(x, 2)
            except:
                return 0.0

        df_tax_lookup['tax_rate'] = df_tax_lookup['tax_rate'].apply(normalize_tax_rate)
        df_tax_master['tax_rate'] = df_tax_master['tax_rate'].apply(normalize_tax_rate)

        df_tax_merged = pd.merge(
            df_tax_lookup,
            df_tax_master,
            left_on=['tax_rate', 'business_unit'],
            right_on=['tax_rate', 'bu_name'],
            how='left'
        )

        df_tax_merged['tax_regime_code'] = df_tax_merged['tax_regime_code'].fillna('')
        df_tax_merged['tax'] = df_tax_merged['tax'].fillna(0)
        df_tax_merged['tax_status_code'] = df_tax_merged['tax_status_code'].fillna('')
        df_tax_merged['tax_rate_code'] = df_tax_merged['tax_rate_code'].fillna('')
        df_tax_merged['tax_rate'] = df_tax_merged['tax_rate'].fillna(0)

        tax_update_values = [
            (
                str(row.tax_rate_code),
                float(row.tax_rate),
                int(row.invoice_id),
                int(row.line_number)
            )
            for row in df_tax_merged.itertuples()
        ]

        update_sql = f"""
        UPDATE "{SCHEMA}"."{TABLE_LINES}" AS L
        SET 
            tax_rate_code = data.tax_rate_code,
            tax_rate = data.tax_rate
        FROM (VALUES %s) AS data(
            tax_rate_code, tax_rate, invoice_id, line_number
        )
        WHERE L.invoice_id = data.invoice_id::BIGINT
          AND L.line_number = data.line_number::INT;
        """
        execute_values(cursor, update_sql, tax_update_values)
        conn.commit()
        print("✔ Tax master updated (ONLY tax_rate_code + tax_rate)")

        # ----------------- STEP 5: TAX CLASSIFICATION -----------------
        df_lines["tax_classification_code"] = df_lines.apply(
            lambda r: r["tax_classification_code"] if str(r["line_type"]).upper() == "TAX" else "",
            axis=1
        )

        # ----------------- STEP 6: GENERATE FBDI -----------------
        for bu in df_headers['business_unit'].unique():
            print(f"\n🚀 GENERATING FBDI FOR BU: {bu}")
            hdf = df_headers[df_headers['business_unit'] == bu].copy()
            ldf = df_lines[df_lines['invoice_id'].isin(hdf['invoice_id'])].copy()

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            auto_value = f"BDOC_{bu}_{timestamp}"
            hdf["attribute_1_fbdi"] = auto_value
            print(f"✔ attribute_1_fbdi set to → {auto_value}")

            hdf = add_end_column(clean_nulls(hdf))
            ldf = add_end_column(clean_nulls(ldf))

            header_csv = os.path.join(OUTPUT_FOLDER, "ApInvoicesInterface.csv")
            lines_csv = os.path.join(OUTPUT_FOLDER, "ApInvoiceLinesInterface.csv")
            export_to_csv(hdf, header_csv)
            export_to_csv(ldf, lines_csv)

            zip_path = os.path.join(
                OUTPUT_FOLDER,
                f"{bu.replace(' ', '_')}_apinvoiceimport_{timestamp}.zip"
            )
            zip_files(header_csv, lines_csv, zip_path)

            # ----------------- STEP 7: UPDATE HEADER STATUS -----------------
            df_inv = pd.read_csv(header_csv, header=None)
            invoice_numbers = df_inv[3].dropna().astype(str).tolist()
            update_header_sql = f"""
                UPDATE "{SCHEMA}"."{TABLE_HEADERS}"
                SET status = 'P',
                    erp_interface_status = 'Processed',
                    erp_interface_description = NULL
                WHERE invoice_number = ANY(%s);
            """
            cursor.execute(update_header_sql, (invoice_numbers,))
            conn.commit()
            print("✔ Header status updated to P / Processed")

            os.remove(header_csv)
            os.remove(lines_csv)

    except Exception as e:
        print("❌ ERROR:", e)

    finally:
        if conn:
            conn.close()
            print("🔒 Database Closed")
