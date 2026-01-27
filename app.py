import streamlit as st
import pandas as pd
import sqlite3
from fpdf import FPDF
from datetime import date
import io
import re
import tempfile
import os

# --- Database Setup (SQLite) ---
def init_db():
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS invoice_history
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  invoice_number TEXT,
                  date_created TEXT,
                  total_value REAL,
                  pdf_data BLOB,
                  buyer_name TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS product_catalog_v3
                 (sku TEXT PRIMARY KEY,
                  product_name TEXT,
                  description TEXT,
                  hts_code TEXT,
                  fda_code TEXT)''')
    conn.commit()
    conn.close()

# --- DB Functions ---
def save_invoice_to_db(inv_num, total_val, pdf_bytes, buyer):
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    c.execute("INSERT INTO invoice_history (invoice_number, date_created, total_value, pdf_data, buyer_name) VALUES (?, ?, ?, ?, ?)",
              (inv_num, date.today(), total_val, pdf_bytes, buyer))
    conn.commit()
    conn.close()

def get_history():
    conn = sqlite3.connect('invoices.db')
    df = pd.read_sql_query("SELECT invoice_number, date_created, buyer_name, total_value FROM invoice_history ORDER BY id DESC", conn)
    conn.close()
    return df

def get_pdf_from_db(invoice_number):
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    c.execute("SELECT pdf_data FROM invoice_history WHERE invoice_number=?", (invoice_number,))
    data = c.fetchone()
    conn.close()
    return data[0] if data else None

def get_catalog():
    conn = sqlite3.connect('invoices.db')
    df = pd.read_sql_query("SELECT * FROM product_catalog_v3", conn)
    conn.close()
    return df

def upsert_catalog_from_df(df):
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    for _, row in df.iterrows():
        p_name = row.get('product_name', '')
        desc = row.get('description', '')
        if pd.isna(desc) or desc == "":
            desc = p_name
        c.execute("""INSERT OR REPLACE INTO product_catalog_v3 
                     (sku, product_name, description, hts_code, fda_code) 
                     VALUES (?, ?, ?, ?, ?)""",
                  (str(row['sku']), p_name, desc, str(row['hts_code']), str(row['fda_code'])))
    conn.commit()
    conn.close()

def clear_catalog():
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    c.execute("DELETE FROM product_catalog_v3")
    conn.commit()
    conn.close()

init_db()

# --- Page Config ---
st.set_page_config(page_title="Holistic Roasters Export Hub", layout="wide")
st.title("☕ Holistic Roasters Export Hub")

# --- Tabs ---
tab_generate, tab_catalog, tab_history = st.tabs(["📝 Generate Documents", "📦 Product Catalog", "VX Archive History"])

# --- DEFAULT DATA ---
DEFAULT_SHIPPER = """Holistic Roasters inc.
3780 St-Patrick
Montreal, QC, Canada H4E 1A2
BN/GST: 780810917RC0001
TVQ: 1225279701TQ0001"""

DEFAULT_CONSIGNEE = """c/o FedEx Ship Center
1049 US-11
Champlain, NY 12919, United States"""

DEFAULT_IMPORTER = """Holistic Roasters USA
30 N Gould St, STE R
Sheridan, WY 82801
IRS/EIN: 32-082713200"""

DEFAULT_NOTES = """CUSTOMS BROKER: Strix
HOLISTIC ROASTERS inc. Canada FDA #: 11638755492
- ALL PRICES IN USD
- Incoterms: EXW"""

DEFAULT_HTS = "0901.21.00.20"
DEFAULT_FDA = "31ADT01"

# --- PDF CLASS ---
class ProInvoice(FPDF):
    def header(self):
        pass
    def footer(self):
        # REMOVED FOOTER ENTIRELY AS REQUESTED
        pass

def generate_pdf(doc_type, df, inv_num, inv_date, addr_from, addr_to, addr_ship, notes, total_val, signature_file=None):
    pdf = ProInvoice()
    pdf.add_page()
    # Margin set to 20mm to allow content to flow naturally without footer collision
    pdf.set_auto_page_break(auto=True, margin=20)
    
    # --- TITLE ---
    pdf.set_font('Helvetica', 'B', 20)
    pdf.cell(0, 10, doc_type, 0, 1, 'C')
    pdf.ln(5)

    # --- INFO BLOCKS ---
    pdf.set_font("Helvetica", '', 9)
    y_start = pdf.get_y()
    
    # --- COLUMN 1: LEFT ---
    pdf.set_xy(10, y_start)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_from = "SHIPPER / EXPORTER:" if "COMMERCIAL" in doc_type else "FROM (SELLER):"
    if "PURCHASE" in doc_type: lbl_from = "FROM (BUYER):"
    
    pdf.cell(70, 5, lbl_from, 0, 1)
    pdf.set_x(10) 
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(70, 4, addr_from)
    y_end_1 = pdf.get_y()

    # --- COLUMN 2: CENTER ---
    pdf.set_xy(90, y_start) 
    pdf.set_font("Helvetica", 'B', 10)
    lbl_to = "CONSIGNEE (SHIP TO):" if "COMMERCIAL" in doc_type else "SHIP TO:"
    
    pdf.cell(70, 5, lbl_to, 0, 1)
    pdf.set_xy(90, pdf.get_y()) 
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(70, 4, addr_ship)
    y_end_2 = pdf.get_y()

    # --- COLUMN 3: RIGHT ---
    x_right = 160
    pdf.set_xy(x_right, y_start)
    pdf.set_font("Helvetica", 'B', 12)
    pdf.cell(40, 6, f"Ref #: {inv_num}", 0, 1, 'R')
    
    pdf.set_x(x_right)
    pdf.set_font("Helvetica", '', 10)
    pdf.cell(40, 6, f"Date: {inv_date}", 0, 1, 'R')
    
    pdf.set_x(x_right)
    pdf.cell(40, 6, "Currency: USD", 0, 1, 'R')
    
    if "COMMERCIAL" in doc_type:
        pdf.set_x(x_right)
        pdf.cell(40, 6, "Origin: CANADA", 0, 1, 'R')
    
    y_end_3 = pdf.get_y()

    # --- ROW 2: BILL TO / NOTES ---
    y_mid = max(y_end_1, y_end_2, y_end_3) + 10
    
    # Left: Bill To
    pdf.set_xy(10, y_mid)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_bill = "IMPORTER OF RECORD:" if "COMMERCIAL" in doc_type else "BILL TO:"
    if "PURCHASE" in doc_type: lbl_bill = "TO (VENDOR):"
    
    pdf.cell(80, 5, lbl_bill, 0, 1)
    pdf.set_x(10)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(80, 4, addr_to)
    
    # Right: Notes Box
    pdf.set_xy(100, y_mid)
    pdf.set_fill_color(245, 245, 245)
    pdf.rect(100, y_mid, 95, 30, 'F')
    
    pdf.set_xy(102, y_mid + 2)
    pdf.set_font("Helvetica", 'B', 9)
    pdf.cell(50, 5, "NOTES / BROKER / FDA:", 0, 1)
    
    pdf.set_xy(102, pdf.get_y())
    pdf.set_font("Helvetica", '', 8)
    pdf.multi_cell(90, 4, notes)
    
    pdf.set_y(y_mid + 35)

    # --- TABLE ---
    w = [12, 40, 45, 23, 23, 22, 25]
    headers = ["QTY", "PRODUCT", "DESCRIPTION", "HTS #", "FDA CODE", "UNIT ($)", "TOTAL ($)"]
    
    pdf.set_font("Helvetica", 'B', 7)
    pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers):
        pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln()
    
    pdf.set_font("Helvetica", '', 7)
    
    for _, row in df.iterrows():
        qty = str(int(row['Quantity']))
        prod_name = str(row['Product Name'])[:25]
        desc = str(row['Description'])[:45]
        
        hts = str(row.get('HTS Code', '') or '')
        fda = str(row.get('FDA Code', '') or '')
        price = f"{row['Transfer Price (Unit)']:.2f}"
        tot = f"{row['Transfer Total']:.2f}"
        
        pdf.cell(w[0], 6, qty, 1, 0, 'C')
        pdf.cell(w[1], 6, prod_name, 1, 0, 'L')
        pdf.cell(w[2], 6, desc, 1, 0, 'L')
        pdf.cell(w[3], 6, hts, 1, 0, 'C')
        pdf.cell(w[4], 6, fda, 1, 0, 'C')
        pdf.cell(w[5], 6, price, 1, 0, 'R')
        pdf.cell(w[6], 6, tot, 1, 1, 'R')

    pdf.ln(2)
    pdf.set_font("Helvetica", 'B', 9)
    pdf.set_x(10)
    pdf.cell(sum(w[:-1]), 8, "TOTAL VALUE (USD):", 0, 0, 'R')
    pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    
    # --- SIGNATURE BLOCK ---
    pdf.ln(10)
    # Check if we need to start a new page for signature to avoid split
    if pdf.get_y() > 250:
        pdf.add_page()
    
    pdf.set_font("Helvetica", '', 10)
    pdf.cell(0, 5, "I declare that all information contained in this invoice to be true and correct.", 0, 1, 'L')
    pdf.ln(5)
    
    # IMAGE
    if signature_file:
        # Save uploaded file to temp path for FPDF
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            tmp.write(signature_file.getvalue())
            tmp_path = tmp.name
        
        # Place image (Width 40mm)
        try:
            pdf.image(tmp_path, w=40)
        except:
            pdf.cell(0, 5, "[Error rendering signature image]", 0, 1)
        
        # Cleanup temp file
        os.unlink(tmp_path)
    else:
        pdf.ln(15) # Space if no image
    
    # Name
    pdf.ln(2)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(0, 5, "Dean Turner", 0, 1, 'L')

    return bytes(pdf.output())

# ================= TAB 1: GENERATE DOCUMENTS =================
with tab_generate:
    # --- CONFIGURATION EXPANDER ---
    with st.expander("📝 Invoice Details & Addresses", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            inv_number = st.text_input("Invoice #", value=f"INV-{date.today().strftime('%Y%m%d')}")
            inv_date = st.date_input("Date", value=date.today())
            discount_rate = st.number_input("Target Transfer Discount %", min_value=0.0, max_value=100.0, value=50.0, step=0.1, format="%.1f")
            
            # --- SIGNATURE UPLOAD ---
            st.markdown("---")
            st.markdown("**Signature**")
            sig_file = st.file_uploader("Upload Signature (PNG/JPG)", type=['png', 'jpg', 'jpeg'], key="sig_upl")
            
        with c2:
            shipper_txt = st.text_area("Shipper / Exporter", value=DEFAULT_SHIPPER, height=120)
            importer_txt = st.text_area("Importer (Bill To)", value=DEFAULT_IMPORTER, height=100)
        with c3:
            consignee_txt = st.text_area("Consignee (Ship To)", value=DEFAULT_CONSIGNEE, height=120)
            notes_txt = st.text_area("Notes / Broker", value=DEFAULT_NOTES, height=100)

    st.subheader("Upload Orders")
    uploaded_file = st.file_uploader("Upload Daily Orders CSV", type=['csv'])

    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            
            if 'Ship to country' not in df.columns:
                st.error("⚠️ Error: Column 'Ship to country' not found. Did you upload the Product Catalog by mistake?")
            else:
                us_shipments = df[df['Ship to country'] == 'United States'].copy()
                if 'Item type' in df.columns:
                    us_shipments = us_shipments[us_shipments['Item type'] == 'product']
                
                # --- PREPARE DATA ---
                if 'Discount' not in us_shipments.columns:
                    us_shipments['Discount'] = "0%"

                sales_data = us_shipments[['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit', 'Discount']].copy()
                sales_data['Variant code / SKU'] = sales_data['Variant code / SKU'].astype(str)
                
                # --- MERGE WITH CATALOG ---
                catalog = get_catalog()
                if not catalog.empty:
                    catalog['sku'] = catalog['sku'].astype(str)
                    merged = pd.merge(sales_data, catalog, left_on='Variant code / SKU', right_on='sku', how='left')
                    merged['Final Product Name'] = merged['product_name'].fillna(merged['Item variant'])
                    merged['Final Desc'] = merged['description'].fillna(merged['product_name']).fillna(merged['Item variant'])
                    merged['Final HTS'] = merged['hts_code'].fillna(DEFAULT_HTS)
                    merged['Final FDA'] = merged['fda_code'].fillna(DEFAULT_FDA)
                    processed = merged.copy()
                else:
                    processed = sales_data.copy()
                    processed['Final Product Name'] = processed['Item variant']
                    processed['Final Desc'] = processed['Item variant']
                    processed['Final HTS'] = DEFAULT_HTS
                    processed['Final FDA'] = DEFAULT_FDA
                
                # --- MATH ---
                processed['Discount_Float'] = processed['Discount'].astype(str).str.replace('%', '', regex=False)
                processed['Discount_Float'] = pd.to_numeric(processed['Discount_Float'], errors='coerce').fillna(0) / 100.0
                
                processed['Original_Retail'] = processed.apply(
                    lambda row: row['Price per unit'] / (1 - row['Discount_Float']) if row['Discount_Float'] < 1.0 else 0, axis=1
                )
                
                app_discount_decimal = discount_rate / 100.0
                processed['Transfer Price (Unit)'] = processed['Original_Retail'] * (1 - app_discount_decimal)
                processed['Transfer Total'] = processed['Quantity'] * processed['Transfer Price (Unit)']
                
                # --- CONSOLIDATE ---
                consolidated = processed.groupby(['Variant code / SKU', 'Final Product Name', 'Final Desc']).agg({
                    'Quantity': 'sum',
                    'Final HTS': 'first',
                    'Final FDA': 'first',
                    'Transfer Price (Unit)': 'mean',
                    'Transfer Total': 'sum'
                }).reset_index()
                
                consolidated.rename(columns={
                    'Final Product Name': 'Product Name',
                    'Final Desc': 'Description', 
                    'Final HTS': 'HTS Code', 
                    'Final FDA': 'FDA Code'
                }, inplace=True)
                
                # --- EDITABLE TABLE ---
                st.info("👇 Review Line Items")
                edited_df = st.data_editor(
                    consolidated,
                    column_config={
                        "Transfer Total": st.column_config.NumberColumn("Total $", format="$%.2f"),
                        "Transfer Price (Unit)": st.column_config.NumberColumn("Transfer Price", format="$%.2f")
                    },
                    num_rows="dynamic"
                )
                total_val = edited_df['Transfer Total'].sum()

                # --- DOWNLOADS ---
                st.subheader("🖨️ Download Documents")
                col_d1, col_d2, col_d3 = st.columns(3)

                with col_d1:
                    # Pass sig_file to PDF generator
                    pdf_ci = generate_pdf("COMMERCIAL INVOICE", edited_df, inv_number, inv_date, 
                                          shipper_txt, importer_txt, consignee_txt, notes_txt, total_val, sig_file)
                    st.download_button("📄 Commercial Invoice", pdf_ci, file_name=f"CI-{inv_number}.pdf", mime="application/pdf")
                    if st.button("💾 Save to History", key="save_ci"):
                        save_invoice_to_db(inv_number, total_val, pdf_ci, importer_txt.split('\n')[0])
                        st.success("Saved!")

                with col_d2:
                    pdf_po = generate_pdf("PURCHASE ORDER", edited_df, inv_number, inv_date, 
                                          importer_txt, shipper_txt, consignee_txt, notes_txt, total_val, sig_file)
                    st.download_button("📄 Purchase Order", pdf_po, file_name=f"PO-{inv_number}.pdf", mime="application/pdf")

                with col_d3:
                    pdf_si = generate_pdf("SALES INVOICE", edited_df, inv_number, inv_date, 
                                          shipper_txt, importer_txt, consignee_txt, notes_txt, total_val, sig_file)
                    st.download_button("📄 Sales Invoice", pdf_si, file_name=f"SI-{inv_number}.pdf", mime="application/pdf")

        except Exception as e:
            st.error(f"Processing Error: {e}")

# ================= TAB 2: PRODUCT CATALOG =================
with tab_catalog:
    st.header("📦 Product Catalog")
    st.markdown("Auto-fill details by SKU.")
    
    col_tools1, col_tools2, col_tools3 = st.columns(3)
    
    with col_tools1:
        template_df = pd.DataFrame(columns=['sku', 'product_name', 'description', 'hts_code', 'fda_code'])
        csv_template = template_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Empty Template", csv_template, "catalog_template.csv", "text/csv")
    
    with col_tools2:
        current_catalog = get_catalog()
        if not current_catalog.empty:
            csv_current = current_catalog.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Current Catalog", csv_current, "full_catalog.csv", "text/csv")
    
    with col_tools3:
        if st.button("⚠️ Clear Entire Catalog"):
            clear_catalog()
            st.rerun()

    st.subheader("Upload Catalog (CSV)")
    cat_upload = st.file_uploader("Upload filled template to update catalog", type=['csv'])
    if cat_upload:
        try:
            new_cat_df = pd.read_csv(cat_upload)
            req_cols = ['sku', 'product_name', 'description', 'hts_code', 'fda_code']
            if all(col in new_cat_df.columns for col in req_cols):
                upsert_catalog_from_df(new_cat_df)
                st.success("Catalog Updated Successfully!")
                st.rerun()
            else:
                st.error(f"CSV must have columns: {req_cols}")
        except Exception as e:
            st.error(f"Error: {e}")

    st.subheader("Edit Catalog Manually")
    if not current_catalog.empty:
        edited_catalog = st.data_editor(current_catalog, num_rows="dynamic", key="cat_editor")
        if st.button("💾 Save Changes to Catalog"):
            upsert_catalog_from_df(edited_catalog)
            st.success("Changes saved!")
    else:
        st.info("Catalog is empty. Upload a CSV or add rows manually if enabled.")

# ================= TAB 3: HISTORY =================
with tab_history:
    st.header("🗄️ Invoice Archive")
    history_df = get_history()
    st.dataframe(history_df, use_container_width=True)
    inv_list = history_df['invoice_number'].unique()
    if len(inv_list) > 0:
        sel_inv = st.selectbox("Select Invoice:", inv_list)
        if st.button("Fetch PDF"):
            pdf_data = get_pdf_from_db(sel_inv)
            if pdf_data:
                st.download_button("Download", pdf_data, file_name=f"{sel_inv}.pdf", mime="application/pdf")
