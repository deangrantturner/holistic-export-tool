import streamlit as st
import pandas as pd
import sqlite3
from fpdf import FPDF
from datetime import date
import base64
import io
import re

# --- Database Setup (SQLite) ---
def init_db():
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    # History Table
    c.execute('''CREATE TABLE IF NOT EXISTS invoice_history
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  invoice_number TEXT,
                  date_created TEXT,
                  total_value REAL,
                  pdf_data BLOB,
                  buyer_name TEXT)''')
    
    # Catalog Table V3 (Removed Weight)
    c.execute('''CREATE TABLE IF NOT EXISTS product_catalog_v3
                 (sku TEXT PRIMARY KEY,
                  product_name TEXT,
                  description TEXT,
                  hts_code TEXT,
                  fda_code TEXT)''')
    conn.commit()
    conn.close()

# --- DB: History Functions ---
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

# --- DB: Catalog Functions ---
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
        self.set_y(-25)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 5, "I declare that all information contained in this invoice to be true and correct.", 0, 1, 'C')
        self.cell(0, 5, "These goods are of Canadian origin (CUSMA/USMCA qualified).", 0, 1, 'C')
        self.ln(2)
        self.cell(0, 5, f'Page {self.page_no()}', 0, 0, 'C')

def generate_pdf(doc_type, df, inv_num, inv_date, addr_from, addr_to, addr_ship, notes, total_val):
    pdf = ProInvoice()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=20)
    
    # --- TITLE ---
    pdf.set_font('Helvetica', 'B', 20)
    pdf.cell(0, 10, doc_type, 0, 1, 'C')
    pdf.ln(5)

    # --- INFO BLOCKS ---
    pdf.set_font("Helvetica", '', 9)
    y_top = pdf.get_y()
    
    # BLOCK 1: LEFT
    pdf.set_xy(10, y_top)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_from = "SHIPPER / EXPORTER:" if "COMMERCIAL" in doc_type else "FROM (SELLER):"
    if "PURCHASE" in doc_type: lbl_from = "FROM (BUYER):"
    pdf.cell(65, 5, lbl_from, 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(65, 4, addr_from)
    y_end_1 = pdf.get_y()

    # BLOCK 2: CENTER
    pdf.set_xy(80, y_top)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_to = "CONSIGNEE (SHIP TO):" if "COMMERCIAL" in doc_type else "SHIP TO:"
    pdf.cell(65, 5, lbl_to, 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(65, 4, addr_ship)
    y_end_2 = pdf.get_y()

    # BLOCK 3: RIGHT
    x_right = 150
    pdf.set_xy(x_right, y_top)
    pdf.set_font("Helvetica", 'B', 12)
    pdf.cell(50, 6, f"Ref #: {inv_num}", 0, 0, 'R'); pdf.ln(6)
    pdf.set_xy(x_right, pdf.get_y())
    pdf.set_font("Helvetica", '', 10)
    pdf.cell(50, 6, f"Date: {inv_date}", 0, 0, 'R'); pdf.ln(6)
    pdf.set_xy(x_right, pdf.get_y())
    pdf.cell(50, 6, "Currency: USD", 0, 0, 'R'); pdf.ln(6)
    if "COMMERCIAL" in doc_type:
        pdf.set_xy(x_right, pdf.get_y())
        pdf.cell(50, 6, "Origin: CANADA", 0, 0, 'R'); pdf.ln(6)
    y_end_3 = pdf.get_y()

    # --- ROW 2 ---
    y_mid = max(y_end_1, y_end_2, y_end_3) + 8
    pdf.set_xy(10, y_mid)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_bill = "IMPORTER OF RECORD:" if "COMMERCIAL" in doc_type else "BILL TO:"
    if "PURCHASE" in doc_type: lbl_bill = "TO (VENDOR):"
    pdf.cell(80, 5, lbl_bill, 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(80, 4, addr_to)
    
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
    # Widths: Qty(15), Desc(80), HTS(25), FDA(25), Price(22), Total(23) = 190
    w = [15, 80, 25, 25, 22, 23]
    headers = ["QTY", "DESCRIPTION", "HTS #", "FDA CODE", "UNIT ($)", "TOTAL ($)"]
    pdf.set_font("Helvetica", 'B', 8)
    pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers):
        pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln()
    
    pdf.set_font("Helvetica", '', 8)
    
    for _, row in df.iterrows():
        qty = str(int(row['Quantity']))
        # FIX: The dataframe column is 'Item variant' after renaming, not 'Final Desc'
        desc = str(row['Item variant'])[:60] 
        
        hts = str(row.get('HTS Code', '') or '')
        fda = str(row.get('FDA Code', '') or '')
        price = f"{row['Transfer Price (Unit)']:.2f}"
        tot = f"{row['Transfer Total']:.2f}"
        
        pdf.cell(w[0], 6, qty, 1, 0, 'C')
        pdf.cell(w[1], 6, desc, 1, 0, 'L')
        pdf.cell(w[2], 6, hts, 1, 0, 'C')
        pdf.cell(w[3], 6, fda, 1, 0, 'C')
        pdf.cell(w[4], 6, price, 1, 0, 'R')
        pdf.cell(w[5], 6, tot, 1, 1, 'R')

    pdf.ln(2)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.set_x(10)
    pdf.cell(sum(w[:-1]), 8, "TOTAL VALUE (USD):", 0, 0, 'R')
    pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    
    pdf.ln(15)
    if "COMMERCIAL" in doc_type:
        pdf.cell(100, 5, "Authorized Signature: __________________________", 0, 1)

    return bytes(pdf.output())

# ================= TAB 1: GENERATE DOCUMENTS =================
with tab_generate:
    with st.expander("📝 Invoice Details & Addresses", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            inv_number = st.text_input("Invoice #", value=f"INV-{date.today().strftime('%Y%m%d')}")
            inv_date = st.date_input("Date", value=date.today())
            discount_rate = st.slider("Discount %", 0, 100, 50)
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
            us_shipments = df[df['Ship to country'] == 'United States'].copy()
            if 'Item type' in df.columns:
                us_shipments = us_shipments[us_shipments['Item type'] == 'product']
            
            # --- MERGE LOGIC WITH CATALOG ---
            # 1. Prepare raw sales data
            sales_data = us_shipments[['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit']].copy()
            sales_data['Variant code / SKU'] = sales_data['Variant code / SKU'].astype(str)
            
            # 2. Load Catalog
            catalog = get_catalog()
            
            if not catalog.empty:
                catalog['sku'] = catalog['sku'].astype(str)
                merged = pd.merge(sales_data, catalog, left_on='Variant code / SKU', right_on='sku', how='left')
                
                # 3. Fill values
                merged['Final Desc'] = merged['description'].fillna(merged['product_name']).fillna(merged['Item variant'])
                merged['Final HTS'] = merged['hts_code'].fillna(DEFAULT_HTS)
                merged['Final FDA'] = merged['fda_code'].fillna(DEFAULT_FDA)
                processed = merged.copy()
            else:
                processed = sales_data.copy()
                processed['Final Desc'] = processed['Item variant']
                processed['Final HTS'] = DEFAULT_HTS
                processed['Final FDA'] = DEFAULT_FDA
            
            # 4. Calculations
            processed['Transfer Price (Unit)'] = processed['Price per unit'] * (1 - discount_rate/100.0)
            processed['Transfer Total'] = processed['Quantity'] * processed['Transfer Price (Unit)']
            
            # 5. Consolidate
            consolidated = processed.groupby(['Variant code / SKU', 'Final Desc']).agg({
                'Quantity': 'sum',
                'Final HTS': 'first',
                'Final FDA': 'first',
                'Transfer Price (Unit)': 'mean',
                'Transfer Total': 'sum'
            }).reset_index()
            
            # RENAME FOR DISPLAY (This was causing the bug! Now the PDF generator knows this name)
            consolidated.rename(columns={
                'Final Desc': 'Item variant', 
                'Final HTS': 'HTS Code', 
                'Final FDA': 'FDA Code'
            }, inplace=True)
            
            # 6. Editable Table
            st.info("👇 Review Line Items (Auto-filled from Catalog)")
            edited_df = st.data_editor(
                consolidated,
                column_config={
                    "Transfer Total": st.column_config.NumberColumn("Total $", format="$%.2f")
                },
                num_rows="dynamic"
            )
            total_val = edited_df['Transfer Total'].sum()

            # --- PREVIEW ---
            st.subheader("🖨️ Document Preview & Download")
            d_tab1, d_tab2, d_tab3 = st.tabs(["Commercial Invoice", "Purchase Order", "Sales Invoice"])

            with d_tab1:
                pdf_ci = generate_pdf("COMMERCIAL INVOICE", edited_df, inv_number, inv_date, 
                                      shipper_txt, importer_txt, consignee_txt, notes_txt, total_val)
                col_a, col_b = st.columns([1, 4])
                with col_a:
                    st.download_button("📥 PDF", pdf_ci, file_name=f"CI-{inv_number}.pdf", mime="application/pdf")
                    if st.button("💾 Save", key="s_ci"): 
                        save_invoice_to_db(inv_number, total_val, pdf_ci, importer_txt.split('\n')[0])
                        st.success("Saved!")
                b64_ci = base64.b64encode(pdf_ci).decode('utf-8')
                st.markdown(f'<iframe src="data:application/pdf;base64,{b64_ci}" width="100%" height="600"></iframe>', unsafe_allow_html=True)
            
            with d_tab2:
                pdf_po = generate_pdf("PURCHASE ORDER", edited_df, inv_number, inv_date, 
                                      importer_txt, shipper_txt, consignee_txt, notes_txt, total_val)
                st.download_button("📥 PDF", pdf_po, file_name=f"PO-{inv_number}.pdf", mime="application/pdf")
                b64_po = base64.b64encode(pdf_po).decode('utf-8')
                st.markdown(f'<iframe src="data:application/pdf;base64,{b64_po}" width="100%" height="600"></iframe>', unsafe_allow_html=True)
            
            with d_tab3:
                pdf_si = generate_pdf("SALES INVOICE", edited_df, inv_number, inv_date, 
                                      shipper_txt, importer_txt, consignee_txt, notes_txt, total_val)
                st.download_button("📥 PDF", pdf_si, file_name=f"SI-{inv_number}.pdf", mime="application/pdf")
                b64_si = base64.b64encode(pdf_si).decode('utf-8')
                st.markdown(f'<iframe src="data:application/pdf;base64,{b64_si}" width="100%" height="600"></iframe>', unsafe_allow_html=True)

        except Exception as e:
            st.error(f"Processing Error: {e}")

# ================= TAB 2: PRODUCT CATALOG =================
with tab_catalog:
    st.header("📦 Product Catalog (Master List)")
    st.markdown("Auto-fill Invoice Descriptions, HTS, and FDA codes by SKU.")
    
    col_tools1, col_tools2, col_tools3 = st.columns(3)
    
    # 1. Template
    with col_tools1:
        # Removed Weight from Template
        template_df = pd.DataFrame(columns=['sku', 'product_name', 'description', 'hts_code', 'fda_code'])
        csv_template = template_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Empty Template", csv_template, "catalog_template.csv", "text/csv")
    
    # 2. Download Current
    current_catalog = get_catalog()
    with col_tools2:
        if not current_catalog.empty:
            csv_current = current_catalog.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Current Catalog", csv_current, "full_catalog.csv", "text/csv")
    
    # 3. Clear DB
    with col_tools3:
        if st.button("⚠️ Clear Entire Catalog"):
            clear_catalog()
            st.rerun()

    # Upload
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

    # Editable View
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
