import streamlit as st
import pandas as pd
import sqlite3
from fpdf import FPDF
from datetime import datetime, date
import io
import re
import tempfile
import os
import pytz
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import math

# --- Database Setup (SQLite) ---
def init_db():
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS invoice_history_v3
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  invoice_number TEXT,
                  date_created TEXT,
                  total_value REAL,
                  buyer_name TEXT)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS product_catalog_v3
                 (sku TEXT PRIMARY KEY,
                  product_name TEXT,
                  description TEXT,
                  hts_code TEXT,
                  fda_code TEXT)''')
                  
    c.execute('''CREATE TABLE IF NOT EXISTS settings
                 (key TEXT PRIMARY KEY,
                  value BLOB)''')
    conn.commit()
    conn.close()

# --- DB Functions ---
def save_invoice_metadata(inv_num, total_val, buyer):
    try:
        conn = sqlite3.connect('invoices.db')
        c = conn.cursor()
        
        c.execute("SELECT invoice_number FROM invoice_history_v3 WHERE invoice_number LIKE ?", (inv_num + '%',))
        existing_nums = [row[0] for row in c.fetchall()]
        
        count = 0
        for num in existing_nums:
            if num == inv_num: count += 1
            elif num.startswith(inv_num + "-"): count += 1
        
        new_version_num = inv_num if count == 0 else f"{inv_num}-{count}"
        
        est = pytz.timezone('US/Eastern')
        timestamp = datetime.now(est).strftime("%Y-%m-%d %H:%M EST")
        
        c.execute("""INSERT INTO invoice_history_v3 
                     (invoice_number, date_created, total_value, buyer_name) 
                     VALUES (?, ?, ?, ?)""",
                  (new_version_num, timestamp, total_val, buyer))
        conn.commit()
        conn.close()
        return new_version_num
    except Exception:
        return inv_num

def get_history():
    conn = sqlite3.connect('invoices.db')
    df = pd.read_sql_query("SELECT invoice_number, date_created, buyer_name, total_value FROM invoice_history_v3 ORDER BY id DESC", conn)
    conn.close()
    return df

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

# --- Settings & Signature Functions (FIXED) ---
def save_setting(key, value_bytes):
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value_bytes))
    conn.commit()
    conn.close()

def get_setting(key):
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key=?", (key,))
    data = c.fetchone()
    conn.close()
    return data[0] if data else None

# Wrapper functions to maintain compatibility with existing calls
def save_signature(image_bytes):
    save_setting('signature', image_bytes)

def get_signature():
    return get_setting('signature')

# --- Email Function ---
def send_email_with_attachments(sender_email, sender_password, recipient_email, subject, body, files):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    for f in files:
        part = MIMEApplication(f['data'], Name=f['name'])
        part['Content-Disposition'] = f'attachment; filename="{f["name"]}"'
        msg.attach(part)

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, recipient_email, msg.as_string())
        server.quit()
        return True, "Email sent successfully!"
    except Exception as e:
        return False, str(e)

init_db()

# --- Page Config ---
st.set_page_config(page_title="Holistic Roasters Export Hub", layout="wide")

# --- CUSTOM BRANDING CSS ---
st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&family=Open+Sans:wght@400;600&display=swap');

        /* Main App Background */
        .stApp {
            background-color: #FAFAFA;
            font-family: 'Open Sans', sans-serif;
        }

        /* --- STICKY TABS FIX --- */
        div[data-testid="stTabs"] > div:first-child {
            position: sticky !important;
            top: 0px !important;
            z-index: 99999 !important;
            background-color: #FAFAFA !important;
            padding-top: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #E0E0E0;
            box-shadow: 0px 4px 6px rgba(0,0,0,0.05);
        }

        /* Headers */
        h1, h2, h3 {
            font-family: 'Montserrat', sans-serif !important;
            color: #6F4E37 !important;
            font-weight: 700;
        }

        /* Buttons (Primary) */
        div.stButton > button {
            background-color: #6F4E37 !important;
            color: white !important;
            border-radius: 8px !important;
            border: none !important;
            font-family: 'Montserrat', sans-serif !important;
            font-weight: 600 !important;
        }
        div.stButton > button:hover {
            background-color: #5A3E2B !important;
        }

        /* Inputs */
        .stTextInput input, .stTextArea textarea, .stDateInput input, .stNumberInput input {
            border-radius: 8px !important;
            border: 1px solid #D0D0D0 !important;
        }
        .stTextInput input:focus, .stTextArea textarea:focus {
            border-color: #6F4E37 !important;
            box-shadow: 0 0 0 1px #6F4E37 !important;
        }
    </style>
""", unsafe_allow_html=True)

st.title("☕ Holistic Roasters Export Hub")

tab_generate, tab_catalog, tab_history = st.tabs(["📝 Generate Documents", "📦 Product Catalog", "🗄️ Documents Archive"])

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

class ProInvoice(FPDF):
    def header(self):
        pass
    def footer(self):
        pass

def generate_pdf(doc_type, df, inv_num, inv_date, addr_from, addr_to, addr_ship, notes, total_val, sig_bytes=None, signer_name="Dean Turner"):
    pdf = ProInvoice()
    pdf.add_page()
    pdf.set_auto_page_break(auto=False)
    
    # --- HEADER ---
    pdf.set_font('Helvetica', 'B', 20)
    pdf.cell(0, 10, doc_type, 0, 1, 'C')
    pdf.ln(5)

    # --- INFO BLOCKS ---
    pdf.set_font("Helvetica", '', 9)
    y_start = pdf.get_y()
    
    # Column 1
    pdf.set_xy(10, y_start)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_from = "SHIPPER / EXPORTER:" if "COMMERCIAL" in doc_type else "FROM (SELLER):"
    if "PURCHASE" in doc_type: lbl_from = "FROM (BUYER):"
    pdf.cell(70, 5, lbl_from, 0, 1)
    pdf.set_x(10) 
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(70, 4, addr_from)
    y_end_1 = pdf.get_y()

    # Column 2
    pdf.set_xy(90, y_start) 
    pdf.set_font("Helvetica", 'B', 10)
    lbl_to = "CONSIGNEE (SHIP TO):" if "COMMERCIAL" in doc_type else "SHIP TO:"
    pdf.cell(70, 5, lbl_to, 0, 1)
    pdf.set_xy(90, pdf.get_y()) 
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(70, 4, addr_ship)
    y_end_2 = pdf.get_y()

    # Column 3
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

    # Row 2
    y_mid = max(y_end_1, y_end_2, y_end_3) + 10
    
    pdf.set_xy(10, y_mid)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_bill = "IMPORTER OF RECORD:" if "COMMERCIAL" in doc_type else "BILL TO:"
    if "PURCHASE" in doc_type: lbl_bill = "TO (VENDOR):"
    pdf.cell(80, 5, lbl_bill, 0, 1)
    pdf.set_x(10)
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

    # --- TABLE HEADERS ---
    w = [12, 40, 45, 23, 23, 22, 25]
    headers = ["QTY", "PRODUCT", "DESCRIPTION", "HTS #", "FDA CODE", "UNIT ($)", "TOTAL ($)"]
    
    pdf.set_font("Helvetica", 'B', 7)
    pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers):
        pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln()
    
    # --- DYNAMIC TABLE ROWS ---
    pdf.set_font("Helvetica", '', 7)
    line_h = 5

    for _, row in df.iterrows():
        qty = str(int(row['Quantity']))
        prod_name = str(row['Product Name'])
        desc = str(row['Description'])
        hts = str(row.get('HTS Code', '') or '')
        fda = str(row.get('FDA Code', '') or '')
        price = f"{row['Transfer Price (Unit)']:.2f}"
        tot = f"{row['Transfer Total']:.2f}"
        
        data_row = [
            (qty, 'C'), (prod_name, 'L'), (desc, 'L'),
            (hts, 'C'), (fda, 'C'), (price, 'R'), (tot, 'R')
        ]
        
        max_lines = 1
        for i, (txt, align) in enumerate(data_row):
            col_w = w[i]
            txt_w = pdf.get_string_width(txt)
            avail_w = col_w - 2
            lines = math.ceil(txt_w / avail_w)
            if lines < 1: lines = 1
            lines += txt.count('\n')
            if lines > max_lines: max_lines = lines
            
        row_h = max_lines * line_h
        
        if pdf.get_y() + row_h > 270:
            pdf.add_page()
            pdf.set_font("Helvetica", 'B', 7)
            pdf.set_fill_color(220, 220, 220)
            for i, h in enumerate(headers):
                pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
            pdf.ln()
            pdf.set_font("Helvetica", '', 7)
            
        y_start = pdf.get_y()
        x_start = 10
        
        for i, (txt, align) in enumerate(data_row):
            current_x = x_start + sum(w[:i])
            pdf.set_xy(current_x, y_start)
            pdf.multi_cell(w[i], line_h, txt, 0, align)
            
        pdf.set_xy(x_start, y_start)
        for i in range(len(w)):
            current_x = x_start + sum(w[:i])
            pdf.rect(current_x, y_start, w[i], row_h)
            
        pdf.set_xy(x_start, y_start + row_h)

    pdf.ln(2)
    pdf.set_font("Helvetica", 'B', 9)
    pdf.set_x(10)
    pdf.cell(sum(w[:-1]), 8, "TOTAL VALUE (USD):", 0, 0, 'R')
    pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    
    # --- SIGNATURE BLOCK ---
    pdf.ln(10)
    if pdf.get_y() > 250: pdf.add_page()
    
    pdf.set_font("Helvetica", '', 10)
    pdf.cell(0, 5, "I declare that all information contained in this invoice to be true and correct.", 0, 1, 'L')
    pdf.ln(5)
    
    if sig_bytes:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            tmp.write(sig_bytes)
            tmp_path = tmp.name
        try:
            pdf.image(tmp_path, w=40)
        except:
            pdf.cell(0, 5, "[Signature Error]", 0, 1)
        os.unlink(tmp_path)
    else:
        pdf.ln(15)
    
    pdf.ln(2)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(0, 5, signer_name, 0, 1, 'L')

    return bytes(pdf.output())

# ================= TAB 1: GENERATE DOCUMENTS =================
with tab_generate:
    with st.expander("📝 Invoice Details, Addresses & Signature", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            inv_number = st.text_input("Invoice #", value=f"INV-{date.today().strftime('%Y%m%d')}")
            inv_date = st.date_input("Date", value=date.today())
            discount_rate = st.number_input("Target Transfer Discount %", min_value=0.0, max_value=100.0, value=50.0, step=0.1, format="%.1f")
            
        with c2:
            shipper_txt = st.text_area("Shipper / Exporter", value=DEFAULT_SHIPPER, height=120)
            importer_txt = st.text_area("Importer (Bill To)", value=DEFAULT_IMPORTER, height=100)
        with c3:
            consignee_txt = st.text_area("Consignee (Ship To)", value=DEFAULT_CONSIGNEE, height=120)
            notes_txt = st.text_area("Notes / Broker", value=DEFAULT_NOTES, height=100)

        st.markdown("---")
        st.markdown("#### Signature Settings")
        sig_col_a, sig_col_b = st.columns(2)
        with sig_col_a:
            signer_name = st.text_input("Signatory Name", value="Dean Turner")
            saved_sig_bytes = get_signature()
            if saved_sig_bytes:
                st.success("✅ Signature on file")
                if st.button("Clear Signature"):
                    conn = sqlite3.connect('invoices.db')
                    conn.execute("DELETE FROM settings WHERE key='signature'")
                    conn.commit()
                    conn.close()
                    st.rerun()
            else:
                st.warning("⚠️ No signature")
        with sig_col_b:
            sig_upload = st.file_uploader("Upload New (PNG/JPG)", type=['png', 'jpg', 'jpeg'], key="sig_upl")
            if sig_upload:
                bytes_data = sig_upload.getvalue()
                save_signature(bytes_data)
                st.success("Saved!")
                st.rerun()
        final_sig_bytes = saved_sig_bytes if saved_sig_bytes else (sig_upload.getvalue() if sig_upload else None)

    st.subheader("Upload Orders")
    uploaded_file = st.file_uploader("Upload Daily Orders CSV", type=['csv'])

    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            
            if 'Ship to country' not in df.columns:
                st.error("⚠️ Error: Column 'Ship to country' not found.")
            else:
                us_shipments = df[df['Ship to country'] == 'United States'].copy()
                if 'Item type' in df.columns:
                    us_shipments = us_shipments[us_shipments['Item type'] == 'product']
                
                if 'Discount' not in us_shipments.columns: us_shipments['Discount'] = "0%"
                sales_data = us_shipments[['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit', 'Discount']].copy()
                sales_data['Variant code / SKU'] = sales_data['Variant code / SKU'].astype(str)
                
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
                
                processed['Discount_Float'] = processed['Discount'].astype(str).str.replace('%', '', regex=False)
                processed['Discount_Float'] = pd.to_numeric(processed['Discount_Float'], errors='coerce').fillna(0) / 100.0
                processed['Original_Retail'] = processed.apply(
                    lambda row: row['Price per unit'] / (1 - row['Discount_Float']) if row['Discount_Float'] < 1.0 else 0, axis=1
                )
                app_discount_decimal = discount_rate / 100.0
                processed['Transfer Price (Unit)'] = processed['Original_Retail'] * (1 - app_discount_decimal)
                processed['Transfer Total'] = processed['Quantity'] * processed['Transfer Price (Unit)']
                
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
                
                # --- GENERATE PDFS ---
                pdf_ci = generate_pdf("COMMERCIAL INVOICE", edited_df, inv_number, inv_date, 
                                      shipper_txt, importer_txt, consignee_txt, notes_txt, total_val, final_sig_bytes, signer_name)
                pdf_po = generate_pdf("PURCHASE ORDER", edited_df, inv_number, inv_date, 
                                      importer_txt, shipper_txt, consignee_txt, notes_txt, total_val, final_sig_bytes, signer_name)
                pdf_si = generate_pdf("SALES INVOICE", edited_df, inv_number, inv_date, 
                                      shipper_txt, importer_txt, consignee_txt, notes_txt, total_val, final_sig_bytes, signer_name)
                
                st.session_state['current_pdfs'] = {
                    'ci': pdf_ci, 'po': pdf_po, 'si': pdf_si, 
                    'inv_num': inv_number, 'total': total_val, 'buyer': importer_txt.split('\n')[0]
                }

                # --- ACTIONS AREA ---
                st.divider()
                col_left, col_right = st.columns([1, 2])
                
                with col_left:
                    st.subheader("🖨️ Downloads")
                    st.download_button("📄 Download Commercial Invoice", pdf_ci, f"CI-{inv_number}.pdf", "application/pdf")
                    st.download_button("📄 Download Purchase Order", pdf_po, f"PO-{inv_number}.pdf", "application/pdf")
                    st.download_button("📄 Download Sales Invoice", pdf_si, f"SI-{inv_number}.pdf", "application/pdf")
                
                with col_right:
                    st.subheader("📧 Email Center")
                    with st.expander("⚙️ Sender Settings (Required)", expanded=False):
                        st.info("You need an 'App Password' for Gmail. Normal passwords won't work.")
                        
                        # Load Settings from DB
                        db_email = get_setting('smtp_email')
                        db_pass = get_setting('smtp_pass')
                        
                        # Decode if bytes
                        default_email = db_email.decode('utf-8') if db_email else "dean.turner@holisticroasters.com"
                        default_pass = db_pass.decode('utf-8') if db_pass else ""
                        
                        sender_email = st.text_input("Your Email", value=default_email)
                        sender_pass = st.text_input("App Password", value=default_pass, type="password")
                        
                        if st.button("💾 Save Credentials"):
                            save_setting('smtp_email', sender_email.encode('utf-8'))
                            save_setting('smtp_pass', sender_pass.encode('utf-8'))
                            st.success("Credentials Saved!")
                            st.rerun()
                    
                    recipient_email = st.text_input("Send To:", value="dean.turner@holisticroasters.com")
                    
                    if st.button("📧 Email All Documents", type="primary"):
                        if not sender_pass:
                            st.error("Please enter your App Password in the settings above.")
                        else:
                            new_ver = save_invoice_metadata(inv_number, total_val, importer_txt.split('\n')[0])
                            
                            files_to_send = [
                                {'name': f"CI-{new_ver}.pdf", 'data': pdf_ci},
                                {'name': f"PO-{new_ver}.pdf", 'data': pdf_po},
                                {'name': f"SI-{new_ver}.pdf", 'data': pdf_si}
                            ]
                            
                            success, msg = send_email_with_attachments(sender_email, sender_pass, recipient_email, 
                                                                       f"Export Docs: {new_ver}", 
                                                                       f"Attached are the export documents for {new_ver}.", 
                                                                       files_to_send)
                            if success:
                                st.success(f"✅ Sent to {recipient_email}")
                            else:
                                st.error(f"Failed: {msg}")

        except Exception as e:
            st.error(f"Processing Error: {e}")

# ================= TAB 2: CATALOG =================
with tab_catalog:
    st.header("📦 Product Catalog")
    col_tools1, col_tools2, col_tools3 = st.columns(3)
    with col_tools1:
        template_df = pd.DataFrame(columns=['sku', 'product_name', 'description', 'hts_code', 'fda_code'])
        csv_template = template_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Template", csv_template, "catalog_template.csv", "text/csv")
    with col_tools2:
        current_catalog = get_catalog()
        if not current_catalog.empty:
            csv_current = current_catalog.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Catalog", csv_current, "full_catalog.csv", "text/csv")
    with col_tools3:
        if st.button("⚠️ Clear Catalog"):
            clear_catalog()
            st.rerun()

    st.subheader("Upload Catalog")
    cat_upload = st.file_uploader("Upload filled template", type=['csv'])
    if cat_upload:
        try:
            new_cat_df = pd.read_csv(cat_upload)
            upsert_catalog_from_df(new_cat_df)
            st.success("Catalog Updated!")
            st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

    st.subheader("Edit Catalog")
    if not current_catalog.empty:
        edited_catalog = st.data_editor(current_catalog, num_rows="dynamic")
        if st.button("💾 Save Changes"):
            upsert_catalog_from_df(edited_catalog)
            st.success("Saved!")

# ================= TAB 3: HISTORY =================
with tab_history:
    st.header("🗄️ Documents Archive")
    st.caption("Shows log of generated invoices. (Download feature disabled in this view).")
    history_df = get_history()
    st.dataframe(history_df, use_container_width=True, hide_index=True)
