import streamlit as st
import pandas as pd
import sqlite3
from fpdf import FPDF
from datetime import datetime, date, timedelta
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
import random
import json
import time

# --- GLOBAL DEFAULTS ---
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

DEFAULT_NOTES = """CUSTOMS BROKER: Strix (Entry@strixsmart.com)
HOLISTIC ROASTERS inc. Canada FDA #: 11638755492
- ALL PRICES IN USD
- Incoterms: EXW"""

DEFAULT_HTS = "0901.21.00.20"
DEFAULT_FDA = "31ADT01"

# --- Database Setup ---
def init_db():
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    # 1. Existing Tables (Preserved)
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
                  fda_code TEXT,
                  weight_lbs REAL)''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS settings
                 (key TEXT PRIMARY KEY,
                  value BLOB)''')
    
    # 2. NEW Table for Batches (Added safely)
    c.execute('''CREATE TABLE IF NOT EXISTS batches
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  batch_name TEXT,
                  status TEXT,
                  created_at TEXT,
                  updated_at TEXT,
                  data TEXT)''')
                  
    conn.commit()
    conn.close()

# --- DB Functions ---
def clean_sku(val):
    """Fixes '10121' vs 10121.0 mismatch"""
    if pd.isna(val) or val == "": return ""
    val_str = str(val).strip()
    if val_str.endswith(".0"): return val_str[:-2]
    return val_str

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
        weight = row.get('weight_lbs', 0.0)
        try: weight = float(weight)
        except: weight = 0.0
        if pd.isna(desc) or desc == "": desc = p_name
        
        # CLEAN SKU before saving
        sku_val = clean_sku(row['sku'])
        
        c.execute("""INSERT OR REPLACE INTO product_catalog_v3 
                     (sku, product_name, description, hts_code, fda_code, weight_lbs) 
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  (sku_val, p_name, desc, str(row['hts_code']), str(row['fda_code']), weight))
    conn.commit()
    conn.close()

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

def get_signature():
    return get_setting('signature')

# --- BATCH FUNCTIONS ---
def get_batches(status='Active'):
    conn = sqlite3.connect('invoices.db')
    df = pd.read_sql_query("SELECT * FROM batches WHERE status=? ORDER BY updated_at DESC", conn, params=(status,))
    conn.close()
    return df

def create_batch(name):
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    est = pytz.timezone('US/Eastern')
    now = datetime.now(est).strftime("%Y-%m-%d %H:%M:%S")
    
    # 1. Grab defaults from Global Settings (if they exist)
    saved_cons = get_setting('default_consignee')
    def_cons = saved_cons.decode('utf-8') if saved_cons else DEFAULT_CONSIGNEE
    
    saved_notes = get_setting('default_notes')
    def_notes = saved_notes.decode('utf-8') if saved_notes else DEFAULT_NOTES
    
    saved_carrier = get_setting('default_carrier')
    def_carrier = saved_carrier.decode('utf-8') if saved_carrier else "FX (FedEx)"

    # 2. Initialize Data
    new_data = {
        "inv_number": f"{date.today().strftime('%Y%m%d')}1",
        "inv_date": str(date.today()),
        "discount": 75.0,
        "consignee": def_cons,
        "notes": def_notes,
        "carrier": def_carrier,
        "pallets": 1,
        "cartons": 1,
        "gross_weight": 0.0,
        "orders_json": None
    }

    # 3. Inherit from LAST batch (Overrides global defaults if available)
    try:
        c.execute("SELECT data FROM batches ORDER BY updated_at DESC LIMIT 1")
        row = c.fetchone()
        if row:
            last_data = json.loads(row[0])
            # Only copy specific fields
            if 'discount' in last_data: new_data['discount'] = last_data['discount']
            if 'consignee' in last_data and last_data['consignee']: new_data['consignee'] = last_data['consignee']
            if 'notes' in last_data and last_data['notes']: new_data['notes'] = last_data['notes']
            if 'carrier' in last_data: new_data['carrier'] = last_data['carrier']
            if 'gross_weight' in last_data: new_data['gross_weight'] = last_data['gross_weight']
            if 'pallets' in last_data: new_data['pallets'] = last_data['pallets']
    except Exception as e:
        print(f"Inheritance error: {e}")
    
    c.execute("INSERT INTO batches (batch_name, status, created_at, updated_at, data) VALUES (?, ?, ?, ?, ?)",
              (name, 'Active', now, now, json.dumps(new_data)))
    new_id = c.lastrowid
    conn.commit()
    conn.close()
    return new_id

def update_batch(batch_id, data_dict):
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    est = pytz.timezone('US/Eastern')
    now = datetime.now(est).strftime("%Y-%m-%d %H:%M:%S")
    c.execute("UPDATE batches SET data=?, updated_at=? WHERE id=?", 
              (json.dumps(data_dict), now, batch_id))
    conn.commit()
    conn.close()

def finalize_batch_in_db(batch_id):
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    c.execute("UPDATE batches SET status='Completed' WHERE id=?", (batch_id,))
    conn.commit()
    conn.close()

def save_invoice_metadata(inv_num, total_val, buyer):
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    est = pytz.timezone('US/Eastern')
    timestamp = datetime.now(est).strftime("%Y-%m-%d %H:%M EST")
    c.execute("""INSERT INTO invoice_history_v3 
                 (invoice_number, date_created, total_value, buyer_name) 
                 VALUES (?, ?, ?, ?)""",
              (inv_num, timestamp, total_val, buyer))
    conn.commit()
    conn.close()

def get_history():
    conn = sqlite3.connect('invoices.db')
    df = pd.read_sql_query("SELECT invoice_number, date_created, buyer_name, total_value FROM invoice_history_v3 ORDER BY id DESC", conn)
    conn.close()
    return df

# --- Helper: Prompt for Backup ---
def show_backup_prompt(key_suffix):
    if os.path.exists("invoices.db"):
        st.info("✅ **Changes Saved to Database!**")
        with open("invoices.db", "rb") as f:
            st.download_button(
                "📥 DOWNLOAD BACKUP NOW",
                data=f,
                file_name=f"holistic_backup_{datetime.now().strftime('%Y-%m-%d_%H%M')}.db",
                mime="application/x-sqlite3",
                key=f"prompt_backup_{key_suffix}",
                type="primary"
            )

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
        .stApp { background-color: #FAFAFA; font-family: 'Open Sans', sans-serif; }
        h1, h2, h3 { font-family: 'Montserrat', sans-serif !important; color: #6F4E37 !important; font-weight: 700; }
        div.stButton > button {
            background-color: #6F4E37 !important; color: white !important; border-radius: 8px !important;
            border: none !important; font-family: 'Montserrat', sans-serif !important; font-weight: 600 !important;
        }
        div.stButton > button:hover { background-color: #5A3E2B !important; }
        .stTextInput input, .stTextArea textarea, .stDateInput input, .stNumberInput input {
            border-radius: 8px !important; border: 1px solid #D0D0D0 !important;
        }
        .stTextInput input:focus, .stTextArea textarea:focus {
            border-color: #6F4E37 !important; box-shadow: 0 0 0 1px #6F4E37 !important;
        }
        [data-testid="stSidebar"] { background-color: #f0f2f6; }
    </style>
""", unsafe_allow_html=True)

# --- BACKUP SIDEBAR ---
st.sidebar.header("💾 Data Persistence")

if os.path.exists("invoices.db"):
    mod_time = os.path.getmtime("invoices.db")
    dt_mod = datetime.fromtimestamp(mod_time).strftime('%I:%M:%S %p')
    st.sidebar.caption(f"Last Saved: {dt_mod}")
    with open("invoices.db", "rb") as f:
        st.sidebar.download_button("📥 Backup Current State", data=f, file_name=f"holistic_backup_{datetime.now().strftime('%Y-%m-%d_%H%M')}.db", mime="application/x-sqlite3", key="sidebar_backup")

st.sidebar.markdown("---")
st.sidebar.info("Upload backup to restore Catalog & Defaults")
uploaded_db = st.sidebar.file_uploader("📤 Restore Backup", type=["db"])
if uploaded_db:
    if st.sidebar.button("⚠️ Confirm Restore"):
        try:
            with open("invoices.db", "wb") as f:
                f.write(uploaded_db.getvalue())
            st.sidebar.success("✅ Restored! Reloading page...")
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"Error: {e}")

st.title("☕ Holistic Roasters Export Hub")

# --- GLOBAL NAVIGATION (SIDEBAR) ---
st.sidebar.header("📁 Workflow")
page = st.sidebar.radio("Go to:", ["Batches (Dashboard)", "Catalog", "Archive (History)"])

# --- PDF Class ---
class ProInvoice(FPDF):
    def header(self): pass
    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()} of {{nb}}', 0, 0, 'R')

# --- PDF Generators ---
def generate_ci_pdf(doc_type, df, inv_num, inv_date, addr_from, addr_to, addr_ship, notes, total_val, sig_bytes=None, signer_name="Dean Turner"):
    pdf = ProInvoice(); pdf.alias_nb_pages(); pdf.add_page(); pdf.set_auto_page_break(auto=False)
    pdf.set_font('Helvetica', 'B', 20); pdf.cell(0, 10, doc_type, 0, 1, 'C'); pdf.ln(5)
    pdf.set_font("Helvetica", '', 9); y_start = pdf.get_y()
    pdf.set_xy(10, y_start); pdf.set_font("Helvetica", 'B', 10); pdf.cell(70, 5, "SHIPPER / EXPORTER:", 0, 1)
    pdf.set_x(10); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(70, 4, addr_from)
    pdf.set_xy(90, y_start); pdf.set_font("Helvetica", 'B', 10); pdf.cell(70, 5, "CONSIGNEE (SHIP TO):", 0, 1)
    pdf.set_xy(90, pdf.get_y()); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(70, 4, addr_ship)
    pdf.set_xy(160, y_start); pdf.set_font("Helvetica", 'B', 12); pdf.cell(40, 6, f"Invoice #: {inv_num}", 0, 1, 'R')
    pdf.set_x(160); pdf.set_font("Helvetica", '', 10); pdf.cell(40, 6, f"Date: {inv_date}", 0, 1, 'R')
    pdf.set_x(160); pdf.cell(40, 6, "Currency: USD", 0, 1, 'R'); pdf.set_x(160); pdf.cell(40, 6, "Origin: CANADA", 0, 1, 'R')
    y_mid = max(pdf.get_y(), 60) + 10; pdf.set_xy(10, y_mid); pdf.set_font("Helvetica", 'B', 10); pdf.cell(80, 5, "IMPORTER OF RECORD:", 0, 1)
    pdf.set_x(10); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(80, 4, addr_to)
    pdf.set_xy(100, y_mid); pdf.set_fill_color(245, 245, 245); pdf.rect(100, y_mid, 95, 30, 'F')
    pdf.set_xy(102, y_mid + 2); pdf.set_font("Helvetica", 'B', 9); pdf.cell(50, 5, "NOTES / BROKER / FDA:", 0, 1)
    pdf.set_xy(102, pdf.get_y()); pdf.set_font("Helvetica", '', 8); pdf.multi_cell(90, 4, notes); pdf.set_y(y_mid + 35)
    w = [12, 40, 45, 23, 23, 22, 25]; headers = ["QTY", "PRODUCT", "DESCRIPTION", "HTS #", "FDA CODE", "UNIT ($)", "TOTAL ($)"]
    pdf.set_font("Helvetica", 'B', 7); pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers): pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln(); pdf.set_font("Helvetica", '', 7)
    for _, row in df.iterrows():
        data = [str(int(row['Quantity'])), str(row['Product Name']), str(row['Description']), str(row.get('HTS Code','')), str(row.get('FDA Code','')), f"{row['Transfer Price (Unit)']:.2f}", f"{row['Transfer Total']:.2f}"]
        for i, d in enumerate(data): pdf.cell(w[i], 6, d, 1, 0, 'L' if i in [1,2] else 'C')
        pdf.ln()
    pdf.ln(2); pdf.set_font("Helvetica", 'B', 9); pdf.cell(sum(w[:-1]), 8, "TOTAL VALUE (USD):", 0, 0, 'R'); pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    pdf.ln(10); pdf.set_font("Helvetica", '', 10); pdf.cell(0, 5, "I declare that all information contained in this invoice to be true and correct.", 0, 1, 'L')
    pdf.ln(10); pdf.set_font("Helvetica", 'B', 10); pdf.cell(0, 5, signer_name, 0, 1, 'L')
    if sig_bytes:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp: tmp.write(sig_bytes); tmp_path = tmp.name
        try: pdf.image(tmp_path, x=10, y=pdf.get_y()-20, w=40)
        except: pass
        os.unlink(tmp_path)
    return bytes(pdf.output())

def generate_si_pdf(df, inv_num, inv_date, addr_from, addr_to, addr_ship, notes, total_val, sig_bytes, signer_name):
    # Simplified Sales Invoice
    pdf = ProInvoice(); pdf.alias_nb_pages(); pdf.add_page(); pdf.set_auto_page_break(auto=False)
    pdf.set_font('Helvetica', 'B', 20); pdf.cell(0, 10, "SALES INVOICE", 0, 1, 'C'); pdf.ln(5)
    pdf.set_font("Helvetica", '', 9); y_start = pdf.get_y()
    pdf.set_xy(10, y_start); pdf.set_font("Helvetica", 'B', 10); pdf.cell(70, 5, "SHIPPER / EXPORTER:", 0, 1); pdf.set_x(10); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(70, 4, addr_from)
    pdf.set_xy(90, y_start); pdf.set_font("Helvetica", 'B', 10); pdf.cell(70, 5, "SHIP TO:", 0, 1); pdf.set_xy(90, pdf.get_y()); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(70, 4, addr_ship)
    pdf.set_xy(160, y_start); pdf.set_font("Helvetica", 'B', 12); pdf.cell(40, 6, f"Invoice #: {inv_num}", 0, 1, 'R'); pdf.set_x(160); pdf.set_font("Helvetica", '', 10); pdf.cell(40, 6, f"Date: {inv_date}", 0, 1, 'R'); pdf.set_x(160); pdf.cell(40, 6, "Currency: USD", 0, 1, 'R')
    y_mid = max(pdf.get_y(), 50) + 10; pdf.set_xy(10, y_mid); pdf.set_font("Helvetica", 'B', 10); pdf.cell(80, 5, "BILL TO:", 0, 1); pdf.set_x(10); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(80, 4, addr_to); pdf.set_y(y_mid + 35)
    w = [20, 100, 35, 35]; headers = ["QTY", "PRODUCT", "UNIT ($)", "TOTAL ($)"]
    pdf.set_font("Helvetica", 'B', 7); pdf.set_fill_color(220, 220, 220); 
    for i, h in enumerate(headers): pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln(); pdf.set_font("Helvetica", '', 7)
    for _, row in df.iterrows():
        data = [str(int(row['Quantity'])), str(row['Product Name']), f"{row['Transfer Price (Unit)']:.2f}", f"{row['Transfer Total']:.2f}"]
        for i, d in enumerate(data): pdf.cell(w[i], 6, d, 1, 0, 'L' if i==1 else 'C')
        pdf.ln()
    pdf.ln(2); pdf.set_font("Helvetica", 'B', 9); pdf.cell(sum(w[:-1]), 8, "TOTAL AMOUNT DUE (USD):", 0, 0, 'R'); pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    return bytes(pdf.output())

def generate_pl_pdf(df, inv_num, inv_date, addr_from, addr_to, addr_ship, cartons):
    pdf = ProInvoice(); pdf.alias_nb_pages(); pdf.add_page(); pdf.set_auto_page_break(auto=False)
    pdf.set_font('Helvetica', 'B', 20); pdf.cell(0, 10, "PACKING LIST", 0, 1, 'C'); pdf.ln(5)
    pdf.set_font("Helvetica", '', 9); y_start = pdf.get_y()
    pdf.set_xy(10, y_start); pdf.set_font("Helvetica", 'B', 10); pdf.cell(70, 5, "SHIPPER / EXPORTER:", 0, 1); pdf.set_x(10); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(70, 4, addr_from)
    pdf.set_xy(90, y_start); pdf.set_font("Helvetica", 'B', 10); pdf.cell(70, 5, "SHIP TO:", 0, 1); pdf.set_xy(90, pdf.get_y()); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(70, 4, addr_ship)
    pdf.set_xy(160, y_start); pdf.set_font("Helvetica", 'B', 12); pdf.cell(40, 6, f"Packing List #: {inv_num}", 0, 1, 'R'); pdf.set_x(160); pdf.set_font("Helvetica", '', 10); pdf.cell(40, 6, f"Date: {inv_date}", 0, 1, 'R')
    y_mid = max(pdf.get_y(), 50) + 10; pdf.set_xy(10, y_mid); pdf.set_font("Helvetica", 'B', 10); pdf.cell(80, 5, "BILL TO:", 0, 1); pdf.set_x(10); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(80, 4, addr_to); pdf.set_y(y_mid + 35)
    w = [30, 160]; headers = ["QTY", "PRODUCT"]
    pdf.set_font("Helvetica", 'B', 7); pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers): pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln(); pdf.set_font("Helvetica", '', 7)
    for _, row in df.iterrows():
        data = [str(int(row['Quantity'])), str(row['Product Name'])]
        for i, d in enumerate(data): pdf.cell(w[i], 6, d, 1, 0, 'L' if i==1 else 'C')
        pdf.ln()
    pdf.ln(5); pdf.set_font("Helvetica", 'B', 10); pdf.set_x(10); pdf.cell(sum(w), 8, f"TOTAL CARTONS: {cartons}", 0, 1, 'R')
    return bytes(pdf.output())

def generate_po_pdf(df, inv_num, inv_date, addr_buyer, addr_vendor, addr_ship, total_val):
    pdf = ProInvoice(); pdf.alias_nb_pages(); pdf.add_page(); pdf.set_auto_page_break(auto=False)
    pdf.set_font('Helvetica', 'B', 20); pdf.cell(0, 10, "PURCHASE ORDER", 0, 1, 'C'); pdf.ln(5)
    pdf.set_font("Helvetica", '', 9); y_start = pdf.get_y()
    pdf.set_xy(10, y_start); pdf.set_font("Helvetica", 'B', 10); pdf.cell(70, 5, "FROM (BUYER):", 0, 1); pdf.set_x(10); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(70, 4, addr_buyer)
    pdf.set_xy(90, y_start); pdf.set_font("Helvetica", 'B', 10); pdf.cell(70, 5, "SHIP TO:", 0, 1); pdf.set_xy(90, pdf.get_y()); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(70, 4, addr_ship)
    pdf.set_xy(160, y_start); pdf.set_font("Helvetica", 'B', 12); pdf.cell(40, 6, f"Invoice #: {inv_num}", 0, 1, 'R'); pdf.set_x(160); pdf.set_font("Helvetica", '', 10); pdf.cell(40, 6, f"Date: {inv_date}", 0, 1, 'R'); pdf.set_x(160); pdf.cell(40, 6, "Currency: USD", 0, 1, 'R')
    y_mid = max(pdf.get_y(), 50) + 10; pdf.set_xy(10, y_mid); pdf.set_font("Helvetica", 'B', 10); pdf.cell(80, 5, "TO (VENDOR):", 0, 1); pdf.set_x(10); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(80, 4, addr_vendor); pdf.set_y(y_mid + 35)
    w = [20, 100, 35, 35]; headers = ["QTY", "PRODUCT", "UNIT ($)", "TOTAL ($)"]
    pdf.set_font("Helvetica", 'B', 7); pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers): pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln(); pdf.set_font("Helvetica", '', 7)
    for _, row in df.iterrows():
        data = [str(int(row['Quantity'])), str(row['Product Name']), f"{row['Transfer Price (Unit)']:.2f}", f"{row['Transfer Total']:.2f}"]
        for i, d in enumerate(data): pdf.cell(w[i], 6, d, 1, 0, 'L' if i==1 else 'C')
        pdf.ln()
    pdf.ln(2); pdf.set_font("Helvetica", 'B', 9); pdf.cell(sum(w[:-1]), 8, "TOTAL (USD):", 0, 0, 'R'); pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    return bytes(pdf.output())

def generate_bol_pdf(df, inv_number, inv_date, shipper_txt, consignee_txt, carrier_pdf_display, hbol_number, pallets, cartons, total_weight_lbs, sig_bytes=None):
    pdf = FPDF(); pdf.alias_nb_pages()
    for copy_num in range(2):
        pdf.add_page(); pdf.set_auto_page_break(auto=False)
        pdf.set_font('Helvetica', 'B', 18); pdf.cell(0, 10, "STRAIGHT BILL OF LADING", 0, 1, 'C'); pdf.ln(5)
        pdf.set_font("Helvetica", '', 10); y_top = pdf.get_y()
        pdf.set_xy(10, y_top); pdf.set_font("Helvetica", 'B', 10); pdf.cell(20, 6, "Date:", 0, 0); pdf.set_font("Helvetica", '', 10); pdf.cell(40, 6, str(inv_date), 0, 0)
        pdf.set_xy(130, y_top); pdf.set_font("Helvetica", 'B', 10); pdf.cell(30, 6, "BOL #:", 0, 0, 'R'); pdf.set_font("Helvetica", '', 10); pdf.cell(40, 6, hbol_number, 0, 1, 'R'); pdf.ln(10)
        y_addr = pdf.get_y()
        pdf.set_xy(10, y_addr); pdf.set_font("Helvetica", 'B', 11); pdf.cell(90, 6, "SHIP FROM (SHIPPER)", 1, 1, 'L', fill=False); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(0, 5, shipper_txt, 1, 'L'); pdf.ln(5)
        pdf.set_x(10); pdf.set_font("Helvetica", 'B', 11); pdf.cell(0, 6, "SHIP TO (CONSIGNEE)", 1, 1, 'L', fill=False); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(0, 5, consignee_txt, 1, 'L'); pdf.ln(5)
        pdf.set_font("Helvetica", 'B', 11); pdf.cell(0, 6, f"CARRIER: {carrier_pdf_display}", 0, 1); pdf.ln(5)
        w = [15, 25, 100, 30, 20]; headers = ["HM", "QTY", "DESCRIPTION OF COMMODITY", "WEIGHT", "CLASS"]
        pdf.set_font("Helvetica", 'B', 9); pdf.set_fill_color(220, 220, 220)
        for i, h in enumerate(headers): pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
        pdf.ln()
        pdf.set_font("Helvetica", '', 9)
        # Rows
        row1 = ["", f"{pallets} PLT", "ROASTED COFFEE (NMFC 056820)", f"{total_weight_lbs:.1f} lbs", "60"]
        for i, d in enumerate(row1): pdf.cell(w[i], 6, d, 1, 0, 'C' if i!=2 else 'L')
        pdf.ln()
        row2 = ["", f"{cartons} CTN", "(Contains roasted coffee in bags)", "", ""]
        for i, d in enumerate(row2): pdf.cell(w[i], 6, d, 1, 0, 'C' if i!=2 else 'L')
        pdf.ln(10)
        pdf.set_font("Helvetica", '', 8); legal = "RECEIVED, subject to the classifications and tariffs..."; pdf.multi_cell(0, 4, legal); pdf.ln(15)
        y_sig = pdf.get_y(); pdf.line(10, y_sig, 90, y_sig); pdf.line(110, y_sig, 190, y_sig)
        pdf.set_font("Helvetica", 'B', 8); pdf.set_xy(10, y_sig + 2); pdf.cell(80, 4, "SHIPPER SIGNATURE / DATE", 0, 0)
        pdf.set_xy(110, y_sig + 2); pdf.cell(80, 4, "CARRIER SIGNATURE / DATE", 0, 1)
        if sig_bytes:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp: tmp.write(sig_bytes); tmp_path = tmp.name
            try: pdf.image(tmp_path, x=15, y=y_sig-15, w=35)
            except: pass
            os.unlink(tmp_path)
    return bytes(pdf.output())

def generate_customscity_csv(df, inv_number, inv_date, ship_to_txt, hbol_number, carrier_code):
    lines = [L.strip() for L in ship_to_txt.split('\n') if L.strip()]
    c_name = lines[0] if len(lines) > 0 else ""; c_addr = ""; c_city = ""; c_state = ""; c_zip = ""; c_country = "US"
    if len(lines) >= 2:
        last_line = lines[-1]
        if last_line.upper() in ["UNITED STATES", "USA", "US"]:
            c_country = "US"
            if len(lines) > 2: last_line = lines[-2]; c_addr = ", ".join(lines[1:-2])
            else: c_addr = lines[1]
        else:
            if len(lines) > 2: c_addr = ", ".join(lines[1:-1])
            else: c_addr = lines[1]
        parts = last_line.split(',')
        if len(parts) >= 1: c_city = parts[0].strip()
        if len(parts) >= 2:
            state_zip = parts[1].strip().split(' ')
            state_zip = [x for x in state_zip if x]; 
            if len(state_zip) >= 1: c_state = state_zip[0]
            if len(state_zip) >= 2: c_zip = state_zip[1]
    
    weekday = inv_date.weekday()
    days_to_add = 3 if weekday == 4 else (2 if weekday == 5 else 1)
    est_arrival = inv_date + timedelta(days=days_to_add)
    
    rows = []
    for _, row in df.iterrows():
        fda = str(row.get('FDA Code', '')).strip()
        if not fda or fda.lower() == 'nan': continue
        rows.append({
            'Entry Type': '01', 'Reference Qualifier': 'BOL', 'Reference Number': '', 'Mode of Transport': '30', 'Bill Type': 'R',
            'MBOL/TRIP Number': hbol_number, 'HBOL/ Shipment Control Number': hbol_number,
            'Estimate Date of Arrival': est_arrival.strftime('%Y%m%d'), 'Time of Arrival': '18:00', 'US Port of Arrival': '0712',
            'Equipment Number': '', 'Shipper Name': 'HOLISTIC ROASTERS', 'Shipper Address': '3780 RUE SAINT-PATRICK',
            'Shipper City': 'MONTREAL', 'Shipper Country': 'CA', 'Consignee Name': c_name, 'Consignee Address': c_addr,
            'Consignee City': c_city, 'Consignee State or Province': c_state, 'Consignee Postal Code': c_zip, 'Consignee Country': c_country,
            'Description': row['Description'], 'Product ID': row['Variant code / SKU'], 'Carrier Name': carrier_code, 'Vessel Name': '',
            'Voyage Trip Flight Number': hbol_number, 'Rail Car Number': ''
        })
    return pd.DataFrame(rows).to_csv(index=False).encode('utf-8')

# ==================== PAGE 1: BATCHES ====================
if page == "Batches (Dashboard)":
    st.header("📂 Batch Management")
    
    with st.expander("✨ Start a New Batch", expanded=True):
        new_batch_name = st.text_input("Batch Name", value=f"Export-{date.today()}")
        if st.button("Create Batch"):
            create_batch(new_batch_name)
            st.success("Batch created! Select it below.")
            st.rerun()
    
    st.markdown("---")
    
    batches_df = get_batches()
    if batches_df.empty:
        st.info("No active batches found. Create one above.")
    else:
        batch_options = batches_df['batch_name'].tolist()
        selected_batch_name = st.selectbox("Select Active Batch to Resume", batch_options)
        
        batch_row = batches_df[batches_df['batch_name'] == selected_batch_name].iloc[0]
        batch_id = int(batch_row['id'])
        batch_data = json.loads(batch_row['data'])
        
        st.info(f"**Working on:** {selected_batch_name} | **Last Saved:** {batch_row['updated_at']}")
        
        col_main, col_settings = st.columns([2, 1])
        
        with col_settings:
            st.subheader("⚙️ Settings")
            st.markdown("**Signature**")
            saved_sig = get_setting('signature')
            if saved_sig: st.success("Signature Loaded")
            else: 
                sig_up = st.file_uploader("Upload Sig", type=['png','jpg'])
                if sig_up: 
                    save_setting('signature', sig_up.getvalue())
                    show_backup_prompt("sig_up")
                    st.rerun()
            
            st.markdown("**Carrier**")
            c_opts = ["FX (FedEx)", "GCYD (Green City Courier)", "Other"]
            current_carrier = batch_data.get('carrier', "FX (FedEx)")
            if current_carrier not in c_opts: current_carrier = "Other"
            sel_carrier = st.selectbox("Carrier", c_opts, index=c_opts.index(current_carrier) if current_carrier in c_opts else 2)
            
            carrier_code = "FX"; carrier_name = "FedEx (FX)"
            if "GCYD" in sel_carrier: carrier_code = "GCYD"; carrier_name = "Green City Courier (GCYD)"
            elif "Other" in sel_carrier: carrier_code = st.text_input("Code", value="XYZ"); carrier_name = st.text_input("Name", value="Custom Carrier")

        with col_main:
            st.subheader("📝 Batch Details")
            c1, c2 = st.columns(2)
            with c1:
                b_inv_num = st.text_input("Invoice #", value=batch_data.get('inv_number', ''))
                b_date = st.date_input("Date", value=datetime.strptime(batch_data.get('inv_date', str(date.today())), "%Y-%m-%d"))
                b_discount = st.number_input("Transfer Discount %", value=batch_data.get('discount', 75.0))
            
            with c2:
                b_cons = st.text_area("Consignee", value=batch_data.get('consignee', ""), height=100)
                b_notes = st.text_area("Notes", value=batch_data.get('notes', ""), height=100)

            st.subheader("📦 Orders")
            # CATALOG CHECK
            cat_check = get_catalog()
            if cat_check.empty:
                st.error("🔴 Catalog is EMPTY. Please Restore Backup or Upload Catalog in the 'Catalog' tab.")
            else:
                st.success(f"✅ Catalog Loaded ({len(cat_check)} items)")

            saved_orders_json = batch_data.get('orders_json')
            
            if saved_orders_json:
                st.success("Loaded saved orders from batch.")
                df = pd.read_json(io.StringIO(saved_orders_json), orient='split')
            else:
                uploaded_file = st.file_uploader("Upload CSV", type=['csv'])
                if uploaded_file:
                    df = pd.read_csv(uploaded_file)
                    if 'Ship to country' in df.columns:
                        us_shipments = df[df['Ship to country'] == 'United States'].copy()
                        if 'Item type' in df.columns: us_shipments = us_shipments[us_shipments['Item type'] == 'product']
                        
                        sales_data = us_shipments[['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit']].copy()
                        # CLEAN SKU (String and Strip .0)
                        sales_data['Variant code / SKU'] = sales_data['Variant code / SKU'].apply(clean_sku)
                        sales_data['Price per unit'] = pd.to_numeric(sales_data['Price per unit'], errors='coerce').fillna(0)
                        
                        if not cat_check.empty:
                            # Clean Catalog SKUs too
                            cat_check['sku'] = cat_check['sku'].apply(clean_sku)
                            merged = pd.merge(sales_data, cat_check, left_on='Variant code / SKU', right_on='sku', how='left')
                            merged['Product Name'] = merged['product_name'].fillna(merged['Item variant'])
                            merged['Description'] = merged['description'].fillna(merged['Product Name'])
                            merged['HTS Code'] = merged['hts_code'].fillna(DEFAULT_HTS)
                            merged['FDA Code'] = merged['fda_code'].fillna(DEFAULT_FDA)
                            merged['Weight (lbs)'] = merged['weight_lbs'].fillna(0.0)
                            df = merged 
                        else:
                            sales_data['Product Name'] = sales_data['Item variant']
                            sales_data['Description'] = sales_data['Item variant']
                            sales_data['HTS Code'] = DEFAULT_HTS
                            sales_data['FDA Code'] = DEFAULT_FDA
                            sales_data['Weight (lbs)'] = 0.0
                            df = sales_data
                    else:
                        st.error("Invalid CSV"); df = pd.DataFrame()
                else:
                    df = pd.DataFrame()

            if not df.empty:
                app_discount_decimal = b_discount / 100.0
                df['Transfer Price (Unit)'] = df['Price per unit'] * (1 - app_discount_decimal)
                df['Transfer Total'] = df['Quantity'] * df['Transfer Price (Unit)']
                
                consolidated = df.groupby(['Variant code / SKU', 'Product Name', 'Description']).agg({
                    'Quantity': 'sum', 'HTS Code': 'first', 'FDA Code': 'first', 'Weight (lbs)': 'first',
                    'Transfer Price (Unit)': 'mean', 'Transfer Total': 'sum'
                }).reset_index()
                
                edited_df = st.data_editor(consolidated, num_rows="dynamic")
                total_val = edited_df['Transfer Total'].sum()
                st.metric("Total Value", f"${total_val:,.2f}")
                
                c_log1, c_log2, c_log3 = st.columns(3)
                with c_log1: pallets = st.number_input("Pallets", value=batch_data.get('pallets', 1))
                with c_log2: cartons = st.number_input("Cartons", value=batch_data.get('cartons', 1))
                with c_log3: 
                    calc_w = (edited_df['Quantity'] * edited_df['Weight (lbs)']).sum()
                    gross_weight = st.number_input("Gross Weight", value=batch_data.get('gross_weight', calc_w))

                st.markdown("---")
                
                if st.button("💾 SAVE BATCH PROGRESS", type="primary"):
                    save_data = {
                        "inv_number": b_inv_num, "inv_date": str(b_date), "discount": b_discount,
                        "consignee": b_cons, "notes": b_notes, "carrier": sel_carrier,
                        "pallets": pallets, "cartons": cartons, "gross_weight": gross_weight,
                        "orders_json": edited_df.to_json(orient='split')
                    }
                    update_batch(batch_id, save_data)
                    st.success("✅ Saved! You can close the tab safely.")
                    show_backup_prompt("batch_save")
                    
                st.subheader("🖨️ Documents")
                base_id = b_inv_num; hbol = f"HRUS{base_id}"
                
                pdf_ci = generate_ci_pdf("COMMERCIAL INVOICE", edited_df, f"CI-HRUS{base_id}", b_date, DEFAULT_SHIPPER, DEFAULT_IMPORTER, b_cons, b_notes, total_val, get_signature(), "Dean Turner")
                pdf_po = generate_po_pdf(edited_df, f"PO-HRUS{base_id}", b_date, DEFAULT_IMPORTER, DEFAULT_SHIPPER, b_cons, total_val)
                pdf_si = generate_si_pdf(edited_df, f"SI-HRUS{base_id}", b_date, DEFAULT_SHIPPER, DEFAULT_IMPORTER, b_cons, b_notes, total_val, get_signature(), "Dean Turner")
                pdf_pl = generate_pl_pdf(edited_df, f"PL-HRUS{base_id}", b_date, DEFAULT_SHIPPER, DEFAULT_IMPORTER, b_cons, cartons)
                pdf_bol = generate_bol_pdf(edited_df, b_inv_num, b_date, DEFAULT_SHIPPER, b_cons, carrier_name, hbol, pallets, cartons, gross_weight, get_signature())
                csv_data = generate_customscity_csv(edited_df, b_inv_num, b_date, b_cons, hbol, carrier_code)
                
                c1, c2, c3, c4, c5 = st.columns(5)
                with c1: st.download_button("CI PDF", pdf_ci, "CI.pdf", key=f"dl_ci_{batch_id}")
                with c2: st.download_button("PO PDF", pdf_po, "PO.pdf", key=f"dl_po_{batch_id}")
                with c3: st.download_button("SI PDF", pdf_si, "SI.pdf", key=f"dl_si_{batch_id}")
                with c4: st.download_button("PL PDF", pdf_pl, "PL.pdf", key=f"dl_pl_{batch_id}")
                with c5: st.download_button("BOL PDF", pdf_bol, "BOL.pdf", key=f"dl_bol_{batch_id}")
                
                c_csv_btn, c_csv_link = st.columns([1.5, 2])
                with c_csv_btn: st.download_button("📥 CustomsCity CSV", csv_data, "upload.csv", type="primary", key=f"dl_csv_{batch_id}")
                with c_csv_link: st.markdown("""<div style="margin-top: 8px;"><a href="https://app.customscity.com/upload/document/" target="_blank" style="font-weight: 600; color: #6F4E37; text-decoration: none;">🚀 Upload to CustomsCity</a></div>""", unsafe_allow_html=True)
                
                # EMAIL CENTER
                st.markdown("---")
                st.subheader("📧 Email Center")
                with st.expander("⚙️ Sender Settings", expanded=True):
                    db_email = get_setting('smtp_email'); db_pass = get_setting('smtp_pass')
                    default_email = db_email.decode() if db_email else "dean.turner@holisticroasters.com"
                    default_pass = db_pass.decode() if db_pass else ""
                    sender_email = st.text_input("Your Email", value=default_email)
                    sender_pass = st.text_input("App Password", value=default_pass, type="password")
                    if st.button("💾 Save Credentials"):
                        save_setting('smtp_email', sender_email.encode()); save_setting('smtp_pass', sender_pass.encode())
                        st.success("Saved!")
                        show_backup_prompt("email_creds")

                recipient_email = st.text_input("Send To:", value="dean.turner@holisticroasters.com")
                attach_backup = st.checkbox("Attach App Backup (invoices.db)", value=True, help="Useful for restoring settings if the app reboots.")

                if st.button("📧 Email All Documents", type="primary"):
                    if not sender_pass: st.error("Please enter your App Password in the settings above.")
                    else:
                        new_ver = b_inv_num # Use current invoice number
                        # Log history
                        save_invoice_metadata(b_inv_num, total_val, DEFAULT_IMPORTER.split('\n')[0])
                        finalize_batch_in_db(batch_id)
                        
                        files_to_send = [
                            {'name': f"{base_id}_CI.pdf", 'data': pdf_ci},
                            {'name': f"{base_id}_PO.pdf", 'data': pdf_po},
                            {'name': f"{base_id}_SI.pdf", 'data': pdf_si},
                            {'name': f"{base_id}_PL.pdf", 'data': pdf_pl},
                            {'name': f"{base_id}_BOL.pdf", 'data': pdf_bol},
                            {'name': f"CustomsCity_{base_id}.csv", 'data': csv_data}
                        ]
                        if attach_backup and os.path.exists('invoices.db'):
                            with open('invoices.db', 'rb') as f: files_to_send.append({'name': f'backup_holistic_{date.today()}.db', 'data': f.read()})
                        
                        success, msg = send_email_with_attachments(sender_email, sender_pass, recipient_email, f"Export Docs: {new_ver}", f"Attached documents for {new_ver}.", files_to_send)
                        if success: 
                            st.balloons()
                            st.success(f"✅ Sent to {recipient_email}")
                            st.rerun()
                        else: st.error(f"Failed: {msg}")

# ==================== PAGE 2: CATALOG ====================
elif page == "Catalog":
    st.header("📦 Product Catalog")
    st.info("Updates here apply to NEW uploads/batches.")
    
    c1, c2 = st.columns(2)
    with c1:
        curr_cat = get_catalog()
        if not curr_cat.empty: st.download_button("Download Catalog", curr_cat.to_csv(index=False).encode(), "catalog.csv", key="cat_dl_btn")
    with c2:
        up_cat = st.file_uploader("Upload Catalog", type=['csv'])
        if up_cat:
            upsert_catalog_from_df(pd.read_csv(up_cat))
            st.success("Updated!"); show_backup_prompt("cat_up"); st.rerun()
            
    if not curr_cat.empty:
        edited = st.data_editor(curr_cat, num_rows="dynamic")
        if st.button("💾 Save Catalog Changes"):
            upsert_catalog_from_df(edited)
            st.success("Saved! NOW DOWNLOAD BACKUP ->"); show_backup_prompt("cat_save")

# ==================== PAGE 3: HISTORY ====================
elif page == "Archive (History)":
    st.header("🗄️ Completed Invoices")
    st.dataframe(get_history(), use_container_width=True)
    st.markdown("---")
    st.header("📂 Completed Batches")
    completed = get_batches(status='Completed')
    st.dataframe(completed[['batch_name', 'updated_at', 'status']], use_container_width=True)
