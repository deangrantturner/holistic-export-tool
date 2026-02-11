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

# --- GLOBAL DEFAULTS (UPDATED) ---
DEFAULT_SHIPPER = """Holistic Roasters inc.
3780 St-Patrick
Montreal, QC, Canada H4E 1A2
BN/GST: 780810917RC0001
TVQ: 1225279701TQ0001"""

# UPDATED CONSIGNEE DEFAULTS
DEF_CONS_NAME = "Border Mail Depot"
DEF_CONS_ADDR = "102 W. Service Road"
DEF_CONS_CITY = "Champlain"
DEF_CONS_STATE = "NY"
DEF_CONS_ZIP = "12919"
DEF_CONS_OTHER = "IRS# 461729644"
DEFAULT_CONSIGNEE_FULL = "Border Mail Depot\n102 W. Service Road\nChamplain, NY 12919\nIRS# 461729644\nUnited States"

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
                  weight_lbs REAL,
                  unit_price REAL,
                  country_of_origin TEXT,
                  product_id TEXT)''')
    
    # MIGRATIONS
    try: c.execute("ALTER TABLE product_catalog_v3 ADD COLUMN unit_price REAL")
    except: pass 
    try: c.execute("ALTER TABLE product_catalog_v3 ADD COLUMN country_of_origin TEXT")
    except: pass
    try: c.execute("ALTER TABLE product_catalog_v3 ADD COLUMN product_id TEXT")
    except: pass
    
    c.execute('''CREATE TABLE IF NOT EXISTS settings
                 (key TEXT PRIMARY KEY,
                  value BLOB)''')
    
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
    df.columns = df.columns.str.strip().str.lower()
    col_map = {
        'price': 'unit_price', 'cost': 'unit_price', 'unit price': 'unit_price',
        'origin': 'country_of_origin', 'country': 'country_of_origin', 'coo': 'country_of_origin', 'country of origin': 'country_of_origin',
        'weight': 'weight_lbs', 'lbs': 'weight_lbs', 'weight (lbs)': 'weight_lbs',
        'sku': 'sku', 'variant code': 'sku', 'variant code / sku': 'sku',
        'product': 'product_name', 'name': 'product_name', 'item variant': 'product_name',
        'desc': 'description', 'description': 'description',
        'hts': 'hts_code', 'hs code': 'hts_code', 'hts code': 'hts_code',
        'fda': 'fda_code', 'fda code': 'fda_code',
        'product id': 'product_id', 'id': 'product_id', 'prod id': 'product_id', 'master sku': 'product_id'
    }
    renamed_cols = {}
    for col in df.columns:
        if col in col_map: renamed_cols[col] = col_map[col]
    df.rename(columns=renamed_cols, inplace=True)

    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    for _, row in df.iterrows():
        p_name = row.get('product_name', '')
        desc = row.get('description', '')
        weight = row.get('weight_lbs', 0.0)
        price = row.get('unit_price', 0.0)
        origin = row.get('country_of_origin', 'CA')
        prod_id = row.get('product_id', '')

        if pd.isna(origin) or origin == "": origin = "CA"
        try: weight = float(weight)
        except: weight = 0.0
        try: price = float(price)
        except: price = 0.0
        if pd.isna(desc) or desc == "": desc = p_name
        
        sku_val = clean_sku(row.get('sku', ''))
        
        if pd.isna(prod_id) or str(prod_id).strip() == "":
            prod_id = sku_val

        if sku_val:
            c.execute("""INSERT OR REPLACE INTO product_catalog_v3 
                         (sku, product_name, description, hts_code, fda_code, weight_lbs, unit_price, country_of_origin, product_id) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                      (sku_val, p_name, desc, str(row.get('hts_code', '')), str(row.get('fda_code', '')), weight, price, str(origin), str(prod_id)))
    conn.commit()
    conn.close()

def clear_catalog():
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    c.execute("DELETE FROM product_catalog_v3")
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

def clear_signature():
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    c.execute("DELETE FROM settings WHERE key='signature'")
    conn.commit()
    conn.close()

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
    
    # Defaults (Now using the updated CONSTANTS at the top)
    saved_notes = get_setting('default_notes')
    def_notes = saved_notes.decode('utf-8') if saved_notes else DEFAULT_NOTES
    saved_carrier = get_setting('default_carrier')
    def_carrier = saved_carrier.decode('utf-8') if saved_carrier else "FX (FedEx)"

    new_data = {
        "inv_number": f"{date.today().strftime('%Y%m%d')}1",
        "inv_date": str(date.today()),
        "discount": 75.0,
        "cons_name": DEF_CONS_NAME,
        "cons_addr": DEF_CONS_ADDR,
        "cons_city": DEF_CONS_CITY,
        "cons_state": DEF_CONS_STATE,
        "cons_zip": DEF_CONS_ZIP,
        "cons_other": DEF_CONS_OTHER,
        "notes": def_notes,
        "carrier": def_carrier,
        "pallets": 1,
        "cartons": 1,
        "gross_weight": 0.0,
        "orders_json": None
    }

    try:
        c.execute("SELECT data FROM batches ORDER BY updated_at DESC LIMIT 1")
        row = c.fetchone()
        if row:
            last_data = json.loads(row[0])
            if 'cons_name' in last_data: new_data['cons_name'] = last_data['cons_name']
            if 'cons_addr' in last_data: new_data['cons_addr'] = last_data['cons_addr']
            if 'cons_city' in last_data: new_data['cons_city'] = last_data['cons_city']
            if 'cons_state' in last_data: new_data['cons_state'] = last_data['cons_state']
            if 'cons_zip' in last_data: new_data['cons_zip'] = last_data['cons_zip']
            if 'cons_other' in last_data: new_data['cons_other'] = last_data['cons_other']
            
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

st.set_page_config(page_title="Holistic Roasters Export Hub", layout="wide")

st.markdown("""
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;600;700&family=Open+Sans:wght@400;600&display=swap');
        .stApp { background-color: #FAFAFA; font-family: 'Open Sans', sans-serif; }
        h1, h2, h3 { font-family: 'Montserrat', sans-serif !important; color: #6F4E37 !important; font-weight: 700; }
        div.stButton > button { background-color: #6F4E37 !important; color: white !important; border-radius: 8px !important; border: none !important; font-family: 'Montserrat', sans-serif !important; font-weight: 600 !important; }
        div.stButton > button:hover { background-color: #5A3E2B !important; }
        .stTextInput input, .stTextArea textarea, .stDateInput input, .stNumberInput input { border-radius: 8px !important; border: 1px solid #D0D0D0 !important; }
        .stTextInput input:focus, .stTextArea textarea:focus { border-color: #6F4E37 !important; box-shadow: 0 0 0 1px #6F4E37 !important; }
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
            with open("invoices.db", "wb") as f: f.write(uploaded_db.getvalue())
            st.sidebar.success("✅ Restored! Reloading page..."); time.sleep(1); st.rerun()
        except Exception as e: st.sidebar.error(f"Error: {e}")

st.title("☕ Holistic Roasters Export Hub")
st.sidebar.header("📁 Workflow")
page = st.sidebar.radio("Go to:", ["Batches (Dashboard)", "Catalog", "Archive (History)"])

# --- PDF Class ---
class ProInvoice(FPDF):
    def header(self): pass
    def footer(self):
        self.set_y(-15); self.set_font('Helvetica', 'I', 8); self.cell(0, 10, f'Page {self.page_no()} of {{nb}}', 0, 0, 'R')

# --- PDF Generators (COMMERCIAL INVOICE UPDATED) ---
def generate_ci_pdf(doc_type, df, inv_num, inv_date, addr_from, addr_to, addr_ship, notes, total_val, sig_bytes=None, signer_name="Dean Turner"):
    pdf = ProInvoice(); pdf.alias_nb_pages(); pdf.add_page(); pdf.set_auto_page_break(auto=False)
    pdf.set_font('Helvetica', 'B', 20); pdf.cell(0, 10, doc_type, 0, 1, 'C'); pdf.ln(5)
    pdf.set_font("Helvetica", '', 9); y_start = pdf.get_y()
    pdf.set_xy(10, y_start); pdf.set_font("Helvetica", 'B', 10); pdf.cell(70, 5, "SHIPPER / EXPORTER:", 0, 1); pdf.set_x(10); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(70, 4, addr_from)
    pdf.set_xy(90, y_start); pdf.set_font("Helvetica", 'B', 10); pdf.cell(70, 5, "CONSIGNEE (SHIP TO):", 0, 1); pdf.set_xy(90, pdf.get_y()); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(70, 4, addr_ship)
    pdf.set_xy(160, y_start); pdf.set_font("Helvetica", 'B', 12); pdf.cell(40, 6, f"Invoice #: {inv_num}", 0, 1, 'R'); pdf.set_x(160); pdf.set_font("Helvetica", '', 10); pdf.cell(40, 6, f"Date: {inv_date}", 0, 1, 'R'); pdf.set_x(160); pdf.cell(40, 6, "Currency: USD", 0, 1, 'R')
    y_mid = max(pdf.get_y(), 60) + 10; pdf.set_xy(10, y_mid); pdf.set_font("Helvetica", 'B', 10); pdf.cell(80, 5, "IMPORTER OF RECORD:", 0, 1); pdf.set_x(10); pdf.set_font("Helvetica", '', 9); pdf.multi_cell(80, 4, addr_to)
    pdf.set_xy(100, y_mid); pdf.set_fill_color(245, 245, 245); pdf.rect(100, y_mid, 95, 30, 'F')
    pdf.set_xy(102, y_mid + 2); pdf.set_font("Helvetica", 'B', 9); pdf.cell(50, 5, "NOTES / BROKER / FDA:", 0, 1); pdf.set_xy(102, pdf.get_y()); pdf.set_font("Helvetica", '', 8); pdf.multi_cell(90, 4, notes); pdf.set_y(y_mid + 35)
    
    # Updated Table Columns: Added UNIT WT
    w = [10, 40, 20, 20, 12, 15, 20, 25]; headers = ["QTY", "DESCRIPTION", "HTS #", "FDA", "ORIGIN", "UNIT WT", "UNIT ($)", "TOTAL ($)"]
    
    pdf.set_font("Helvetica", 'B', 7); pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers): pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln(); pdf.set_font("Helvetica", '', 7)
    
    def get_lines(text, width):
        if not text: return 1
        lines = 0; 
        for para in str(text).split('\n'):
            if not para: lines += 1; continue
            words = para.split(' '); curr_w = 0; lines_para = 1
            for word in words:
                word_w = pdf.get_string_width(word + " ")
                if curr_w + word_w > width: lines_para += 1; curr_w = word_w
                else: curr_w += word_w
            lines += lines_para
        return lines

    line_h = 5
    for _, row in df.iterrows():
        origin = str(row.get('country_of_origin', 'CA'))
        desc = str(row['Description'])
        
        d_row = [
            (str(int(row['Quantity'])), 'C'), 
            (desc, 'L'), 
            (str(row.get('HTS Code','')), 'C'), 
            (str(row.get('FDA Code','')), 'C'), 
            (origin, 'C'), 
            (f"{row.get('Weight (lbs)', 0):.2f} lbs", 'C'), 
            (f"{row['Transfer Price (Unit)']:.2f}", 'R'), 
            (f"{row['Transfer Total']:.2f}", 'R')
        ]
        
        max_lines = 1
        for i, (txt, align) in enumerate(d_row):
            lines = get_lines(txt, w[i] - 2); 
            if lines > max_lines: max_lines = lines
        row_h = max_lines * line_h
        
        if pdf.get_y() + row_h > 270:
            pdf.add_page(); pdf.set_font("Helvetica", 'B', 7); pdf.set_fill_color(220, 220, 220)
            for i, h in enumerate(headers): pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
            pdf.ln(); pdf.set_font("Helvetica", '', 7)
        
        y_curr = pdf.get_y(); x_curr = 10
        for i, (txt, align) in enumerate(d_row):
            pdf.set_xy(x_curr + sum(w[:i]), y_curr); pdf.multi_cell(w[i], line_h, txt, 0, align)
        pdf.set_xy(10, y_curr)
        for i in range(len(w)): pdf.rect(10 + sum(w[:i]), y_curr, w[i], row_h)
        pdf.set_y(y_curr + row_h)

    pdf.ln(2); pdf.set_font("Helvetica", 'B', 9); pdf.cell(sum(w[:-1]), 8, "TOTAL VALUE (USD):", 0, 0, 'R'); pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    
    # --- FIX: Ensure Signature is Below Text ---
    pdf.ln(10) # Add vertical spacing before declaration
    pdf.set_font("Helvetica", '', 10)
    pdf.cell(0, 5, "I declare that all information contained in this invoice to be true and correct.", 0, 1, 'L')
    
    pdf.ln(15) # Add spacing specifically for the signature image
    y_sig_line = pdf.get_y()
    
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(0, 5, signer_name, 0, 1, 'L')
    
    if sig_bytes:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp: tmp.write(sig_bytes); tmp_path = tmp.name
        try: 
            # Place signature ABOVE the name line, but below the declaration text
            pdf.image(tmp_path, x=10, y=y_sig_line - 15, w=40) 
        except: pass
        os.unlink(tmp_path)
    return bytes(pdf.output())

# ... (SI, PL, PO, BOL Generators same as before) ...

def generate_si_pdf(df, inv_num, inv_date, addr_from, addr_to, addr_ship, notes, total_val, sig_bytes, signer_name):
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
    
    def get_lines(text, width):
        if not text: return 1
        lines = 0; 
        for para in str(text).split('\n'):
            if not para: lines += 1; continue
            words = para.split(' '); curr_w = 0; lines_para = 1
            for word in words:
                word_w = pdf.get_string_width(word + " ")
                if curr_w + word_w > width: lines_para += 1; curr_w = word_w
                else: curr_w += word_w
            lines += lines_para
        return lines

    line_h = 5
    for _, row in df.iterrows():
        d_row = [(str(int(row['Quantity'])), 'C'), (str(row['Description']), 'L'), (f"{row['Transfer Price (Unit)']:.2f}", 'R'), (f"{row['Transfer Total']:.2f}", 'R')]
        max_lines = 1
        for i, (txt, align) in enumerate(d_row):
            lines = get_lines(txt, w[i] - 2)
            if lines > max_lines: max_lines = lines
        row_h = max_lines * line_h
        if pdf.get_y() + row_h > 270:
            pdf.add_page(); pdf.set_font("Helvetica", 'B', 7); pdf.set_fill_color(220, 220, 220)
            for i, h in enumerate(headers): pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
            pdf.ln(); pdf.set_font("Helvetica", '', 7)
        y_curr = pdf.get_y(); x_curr = 10
        for i, (txt, align) in enumerate(d_row):
            pdf.set_xy(x_curr + sum(w[:i]), y_curr); pdf.multi_cell(w[i], line_h, txt, 0, align)
        pdf.set_xy(10, y_curr)
        for i in range(len(w)): pdf.rect(10 + sum(w[:i]), y_curr, w[i], row_h)
        pdf.set_y(y_curr + row_h)

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
    
    def get_lines(text, width):
        if not text: return 1
        lines = 0; 
        for para in str(text).split('\n'):
            if not para: lines += 1; continue
            words = para.split(' '); curr_w = 0; lines_para = 1
            for word in words:
                word_w = pdf.get_string_width(word + " ")
                if curr_w + word_w > width: lines_para += 1; curr_w = word_w
                else: curr_w += word_w
            lines += lines_para
        return lines

    line_h = 5
    for _, row in df.iterrows():
        d_row = [(str(int(row['Quantity'])), 'C'), (str(row['Description']), 'L')]
        max_lines = 1
        for i, (txt, align) in enumerate(d_row):
            lines = get_lines(txt, w[i] - 2)
            if lines > max_lines: max_lines = lines
        row_h = max_lines * line_h
        
        if pdf.get_y() + row_h > 270:
            pdf.add_page(); pdf.set_font("Helvetica", 'B', 7); pdf.set_fill_color(220, 220, 220)
            for i, h in enumerate(headers): pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
            pdf.ln(); pdf.set_font("Helvetica", '', 7)
            
        y_curr = pdf.get_y(); x_curr = 10
        for i, (txt, align) in enumerate(d_row):
            pdf.set_xy(x_curr + sum(w[:i]), y_curr)
            pdf.multi_cell(w[i], line_h, txt, 0, align)
        pdf.set_xy(10, y_curr)
        for i in range(len(w)): pdf.rect(10 + sum(w[:i]), y_curr, w[i], row_h)
        pdf.set_y(y_curr + row_h)

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
    
    def get_lines(text, width):
        if not text: return 1
        lines = 0; 
        for para in str(text).split('\n'):
            if not para: lines += 1; continue
            words = para.split(' '); curr_w = 0; lines_para = 1
            for word in words:
                word_w = pdf.get_string_width(word + " ")
                if curr_w + word_w > width: lines_para += 1; curr_w = word_w
                else: curr_w += word_w
            lines += lines_para
        return lines

    line_h = 5
    for _, row in df.iterrows():
        d_row = [(str(int(row['Quantity'])), 'C'), (str(row['Description']), 'L'), (f"{row['Transfer Price (Unit)']:.2f}", 'R'), (f"{row['Transfer Total']:.2f}", 'R')]
        max_lines = 1
        for i, (txt, align) in enumerate(d_row):
            lines = get_lines(txt, w[i] - 2)
            if lines > max_lines: max_lines = lines
        row_h = max_lines * line_h
        
        if pdf.get_y() + row_h > 270:
            pdf.add_page(); pdf.set_font("Helvetica", 'B', 7); pdf.set_fill_color(220, 220, 220)
            for i, h in enumerate(headers): pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
            pdf.ln(); pdf.set_font("Helvetica", '', 7)
            
        y_curr = pdf.get_y(); x_curr = 10
        for i, (txt, align) in enumerate(d_row):
            pdf.set_xy(x_curr + sum(w[:i]), y_curr)
            pdf.multi_cell(w[i], line_h, txt, 0, align)
        pdf.set_xy(10, y_curr)
        for i in range(len(w)): pdf.rect(10 + sum(w[:i]), y_curr, w[i], row_h)
        pdf.set_y(y_curr + row_h)

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
        # Grid Row Helper
        def print_grid_row(data_list):
            def get_lines(text, width):
                if not text: return 1
                lines = 0; 
                for para in str(text).split('\n'):
                    if not para: lines += 1; continue
                    words = para.split(' '); curr_w = 0; lines_para = 1
                    for word in words:
                        word_w = pdf.get_string_width(word + " ")
                        if curr_w + word_w > width: lines_para += 1; curr_w = word_w
                        else: curr_w += word_w
                    lines += lines_para
                return lines
            line_h = 5; max_lines = 1
            for i, (txt, align) in enumerate(data_list):
                lines = get_lines(txt, w[i] - 2)
                if lines > max_lines: max_lines = lines
            row_h = max_lines * line_h
            y_start = pdf.get_y(); x_start = 10
            for i, (txt, align) in enumerate(data_list):
                current_x = x_start + sum(w[:i])
                pdf.set_xy(current_x, y_start); pdf.multi_cell(w[i], line_h, txt, 0, align)
            pdf.set_xy(x_start, y_start)
            for i in range(len(w)): pdf.rect(x_start + sum(w[:i]), y_start, w[i], row_h)
            pdf.set_xy(x_start, y_start + row_h)

        row1 = [("", 'C'), (f"{pallets} PLT", 'C'), ("ROASTED COFFEE (NMFC 056820)", 'L'), (f"{total_weight_lbs:.1f} lbs", 'R'), ("60", 'C')]
        print_grid_row(row1)
        row2 = [("", 'C'), (f"{cartons} CTN", 'C'), ("(Contains roasted coffee in bags)", 'L'), ("", 'R'), ("", 'C')]
        print_grid_row(row2)
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

def generate_customscity_csv(df, inv_number, inv_date, c_name, c_addr, c_city, c_state, c_zip, hbol_number, carrier_code):
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
            'Shipper City': 'MONTREAL', 'Shipper Country': 'CA', 'Consignee Name': c_name,
            'Consignee Address': c_addr.replace('\n', ', '),
            'Consignee City': c_city, 'Consignee State or Province': c_state, 'Consignee Postal Code': c_zip, 'Consignee Country': 'US',
            'Description': row['Description'], 'Product ID': row.get('product_id', 'VARIOUS'), 'Carrier Name': carrier_code, 'Vessel Name': '',
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
            if saved_sig: 
                st.success("Signature Loaded")
                if st.button("🗑️ Clear Signature", key=f"clear_sig_{batch_id}"):
                    clear_signature()
                    st.rerun()
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
            
            with c2:
                st.markdown("**Consignee Details**")
                c_name = st.text_input("Consignee Name", value=batch_data.get('cons_name', DEF_CONS_NAME))
                c_addr = st.text_input("Street Address", value=batch_data.get('cons_addr', DEF_CONS_ADDR))
                c3a, c3b, c3c = st.columns(3)
                with c3a: c_city = st.text_input("City", value=batch_data.get('cons_city', DEF_CONS_CITY))
                with c3b: c_state = st.text_input("State", value=batch_data.get('cons_state', DEF_CONS_STATE))
                with c3c: c_zip = st.text_input("Zip Code", value=batch_data.get('cons_zip', DEF_CONS_ZIP))
                c_other = st.text_input("Other Info (e.g. IRS #)", value=batch_data.get('cons_other', DEF_CONS_OTHER))
                lines = [c_name, c_addr, f"{c_city}, {c_state} {c_zip}", c_other, "United States"]
                full_consignee_txt = "\n".join([L for L in lines if L and L.strip()])
                b_notes = st.text_area("Notes", value=batch_data.get('notes', ""), height=100)

            st.subheader("📦 Orders")
            cat_check = get_catalog()
            if cat_check.empty: st.error("🔴 Catalog is EMPTY. Please Restore Backup or Upload Catalog in the 'Catalog' tab.")
            else: st.success(f"✅ Catalog Loaded ({len(cat_check)} items)")

            saved_orders_json = batch_data.get('orders_json')
            uploaded_file = st.file_uploader("Upload CSV", type=['csv'])
            
            df = pd.DataFrame()
            unique_orders_count = 1
            
            if uploaded_file:
                try:
                    raw_df = pd.read_csv(uploaded_file)
                    if 'Ship to country' in raw_df.columns:
                        us_shipments = raw_df[raw_df['Ship to country'] == 'United States'].copy()
                        if 'Item type' in raw_df.columns: us_shipments = us_shipments[us_shipments['Item type'] == 'product']
                        
                        possible_cols = ['SO #', 'Name', 'Order Name', 'Order Number']
                        order_col = next((col for col in possible_cols if col in us_shipments.columns), None)
                        if order_col: unique_orders_count = us_shipments[order_col].nunique()
                        
                        sales_data = us_shipments[['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit']].copy()
                        sales_data['Variant code / SKU'] = sales_data['Variant code / SKU'].astype(str).str.strip()
                        sales_data['CSV_Price'] = pd.to_numeric(sales_data['Price per unit'], errors='coerce').fillna(0)
                        
                        if not cat_check.empty:
                            cat_check['sku'] = cat_check['sku'].apply(clean_sku)
                            merged = pd.merge(sales_data, cat_check, left_on='Variant code / SKU', right_on='sku', how='left')
                            merged['Product Name'] = merged['product_name'].fillna(merged['Item variant'])
                            merged['Description'] = merged['description'].fillna(merged['Product Name'])
                            merged['HTS Code'] = merged['hts_code'].fillna(DEFAULT_HTS)
                            merged['FDA Code'] = merged['fda_code'].fillna(DEFAULT_FDA)
                            merged['Weight (lbs)'] = merged['weight_lbs'].fillna(0.0)
                            merged['unit_price'] = pd.to_numeric(merged['unit_price'], errors='coerce').fillna(0.0)
                            merged['Transfer Price (Unit)'] = merged.apply(lambda x: x['unit_price'] if x['unit_price'] > 0 else x['CSV_Price'], axis=1)
                            if 'country_of_origin' not in merged.columns: merged['country_of_origin'] = "CA"
                            if 'product_id' not in merged.columns: merged['product_id'] = merged['Variant code / SKU']
                            df = merged 
                        else:
                            sales_data['Product Name'] = sales_data['Item variant']
                            sales_data['Description'] = sales_data['Item variant']
                            sales_data['HTS Code'] = DEFAULT_HTS
                            sales_data['FDA Code'] = DEFAULT_FDA
                            sales_data['Weight (lbs)'] = 0.0
                            sales_data['Transfer Price (Unit)'] = sales_data['CSV_Price']
                            sales_data['country_of_origin'] = "CA"
                            sales_data['product_id'] = sales_data['Variant code / SKU']
                            df = sales_data
                    else: st.error("Invalid CSV"); df = pd.DataFrame()
                except Exception as e: st.error(f"Error reading CSV: {e}")
            elif saved_orders_json:
                try: df = pd.read_json(io.StringIO(saved_orders_json), orient='split')
                except: st.error("Failed to load saved orders."); df = pd.DataFrame()

        st.markdown("---")
        if not df.empty:
            df['Transfer Total'] = df['Quantity'] * df['Transfer Price (Unit)']
            
            df['FDA Code'] = df['FDA Code'].fillna("N/A")
            df['country_of_origin'] = df['country_of_origin'].fillna("N/A")
            df['product_id'] = df['product_id'].fillna("N/A")

            consolidated = df.groupby(['product_id', 'HTS Code', 'Weight (lbs)', 'country_of_origin', 'FDA Code']).agg({
                'Quantity': 'sum',
                'Transfer Total': 'sum',
                'Product Name': 'first',
                'Description': 'first',
                'Variant code / SKU': lambda x: ', '.join(x.unique()) if len(x.unique()) < 3 else 'VARIOUS'
            }).reset_index()
            
            consolidated['Transfer Price (Unit)'] = consolidated['Transfer Total'] / consolidated['Quantity']
            
            edited_df = st.data_editor(consolidated, num_rows="dynamic", use_container_width=True,
                column_config={"Transfer Price (Unit)": st.column_config.NumberColumn("Unit Price ($)", format="$%.2f"),
                               "Transfer Total": st.column_config.NumberColumn("Total ($)", format="$%.2f")})
            total_val = edited_df['Transfer Total'].sum()
            st.metric("Total Value", f"${total_val:,.2f}")
            
            c_log1, c_log2, c_log3 = st.columns(3)
            with c_log1: pallets = st.number_input("Pallets", value=batch_data.get('pallets', 1))
            saved_cartons = batch_data.get('cartons', 1)
            default_cartons = unique_orders_count if uploaded_file and unique_orders_count > 1 else saved_cartons
            with c_log2: cartons = st.number_input("Cartons", value=default_cartons)
            calc_w = (edited_df['Quantity'] * edited_df['Weight (lbs)']).sum()
            saved_gw = batch_data.get('gross_weight', 0.0)
            default_gw = calc_w + (pallets * 40) if saved_gw == 0.0 or uploaded_file else saved_gw
            with c_log3: gross_weight = st.number_input("Gross Weight", value=float(default_gw))

            st.markdown("---")
            if st.button("💾 SAVE BATCH PROGRESS", type="primary"):
                save_data = {
                    "inv_number": b_inv_num, "inv_date": str(b_date), 
                    "cons_name": c_name, "cons_addr": c_addr, "cons_city": c_city, "cons_state": c_state, "cons_zip": c_zip, "cons_other": c_other,
                    "notes": b_notes, "carrier": sel_carrier,
                    "pallets": pallets, "cartons": cartons, "gross_weight": gross_weight,
                    "orders_json": edited_df.to_json(orient='split')
                }
                update_batch(batch_id, save_data)
                st.success("✅ Saved! You can close the tab safely."); show_backup_prompt("batch_save")
                
            st.subheader("🖨️ Documents")
            base_id = b_inv_num; hbol = f"HRUS{base_id}"
            
            pdf_ci = generate_ci_pdf("COMMERCIAL INVOICE", edited_df, f"CI-HRUS{base_id}", b_date, DEFAULT_SHIPPER, DEFAULT_IMPORTER, full_consignee_txt, b_notes, total_val, get_signature(), "Dean Turner")
            pdf_po = generate_po_pdf(edited_df, f"PO-HRUS{base_id}", b_date, DEFAULT_IMPORTER, DEFAULT_SHIPPER, full_consignee_txt, total_val)
            pdf_si = generate_si_pdf(edited_df, f"SI-HRUS{base_id}", b_date, DEFAULT_SHIPPER, DEFAULT_IMPORTER, full_consignee_txt, b_notes, total_val, get_signature(), "Dean Turner")
            pdf_pl = generate_pl_pdf(edited_df, f"PL-HRUS{base_id}", b_date, DEFAULT_SHIPPER, DEFAULT_IMPORTER, full_consignee_txt, cartons)
            pdf_bol = generate_bol_pdf(edited_df, b_inv_num, b_date, DEFAULT_SHIPPER, full_consignee_txt, carrier_name, hbol, pallets, cartons, gross_weight, get_signature())
            
            csv_data = generate_customscity_csv(edited_df, b_inv_num, b_date, c_name, c_addr, c_city, c_state, c_zip, hbol, carrier_code)
            
            c1, c2, c3, c4, c5 = st.columns(5)
            with c1: st.download_button("CI PDF", pdf_ci, f"CI-HRUS{base_id}.pdf", key=f"dl_ci_{batch_id}")
            with c2: st.download_button("PO PDF", pdf_po, f"PO-HRUS{base_id}.pdf", key=f"dl_po_{batch_id}")
            with c3: st.download_button("SI PDF", pdf_si, f"SI-HRUS{base_id}.pdf", key=f"dl_si_{batch_id}")
            with c4: st.download_button("PL PDF", pdf_pl, f"PL-HRUS{base_id}.pdf", key=f"dl_pl_{batch_id}")
            with c5: st.download_button("BOL PDF", pdf_bol, f"BOL-HRUS{base_id}.pdf", key=f"dl_bol_{batch_id}")
            
            c_csv_btn, c_csv_link = st.columns([1.5, 2])
            with c_csv_btn: st.download_button("📥 CustomsCity CSV", csv_data, f"CustomsCity_{base_id}.csv", type="primary", key=f"dl_csv_{batch_id}")
            with c_csv_link: st.markdown("""<div style="margin-top: 8px;"><a href="https://app.customscity.com/upload/document/" target="_blank" style="font-weight: 600; color: #6F4E37; text-decoration: none;">🚀 Upload to CustomsCity</a></div>""", unsafe_allow_html=True)
            
            # EMAIL CENTER (Omitted for brevity, same as previous)
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
                    st.success("Saved!"); show_backup_prompt("email_creds")

            recipient_email = st.text_input("Send To:", value="dean.turner@holisticroasters.com")
            attach_backup = st.checkbox("Attach App Backup (invoices.db)", value=True, help="Useful for restoring settings if the app reboots.")

            if st.button("📧 Email All Documents", type="primary"):
                if not sender_pass: st.error("Please enter your App Password in the settings above.")
                else:
                    new_ver = b_inv_num; save_invoice_metadata(b_inv_num, total_val, DEFAULT_IMPORTER.split('\n')[0]); finalize_batch_in_db(batch_id)
                    files_to_send = [
                        {'name': f"CI-HRUS{base_id}.pdf", 'data': pdf_ci},
                        {'name': f"PO-HRUS{base_id}.pdf", 'data': pdf_po},
                        {'name': f"SI-HRUS{base_id}.pdf", 'data': pdf_si},
                        {'name': f"PL-HRUS{base_id}.pdf", 'data': pdf_pl},
                        {'name': f"BOL-HRUS{base_id}.pdf", 'data': pdf_bol},
                        {'name': f"CustomsCity_{base_id}.csv", 'data': csv_data}
                    ]
                    if attach_backup and os.path.exists('invoices.db'):
                        with open('invoices.db', 'rb') as f: files_to_send.append({'name': f'backup_holistic_{date.today()}.db', 'data': f.read()})
                    success, msg = send_email_with_attachments(sender_email, sender_pass, recipient_email, f"Export Docs: {new_ver}", f"Attached documents for {new_ver}.", files_to_send)
                    if success: st.balloons(); st.success(f"✅ Sent to {recipient_email}"); st.rerun()
                    else: st.error(f"Failed: {msg}")

# ==================== PAGE 2: CATALOG ====================
elif page == "Catalog":
    st.header("📦 Product Catalog")
    st.info("Updates here apply to NEW uploads/batches.")
    
    c1, c2, c3 = st.columns(3)
    with c1:
        template_df = pd.DataFrame(columns=['sku', 'product_name', 'description', 'hts_code', 'fda_code', 'weight_lbs', 'unit_price', 'country_of_origin', 'product_id'])
        st.download_button("Download Template", template_df.to_csv(index=False).encode(), "catalog_template.csv", key="cat_dl_temp")
        curr_cat = get_catalog()
        if not curr_cat.empty: st.download_button("Download Current Catalog", curr_cat.to_csv(index=False).encode(), "catalog.csv", key="cat_dl_btn")
    with c2:
        up_cat = st.file_uploader("Upload Catalog", type=['csv'])
        if up_cat:
            upsert_catalog_from_df(pd.read_csv(up_cat)); st.success("Updated!"); show_backup_prompt("cat_up"); st.rerun()
    with c3:
        if st.button("⚠️ Clear Catalog"): clear_catalog(); st.rerun()
            
    if not curr_cat.empty:
        edited = st.data_editor(curr_cat, num_rows="dynamic", use_container_width=True,
            column_config={"unit_price": st.column_config.NumberColumn("Price ($)", format="$%.2f"),
                           "weight_lbs": st.column_config.NumberColumn("Weight (lbs)", format="%.2f"),
                           "country_of_origin": st.column_config.TextColumn("Origin (e.g. CA)")})
        if st.button("💾 Save Catalog Changes"): upsert_catalog_from_df(edited); st.success("Saved! NOW DOWNLOAD BACKUP ->"); show_backup_prompt("cat_save")

# ==================== PAGE 3: HISTORY ====================
elif page == "Archive (History)":
    st.header("🗄️ Completed Invoices")
    st.dataframe(get_history(), use_container_width=True)
    st.markdown("---")
    st.header("📂 Completed Batches")
    completed = get_batches(status='Completed')
    st.dataframe(completed[['batch_name', 'updated_at', 'status']], use_container_width=True)
