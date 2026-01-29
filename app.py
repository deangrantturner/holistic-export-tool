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
import random

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
    
    try:
        c.execute("ALTER TABLE product_catalog_v3 ADD COLUMN weight_lbs REAL")
    except:
        pass 
                  
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
        weight = row.get('weight_lbs', 0.0)
        try: weight = float(weight)
        except: weight = 0.0

        if pd.isna(desc) or desc == "": desc = p_name
            
        c.execute("""INSERT OR REPLACE INTO product_catalog_v3 
                     (sku, product_name, description, hts_code, fda_code, weight_lbs) 
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  (str(row['sku']), p_name, desc, str(row['hts_code']), str(row['fda_code']), weight))
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

        .stApp { background-color: #FAFAFA; font-family: 'Open Sans', sans-serif; }
        
        div[data-testid="stTabs"] > div:first-child {
            position: sticky !important; top: 0px !important; z-index: 99999 !important;
            background-color: #FAFAFA !important; padding-top: 15px; padding-bottom: 10px;
            border-bottom: 2px solid #E0E0E0; box-shadow: 0px 4px 6px rgba(0,0,0,0.05);
        }

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

# --- STANDARD GENERATOR (For CI / PO) ---
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
    
    # --- HELPER: Exact Line Counter ---
    def get_lines_needed(text, width):
        if not text: return 1
        lines = 0
        for para in str(text).split('\n'):
            if not para:
                lines += 1
                continue
            words = para.split(' ')
            curr_w = 0
            lines_para = 1
            for word in words:
                word_w = pdf.get_string_width(word + " ")
                if curr_w + word_w > width:
                    lines_para += 1
                    curr_w = word_w
                else:
                    curr_w += word_w
            lines += lines_para
        return lines

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
            lines = get_lines_needed(txt, w[i] - 2) 
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

# --- NEW: SALES INVOICE GENERATOR (SIMPLIFIED) ---
def generate_si_pdf(df, inv_num, inv_date, addr_from, addr_to, addr_ship, notes, total_val, sig_bytes=None, signer_name="Dean Turner"):
    pdf = ProInvoice()
    pdf.add_page()
    pdf.set_auto_page_break(auto=False)
    
    # --- HEADER ---
    pdf.set_font('Helvetica', 'B', 20)
    pdf.cell(0, 10, "SALES INVOICE", 0, 1, 'C')
    pdf.ln(5)

    # --- INFO BLOCKS ---
    pdf.set_font("Helvetica", '', 9)
    y_start = pdf.get_y()
    
    # Column 1
    pdf.set_xy(10, y_start)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(70, 5, "SHIPPER / EXPORTER:", 0, 1)
    pdf.set_x(10) 
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(70, 4, addr_from)
    y_end_1 = pdf.get_y()

    # Column 2
    pdf.set_xy(90, y_start) 
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(70, 5, "SHIP TO:", 0, 1)
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
    y_end_3 = pdf.get_y()

    # Row 2 (Bill To only, no Notes Box)
    y_mid = max(y_end_1, y_end_2, y_end_3) + 10
    
    pdf.set_xy(10, y_mid)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(80, 5, "BILL TO:", 0, 1)
    pdf.set_x(10)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(80, 4, addr_to)
    
    pdf.set_y(y_mid + 35)

    # --- TABLE HEADERS (Simplified) ---
    # Widths: QTY, PRODUCT, UNIT, TOTAL
    w = [20, 100, 35, 35] 
    headers = ["QTY", "PRODUCT", "UNIT ($)", "TOTAL ($)"]
    
    pdf.set_font("Helvetica", 'B', 7)
    pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers):
        pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln()
    
    # --- HELPER: Exact Line Counter ---
    def get_lines_needed(text, width):
        if not text: return 1
        lines = 0
        for para in str(text).split('\n'):
            if not para:
                lines += 1
                continue
            words = para.split(' ')
            curr_w = 0
            lines_para = 1
            for word in words:
                word_w = pdf.get_string_width(word + " ")
                if curr_w + word_w > width:
                    lines_para += 1
                    curr_w = word_w
                else:
                    curr_w += word_w
            lines += lines_para
        return lines

    # --- DYNAMIC TABLE ROWS ---
    pdf.set_font("Helvetica", '', 7)
    line_h = 5

    for _, row in df.iterrows():
        qty = str(int(row['Quantity']))
        prod_name = str(row['Product Name'])
        # No Description, HTS, FDA
        price = f"{row['Transfer Price (Unit)']:.2f}"
        tot = f"{row['Transfer Total']:.2f}"
        
        data_row = [
            (qty, 'C'), (prod_name, 'L'), (price, 'R'), (tot, 'R')
        ]
        
        max_lines = 1
        for i, (txt, align) in enumerate(data_row):
            lines = get_lines_needed(txt, w[i] - 2) 
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

# --- BOL GENERATOR ---
def generate_bol_pdf(df, inv_number, inv_date, shipper_txt, consignee_txt, carrier_name, hbol_number, pallets, cartons, total_weight_lbs, sig_bytes=None):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto
