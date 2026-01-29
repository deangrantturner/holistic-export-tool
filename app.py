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
            elif num.startswith(inv_num): count += 1
        
        # No hyphen
        new_version_num = inv_num if count == 0 else f"{inv_num}{count}"
        
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

DEFAULT_NOTES = """CUSTOMS BROKER: Strix (Entry@strixsmart.com)
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

# --- 1. COMMERCIAL INVOICE GENERATOR ---
def generate_ci_pdf(doc_type, df, inv_num, inv_date, addr_from, addr_to, addr_ship, notes, total_val, sig_bytes=None, signer_name="Dean Turner"):
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
    lbl_from = "SHIPPER / EXPORTER:"
    pdf.cell(70, 5, lbl_from, 0, 1)
    pdf.set_x(10) 
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(70, 4, addr_from)
    y_end_1 = pdf.get_y()

    # Column 2
    pdf.set_xy(90, y_start) 
    pdf.set_font("Helvetica", 'B', 10)
    lbl_to = "CONSIGNEE (SHIP TO):"
    pdf.cell(70, 5, lbl_to, 0, 1)
    pdf.set_xy(90, pdf.get_y()) 
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(70, 4, addr_ship)
    y_end_2 = pdf.get_y()

    # Column 3
    x_right = 160
    pdf.set_xy(x_right, y_start)
    pdf.set_font("Helvetica", 'B', 12)
    pdf.cell(40, 6, f"Invoice #: {inv_num}", 0, 1, 'R')
    pdf.set_x(x_right)
    pdf.set_font("Helvetica", '', 10)
    pdf.cell(40, 6, f"Date: {inv_date}", 0, 1, 'R')
    pdf.set_x(x_right)
    pdf.cell(40, 6, "Currency: USD", 0, 1, 'R')
    pdf.set_x(x_right)
    pdf.cell(40, 6, "Origin: CANADA", 0, 1, 'R')
    y_end_3 = pdf.get_y()

    # Row 2
    y_mid = max(y_end_1, y_end_2, y_end_3) + 10
    
    pdf.set_xy(10, y_mid)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_bill = "IMPORTER OF RECORD:"
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

# --- 2. SALES INVOICE GENERATOR ---
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
    pdf.cell(40, 6, f"Invoice #: {inv_num}", 0, 1, 'R')
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
    pdf.cell(sum(w[:-1]), 8, "TOTAL AMOUNT DUE (USD):", 0, 0, 'R')
    pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    
    return bytes(pdf.output())

# --- 3. PURCHASE ORDER GENERATOR ---
def generate_po_pdf(df, inv_num, inv_date, addr_buyer, addr_vendor, addr_ship, total_val):
    pdf = ProInvoice()
    pdf.add_page()
    pdf.set_auto_page_break(auto=False)
    
    # --- HEADER ---
    pdf.set_font('Helvetica', 'B', 20)
    pdf.cell(0, 10, "PURCHASE ORDER", 0, 1, 'C')
    pdf.ln(5)

    pdf.set_font("Helvetica", '', 9)
    y_start = pdf.get_y()
    
    # Column 1
    pdf.set_xy(10, y_start)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(70, 5, "FROM (BUYER):", 0, 1)
    pdf.set_x(10) 
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(70, 4, addr_buyer)
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
    pdf.cell(40, 6, f"Invoice #: {inv_num}", 0, 1, 'R')
    pdf.set_x(x_right)
    pdf.set_font("Helvetica", '', 10)
    pdf.cell(40, 6, f"Date: {inv_date}", 0, 1, 'R')
    pdf.set_x(x_right)
    pdf.cell(40, 6, "Currency: USD", 0, 1, 'R')
    y_end_3 = pdf.get_y()

    y_mid = max(y_end_1, y_end_2, y_end_3) + 10
    
    pdf.set_xy(10, y_mid)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(80, 5, "TO (VENDOR):", 0, 1)
    pdf.set_x(10)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(80, 4, addr_vendor)
    
    pdf.set_y(y_mid + 35)

    # --- TABLE HEADERS ---
    w = [20, 100, 35, 35] 
    headers = ["QTY", "PRODUCT", "UNIT ($)", "TOTAL ($)"]
    
    pdf.set_font("Helvetica", 'B', 7)
    pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers):
        pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln()
    
    def get_lines_needed(text, width):
        if not text: return 1
        lines = 0
        for para in str(text).split('\n'):
            if not para: lines += 1; continue
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

    pdf.set_font("Helvetica", '', 7)
    line_h = 5

    for _, row in df.iterrows():
        qty = str(int(row['Quantity']))
        prod_name = str(row['Product Name'])
        price = f"{row['Transfer Price (Unit)']:.2f}"
        tot = f"{row['Transfer Total']:.2f}"
        
        data_row = [ (qty, 'C'), (prod_name, 'L'), (price, 'R'), (tot, 'R') ]
        
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
    pdf.cell(sum(w[:-1]), 8, "TOTAL (USD):", 0, 0, 'R')
    pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    
    return bytes(pdf.output())

# --- BOL GENERATOR ---
def generate_bol_pdf(df, inv_number, inv_date, shipper_txt, consignee_txt, carrier_pdf_display, hbol_number, pallets, cartons, total_weight_lbs, sig_bytes=None):
    pdf = FPDF()
    for copy_num in range(2):
        pdf.add_page()
        pdf.set_auto_page_break(auto=False)
        
        pdf.set_font('Helvetica', 'B', 18)
        pdf.cell(0, 10, "STRAIGHT BILL OF LADING", 0, 1, 'C')
        pdf.ln(5)
        
        pdf.set_font("Helvetica", '', 10)
        y_top = pdf.get_y()
        
        pdf.set_xy(10, y_top)
        pdf.set_font("Helvetica", 'B', 10)
        pdf.cell(20, 6, "Date:", 0, 0)
        pdf.set_font("Helvetica", '', 10)
        pdf.cell(40, 6, str(inv_date), 0, 0)
        
        # INVOICE # REMOVED
        
        pdf.set_xy(130, y_top) 
        pdf.set_font("Helvetica", 'B', 10)
        pdf.cell(30, 6, "BOL #:", 0, 0, 'R')
        pdf.set_font("Helvetica", '', 10)
        pdf.cell(40, 6, hbol_number, 0, 1, 'R')
        
        pdf.ln(10)
        
        # Helper: Exact Line Counter
        def get_lines_needed(text, width):
            if not text: return 1
            lines = 0
            for para in str(text).split('\n'):
                if not para: lines += 1; continue
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

        y_addr = pdf.get_y()
        
        pdf.set_xy(10, y_addr)
        pdf.set_font("Helvetica", 'B', 11)
        pdf.cell(90, 6, "SHIP FROM (SHIPPER)", 1, 1, 'L', fill=False)
        pdf.set_font("Helvetica", '', 9)
        pdf.multi_cell(0, 5, shipper_txt, 1, 'L')
        
        pdf.ln(5)
        
        pdf.set_x(10)
        pdf.set_font("Helvetica", 'B', 11)
        pdf.cell(0, 6, "SHIP TO (CONSIGNEE)", 1, 1, 'L', fill=False)
        pdf.set_font("Helvetica", '', 9)
        pdf.multi_cell(0, 5, consignee_txt, 1, 'L')
        
        pdf.ln(5)
        
        pdf.set_font("Helvetica", 'B', 11)
        pdf.cell(0, 6, f"CARRIER: {carrier_pdf_display}", 0, 1)
        pdf.ln(5)
        
        w = [15, 25, 100, 30, 20] 
        headers = ["HM", "QTY", "DESCRIPTION OF COMMODITY", "WEIGHT", "CLASS"]
        
        pdf.set_font("Helvetica", 'B', 9)
        pdf.set_fill_color(220, 220, 220)
        for i, h in enumerate(headers):
            pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
        pdf.ln()
        
        def print_grid_row(data_list):
            pdf.set_font("Helvetica", '', 9)
            line_h = 5
            max_lines = 1
            
            for i, (txt, align) in enumerate(data_list):
                lines = get_lines_needed(txt, w[i] - 2)
                if lines > max_lines: max_lines = lines
            
            row_h = max_lines * line_h
            y_start = pdf.get_y()
            x_start = 10
            
            for i, (txt, align) in enumerate(data_list):
                current_x = x_start + sum(w[:i])
                pdf.set_xy(current_x, y_start)
                pdf.multi_cell(w[i], line_h, txt, 0, align)
                
            pdf.set_xy(x_start, y_start)
            for i in range(len(w)):
                current_x = x_start + sum(w[:i])
                pdf.rect(current_x, y_start, w[i], row_h)
                
            pdf.set_xy(x_start, y_start + row_h)

        row1 = [
            ("", 'C'),
            (f"{pallets} PLT", 'C'),
            ("ROASTED COFFEE (NMFC 056820)", 'L'),
            (f"{total_weight_lbs:.1f} lbs", 'R'),
            ("60", 'C')
        ]
        print_grid_row(row1)
        
        row2 = [
            ("", 'C'),
            (f"{cartons} CTN", 'C'),
            ("(Contains roasted coffee in bags)", 'L'),
            ("", 'R'),
            ("", 'C')
        ]
        print_grid_row(row2)
        
        pdf.ln(10)
        
        pdf.set_font("Helvetica", '', 8)
        legal = ("RECEIVED, subject to the classifications and tariffs in effect on the date of the issue of this Bill of Lading, "
                 "the property described above in apparent good order, except as noted (contents and condition of contents of packages unknown), "
                 "marked, consigned, and destined as indicated above which said carrier (the word carrier being understood throughout this contract "
                 "as meaning any person or corporation in possession of the property under the contract) agrees to carry to its usual place of delivery "
                 "at said destination.")
        pdf.multi_cell(0, 4, legal)
        
        pdf.ln(15)
        
        y_sig = pdf.get_y()
        
        pdf.line(10, y_sig, 90, y_sig)   # Shipper Line
        pdf.line(110, y_sig, 190, y_sig) # Carrier Line
        
        pdf.set_font("Helvetica", 'B', 8)
        pdf.set_xy(10, y_sig + 2)
        pdf.cell(80, 4, "SHIPPER SIGNATURE / DATE", 0, 0)
        
        pdf.set_xy(110, y_sig + 2)
        pdf.cell(80, 4, "CARRIER SIGNATURE / DATE", 0, 1)
        
        if sig_bytes:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
                tmp.write(sig_bytes)
                tmp_path = tmp.name
            try:
                pdf.image(tmp_path, x=15, y=y_sig-15, w=35) 
            except:
                pass
            os.unlink(tmp_path)
    
    return bytes(pdf.output())

# --- CUSTOMSCITY CSV GENERATOR ---
def generate_customscity_csv(df, inv_number, inv_date, ship_to_txt, hbol_number, carrier_code):
    # UPDATED: Smarter address parsing
    lines = [L.strip() for L in ship_to_txt.split('\n') if L.strip()]
    c_name = lines[0] if len(lines) > 0 else ""
    c_addr = ""
    c_city = ""
    c_state = ""
    c_zip = ""
    c_country = "US"
    
    if len(lines) >= 2:
        # Last line typically City, State Zip Country
        last_line = lines[-1]
        
        # If last line is just country, step back
        if last_line.upper() in ["UNITED STATES", "USA", "US"]:
            c_country = "US"
            if len(lines) > 2:
                last_line = lines[-2]
                c_addr = ", ".join(lines[1:-2])
            else:
                c_addr = lines[1]
        else:
            # Last line is City State Zip
            if len(lines) > 2:
                c_addr = ", ".join(lines[1:-1])
            else:
                c_addr = lines[1]
        
        # Parse City/State/Zip from that line
        parts = last_line.split(',')
        if len(parts) >= 1: c_city = parts[0].strip()
        if len(parts) >= 2:
            state_zip = parts[1].strip().split(' ')
            state_zip = [x for x in state_zip if x]
            if len(state_zip) >= 1: c_state = state_zip[0]
            if len(state_zip) >= 2: c_zip = state_zip[1]

    # CALCULATE NEXT BUSINESS DAY
    weekday = inv_date.weekday() # Mon=0, Sun=6
    if weekday == 4: # Friday -> Monday
        days_to_add = 3
    elif weekday == 5: # Saturday -> Monday
        days_to_add = 2
    else: # Sun-Thurs -> Next Day
        days_to_add = 1
    
    est_arrival = inv_date + timedelta(days=days_to_add)

    columns = [
        'Entry Type', 'Reference Qualifier', 'Reference Number', 'Mode of Transport', 
        'Bill Type', 'MBOL/TRIP Number', 'HBOL/ Shipment Control Number', 
        'Estimate Date of Arrival', 'Time of Arrival', 'US Port of Arrival', 
        'Equipment Number', 'Shipper Name', 'Shipper Address', 'Shipper City', 
        'Shipper Country', 'Consignee Name', 'Consignee Address', 'Consignee City', 
        'Consignee State or Province', 'Consignee Postal Code', 'Consignee Country', 
        'Description', 'Product ID', 'Carrier Name', 'Vessel Name', 
        'Voyage Trip Flight Number', 'Rail Car Number'
    ]
    
    rows = []
    for _, row in df.iterrows():
        # EXCLUDE ROW IF FDA CODE IS MISSING
        fda = str(row.get('FDA Code', '')).strip()
        if not fda or fda.lower() == 'nan':
            continue

        rows.append({
            'Entry Type': '01',
            'Reference Qualifier': 'BOL',
            'Reference Number': '', 
            'Mode of Transport': '30',
            'Bill Type': 'R',
            'MBOL/TRIP Number': hbol_number,
            'HBOL/ Shipment Control Number': hbol_number,
            'Estimate Date of Arrival': est_arrival.strftime('%Y%m%d'),
            'Time of Arrival': '18:00',
            'US Port of Arrival': '0712',
            'Equipment Number': '',
            'Shipper Name': 'HOLISTIC ROASTERS',
            'Shipper Address': '3780 RUE SAINT-PATRICK',
            'Shipper City': 'MONTREAL',
            'Shipper Country': 'CA',
            'Consignee Name': c_name,
            'Consignee Address': c_addr,
            'Consignee City': c_city,
            'Consignee State or Province': c_state,
            'Consignee Postal Code': c_zip,
            'Consignee Country': c_country,
            'Description': row['Description'],
            'Product ID': row['Variant code / SKU'],
            'Carrier Name': carrier_code,
            'Vessel Name': '',
            'Voyage Trip Flight Number': hbol_number,
            'Rail Car Number': ''
        })
    return pd.DataFrame(rows, columns=columns).to_csv(index=False).encode('utf-8')

# ================= TAB 1: GENERATE DOCUMENTS =================
with tab_generate:
    with st.expander("📝 Invoice Details, Addresses & Signature", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            # REMOVED HYPHEN IN DEFAULT
            default_id = f"{date.today().strftime('%Y%m%d')}1"
            inv_number = st.text_input("Daily Batch # (e.g., YYYYMMDD1)", value=default_id)
            inv_date = st.date_input("Date", value=date.today())
            discount_rate = st.number_input("Target Transfer Discount %", min_value=0.0, max_value=100.0, value=75.0, step=0.1, format="%.1f")
            
        with c2:
            shipper_txt = st.text_area("Shipper / Exporter", value=DEFAULT_SHIPPER, height=120)
            importer_txt = st.text_area("Importer (Bill To)", value=DEFAULT_IMPORTER, height=100)
        with c3:
            consignee_txt = st.text_area("Consignee (Ship To)", value=DEFAULT_CONSIGNEE, height=120)
            notes_txt = st.text_area("Notes / Broker", value=DEFAULT_NOTES, height=100)

        st.markdown("---")
        # CARRIER SELECTION
        carrier_col, sig_col = st.columns(2)
        with carrier_col:
            st.markdown("#### Carrier Settings")
            
            # LOAD DEFAULT CARRIER FROM DB
            carrier_options = ["FX (FedEx)", "GCYD (Green City)", "Other"]
            saved_carrier_bytes = get_setting('default_carrier')
            default_idx = 0
            if saved_carrier_bytes:
                saved_carrier_str = saved_carrier_bytes.decode('utf-8')
                if saved_carrier_str in carrier_options:
                    default_idx = carrier_options.index(saved_carrier_str)
            
            carrier_opt = st.selectbox("Select Carrier", carrier_options, index=default_idx)
            
            # SAVE DEFAULT BUTTON
            if st.button("💾 Save as Default"):
                save_setting('default_carrier', carrier_opt.encode('utf-8'))
                st.success(f"Default set to {carrier_opt}")
            
            if carrier_opt == "Other":
                carrier_code = st.text_input("Enter Custom Carrier Code (for CSV)")
                carrier_pdf_display = st.text_input("Enter Carrier Name (for PDF)")
            elif "FX" in carrier_opt:
                carrier_code = "FX"
                carrier_pdf_display = "FedEx (FX)"
            elif "GCYD" in carrier_opt:
                carrier_code = "GCYD"
                carrier_pdf_display = "Green City Carrier (GCYD)"
        
        with sig_col:
            st.markdown("#### Signature")
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
                sig_upload = st.file_uploader("Upload New (PNG/JPG)", type=['png', 'jpg', 'jpeg'], key="sig_upl")
                if sig_upload:
                    bytes_data = sig_upload.getvalue()
                    save_signature(bytes_data)
                    st.success("Saved!")
                    st.rerun()
        final_sig_bytes = saved_sig_bytes if saved_sig_bytes else None

    st.subheader("Upload Orders")
    
    # --- TOTAL SALES METRIC ---
    total_sales_container = st.empty() # Placeholder for total sales
    
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
                
                possible_order_cols = ['SO #', 'Name', 'Order Name', 'Order Number']
                order_col = next((col for col in possible_order_cols if col in us_shipments.columns), None)
                unique_orders_count = us_shipments[order_col].nunique() if order_col else 1
                
                catalog = get_catalog()
                if not catalog.empty:
                    catalog['sku'] = catalog['sku'].astype(str)
                    merged = pd.merge(sales_data, catalog, left_on='Variant code / SKU', right_on='sku', how='left')
                    merged['Final Product Name'] = merged['product_name'].fillna(merged['Item variant'])
                    merged['Final Desc'] = merged['description'].fillna(merged['product_name']).fillna(merged['Item variant'])
                    merged['Final HTS'] = merged['hts_code'].fillna(DEFAULT_HTS)
                    merged['Final FDA'] = merged['fda_code'].fillna(DEFAULT_FDA)
                    merged['Final Weight'] = merged['weight_lbs'].fillna(0.0) 
                    processed = merged.copy()
                else:
                    processed = sales_data.copy()
                    processed['Final Product Name'] = processed['Item variant']
                    processed['Final Desc'] = processed['Item variant']
                    processed['Final HTS'] = DEFAULT_HTS
                    processed['Final FDA'] = DEFAULT_FDA
                    processed['Final Weight'] = 0.0
                
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
                    'Final Weight': 'first',
                    'Transfer Price (Unit)': 'mean',
                    'Transfer Total': 'sum'
                }).reset_index()
                
                consolidated.rename(columns={
                    'Final Product Name': 'Product Name',
                    'Final Desc': 'Description', 
                    'Final HTS': 'HTS Code', 
                    'Final FDA': 'FDA Code',
                    'Final Weight': 'Weight (lbs)'
                }, inplace=True)
                
                st.info("👇 Review Line Items")
                edited_df = st.data_editor(
                    consolidated,
                    column_config={
                        "Transfer Total": st.column_config.NumberColumn("Total $", format="$%.2f"),
                        "Transfer Price (Unit)": st.column_config.NumberColumn("Transfer Price", format="$%.2f"),
                        "Weight (lbs)": st.column_config.NumberColumn("Unit Weight (lbs)", format="%.2f")
                    },
                    num_rows="dynamic"
                )
                
                total_val = edited_df['Transfer Total'].sum()
                
                # UPDATE METRIC
                total_sales_container.metric(label="💰 TOTAL SALES VALUE (USD)", value=f"${total_val:,.2f}")
                
                # --- LOGISTICS SECTION ---
                st.divider()
                st.subheader("🚚 Shipping Logistics (For Bill of Lading)")
                c_log1, c_log2, c_log3 = st.columns(3)
                
                with c_log1:
                    pallets = st.number_input("Total Pallets", min_value=1, value=1, step=1)
                with c_log2:
                    cartons = st.number_input("Total Cartons/Boxes", min_value=1, value=unique_orders_count, step=1)
                with c_log3:
                    calc_weight = (edited_df['Quantity'] * edited_df['Weight (lbs)']).sum()
                    st.metric("Calculated Net Weight (lbs)", f"{calc_weight:,.2f}")
                    gross_weight = st.number_input("Total Gross Weight (lbs)", min_value=0.0, value=calc_weight + (pallets * 40), step=1.0)

                # --- GENERATE FILES ---
                base_id = inv_number
                ci_id = f"CI-HRUS{base_id}"
                si_id = f"SI-HRUS{base_id}"
                po_id = f"PO-HRUS{base_id}"
                bol_id = f"BOL-HRUS{base_id}"
                
                hbol_clean = f"HRUS{base_id}"
                
                pdf_ci = generate_ci_pdf("COMMERCIAL INVOICE", edited_df, ci_id, inv_date, 
                                      shipper_txt, importer_txt, consignee_txt, notes_txt, total_val, final_sig_bytes, signer_name)
                
                pdf_po = generate_po_pdf(edited_df, po_id, inv_date, 
                                      importer_txt, shipper_txt, consignee_txt, total_val)
                
                pdf_si = generate_si_pdf(edited_df, si_id, inv_date, 
                                      shipper_txt, importer_txt, consignee_txt, notes_txt, total_val, final_sig_bytes, signer_name)
                
                # Pass carrier_pdf_display for visual
                pdf_bol = generate_bol_pdf(edited_df, inv_number, inv_date, shipper_txt, consignee_txt, carrier_pdf_display, bol_id, pallets, cartons, gross_weight, final_sig_bytes)
                
                # Pass carrier_code for CSV
                csv_customs = generate_customscity_csv(edited_df, inv_number, inv_date, consignee_txt, hbol_clean, carrier_code)

                st.session_state['current_pdfs'] = {
                    'ci': pdf_ci, 'po': pdf_po, 'si': pdf_si, 'bol': pdf_bol,
                    'inv_num': inv_number, 'total': total_val, 'buyer': importer_txt.split('\n')[0]
                }

                # --- ACTIONS AREA ---
                st.divider()
                col_left, col_right = st.columns([1, 2])
                
                with col_left:
                    st.subheader("🖨️ Downloads")
                    c_pdf1, c_pdf2, c_pdf3, c_pdf4 = st.columns(4)
                    with c_pdf1: st.download_button("📄 Commercial", pdf_ci, f"{ci_id}.pdf", "application/pdf")
                    with c_pdf2: st.download_button("📄 Purch. Order", pdf_po, f"{po_id}.pdf", "application/pdf")
                    with c_pdf3: st.download_button("📄 Sales Inv.", pdf_si, f"{si_id}.pdf", "application/pdf")
                    with c_pdf4: st.download_button("🚛 Bill of Lading", pdf_bol, f"{bol_id}.pdf", "application/pdf")
                    
                    st.divider()
                    st.download_button(
                        label="📥 Download CustomsCity CSV",
                        data=csv_customs,
                        file_name=f"CustomsCity_{inv_number}.csv",
                        mime="text/csv",
                        type="primary"
                    )
                
                with col_right:
                    st.subheader("📧 Email Center")
                    with st.expander("⚙️ Sender Settings", expanded=False):
                        db_email = get_setting('smtp_email')
                        db_pass = get_setting('smtp_pass')
                        
                        default_email = db_email.decode('utf-8') if db_email else "dean.turner@holisticroasters.com"
                        default_pass = db_pass.decode('utf-8') if db_pass else ""
                        
                        sender_email = st.text_input("Your Email", value=default_email)
                        sender_pass = st.text_input("App Password", value=default_pass, type="password")
                        
                        if st.button("💾 Save Credentials"):
                            save_setting('smtp_email', sender_email.encode('utf-8'))
                            save_setting('smtp_pass', sender_pass.encode('utf-8'))
                            st.success("Saved!")
                    
                    recipient_email = st.text_input("Send To:", value="dean.turner@holisticroasters.com")
                    
                    if st.button("📧 Email All Documents", type="primary"):
                        if not sender_pass:
                            st.error("Please enter your App Password in the settings above.")
                        else:
                            new_ver = save_invoice_metadata(inv_number, total_val, importer_txt.split('\n')[0])
                            
                            files_to_send = [
                                {'name': f"{ci_id}.pdf", 'data': pdf_ci},
                                {'name': f"{po_id}.pdf", 'data': pdf_po},
                                {'name': f"{si_id}.pdf", 'data': pdf_si},
                                {'name': f"{bol_id}.pdf", 'data': pdf_bol},
                                {'name': f"CustomsCity_{new_ver}.csv", 'data': csv_customs}
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
        template_df = pd.DataFrame(columns=['sku', 'product_name', 'description', 'hts_code', 'fda_code', 'weight_lbs'])
        csv_template = template_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Template", csv_template, "catalog_template.csv", "text/csv")
    with col_tools2:
        current_catalog = get_catalog()
        if not current_catalog.empty:
            csv_current = current_catalog.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Catalog", csv_current, "full_catalog.csv", "text/csv")
    with col_tools3:
        if st.checkbox("I want to clear the entire catalog"):
            if st.button("⚠️ Clear Catalog Permanently"):
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
        edited_catalog = st.data_editor(current_catalog, num_rows="dynamic", 
                                        column_config={
                                            "weight_lbs": st.column_config.NumberColumn("Weight (lbs)", format="%.2f")
                                        })
        if st.button("💾 Save Changes"):
            upsert_catalog_from_df(edited_catalog)
            st.success("Saved!")

# ================= TAB 3: HISTORY =================
with tab_history:
    st.header("🗄️ Documents Archive")
    st.caption("Shows log of generated invoices. (Download feature disabled in this view).")
    history_df = get_history()
    st.dataframe(history_df, use_container_width=True, hide_index=True)
