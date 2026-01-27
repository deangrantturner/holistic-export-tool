import streamlit as st
import pandas as pd
import sqlite3
from fpdf import FPDF
from datetime import date
import base64
import re

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
    conn.commit()
    conn.close()

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

init_db()

# --- HELPER: Extract Weight ---
def estimate_weight_kg(variant_name):
    variant_name = str(variant_name).lower()
    grams = re.search(r'(\d+)\s*g', variant_name)
    if grams: return float(grams.group(1)) / 1000.0
    kgs = re.search(r'(\d+(?:\.\d+)?)\s*kg', variant_name)
    if kgs: return float(kgs.group(1))
    return 0.0

# --- Page Config ---
st.set_page_config(page_title="Holistic Roasters Export Hub", layout="wide")
st.title("☕ Holistic Roasters Export Hub")

# --- Tabs ---
tab_generate, tab_history = st.tabs(["📝 Generate Documents", "VX Archive History"])

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
        # We handle title manually in the body to control overlapping
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

    # --- INFO BLOCKS (Fixed Layout to prevent overlap) ---
    pdf.set_font("Helvetica", '', 9)
    y_top = pdf.get_y()
    
    # BLOCK 1: LEFT (From/Shipper)
    pdf.set_xy(10, y_top)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_from = "SHIPPER / EXPORTER:" if "COMMERCIAL" in doc_type else "FROM (SELLER):"
    if "PURCHASE" in doc_type: lbl_from = "FROM (BUYER):" # PO comes from Buyer
    
    pdf.cell(65, 5, lbl_from, 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(65, 4, addr_from)
    y_end_1 = pdf.get_y()

    # BLOCK 2: CENTER (To/Consignee)
    pdf.set_xy(80, y_top)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_to = "CONSIGNEE (SHIP TO):" if "COMMERCIAL" in doc_type else "SHIP TO:"
    
    pdf.cell(65, 5, lbl_to, 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(65, 4, addr_ship)
    y_end_2 = pdf.get_y()

    # BLOCK 3: RIGHT (Invoice Details) - LOCKED COORDINATES
    # We use explicit set_xy for each line to prevent margin snapping
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

    # --- ROW 2: BILL TO / NOTES ---
    y_mid = max(y_end_1, y_end_2, y_end_3) + 8
    
    # Left: Bill To / Importer
    pdf.set_xy(10, y_mid)
    pdf.set_font("Helvetica", 'B', 10)
    lbl_bill = "IMPORTER OF RECORD:" if "COMMERCIAL" in doc_type else "BILL TO:"
    if "PURCHASE" in doc_type: lbl_bill = "TO (VENDOR):" # PO goes to Vendor
    
    pdf.cell(80, 5, lbl_bill, 0, 1)
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
    w = [15, 65, 20, 25, 25, 20, 20]
    headers = ["QTY", "DESCRIPTION", "NET KG", "HTS #", "FDA CODE", "UNIT ($)", "TOTAL ($)"]
    
    pdf.set_font("Helvetica", 'B', 8)
    pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers):
        pdf.cell(w[i], 8, h, 1, 0, 'C', fill=True)
    pdf.ln()
    
    pdf.set_font("Helvetica", '', 8)
    total_weight = 0
    
    for _, row in df.iterrows():
        qty = str(int(row['Quantity']))
        desc = str(row['Item variant'])[:45]
        
        weight_unit = row.get('Net Weight (kg)', 0)
        line_weight = weight_unit * row['Quantity']
        total_weight += line_weight
        wgt_str = f"{line_weight:.2f}"
        
        hts = str(row.get('HTS Code', ''))
        fda = str(row.get('FDA Code', ''))
        price = f"{row['Transfer Price (Unit)']:.2f}"
        tot = f"{row['Transfer Total']:.2f}"
        
        pdf.cell(w[0], 6, qty, 1, 0, 'C')
        pdf.cell(w[1], 6, desc, 1, 0, 'L')
        pdf.cell(w[2], 6, wgt_str, 1, 0, 'C')
        pdf.cell(w[3], 6, hts, 1, 0, 'C')
        pdf.cell(w[4], 6, fda, 1, 0, 'C')
        pdf.cell(w[5], 6, price, 1, 0, 'R')
        pdf.cell(w[6], 6, tot, 1, 1, 'R')

    pdf.ln(2)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(w[0]+w[1], 6, "TOTAL NET WEIGHT:", 0, 0, 'R')
    pdf.cell(w[2], 6, f"{total_weight:.2f} KG", 1, 1, 'C')
    
    pdf.set_x(10)
    pdf.cell(sum(w[:-1]), 8, "TOTAL VALUE (USD):", 0, 0, 'R')
    pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    
    # Signatures
    pdf.ln(15)
    if "COMMERCIAL" in doc_type:
        pdf.cell(100, 5, "Authorized Signature: __________________________", 0, 1)

    return bytes(pdf.output())

# ================= TAB 1: GENERATE =================
with tab_generate:
    # 1. CONFIG
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

    # 2. UPLOAD
    st.subheader("Upload Orders")
    uploaded_file = st.file_uploader("Upload CSV", type=['csv'])

    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            
            # Processing
            us_shipments = df[df['Ship to country'] == 'United States'].copy()
            if 'Item type' in df.columns:
                us_shipments = us_shipments[us_shipments['Item type'] == 'product']
            
            processed = us_shipments[['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit']].copy()
            processed['Net Weight (kg)'] = processed['Item variant'].apply(estimate_weight_kg)
            processed['Transfer Price (Unit)'] = processed['Price per unit'] * (1 - discount_rate/100.0)
            processed['Transfer Total'] = processed['Quantity'] * processed['Transfer Price (Unit)']
            
            consolidated = processed.groupby(['Variant code / SKU', 'Item variant']).agg({
                'Quantity': 'sum',
                'Transfer Price (Unit)': 'mean',
                'Transfer Total': 'sum'
            }).reset_index()
            
            # Re-estimate weight for rows
            consolidated['Net Weight (kg)'] = consolidated['Item variant'].apply(estimate_weight_kg)
            consolidated['HTS Code'] = DEFAULT_HTS
            consolidated['FDA Code'] = DEFAULT_FDA
            
            # Editable Table
            st.info("👇 Edit Line Items (HTS/FDA) here:")
            edited_df = st.data_editor(
                consolidated,
                column_config={
                    "Net Weight (kg)": st.column_config.NumberColumn("Unit Kg", format="%.3f"),
                    "Transfer Total": st.column_config.NumberColumn("Total $", format="$%.2f")
                },
                num_rows="dynamic"
            )
            total_val = edited_df['Transfer Total'].sum()

            # --- GENERATION TABS ---
            st.subheader("🖨️ Document Preview & Download")
            d_tab1, d_tab2, d_tab3 = st.tabs(["Commercial Invoice", "Purchase Order", "Sales Invoice"])

            # 1. COMMERCIAL INVOICE (Shipper -> Importer)
            with d_tab1:
                pdf_ci = generate_pdf("COMMERCIAL INVOICE", edited_df, inv_number, inv_date, 
                                      shipper_txt, importer_txt, consignee_txt, notes_txt, total_val)
                
                col_a, col_b = st.columns([1, 4])
                with col_a:
                    st.download_button("📥 Download PDF", pdf_ci, file_name=f"CI-{inv_number}.pdf", mime="application/pdf")
                    if st.button("💾 Save CI to History", key="save_ci"):
                        save_invoice_to_db(inv_number, total_val, pdf_ci, importer_txt.split('\n')[0])
                        st.success("Saved!")
                
                b64_ci = base64.b64encode(pdf_ci).decode('utf-8')
                st.markdown(f'<iframe src="data:application/pdf;base64,{b64_ci}" width="100%" height="600"></iframe>', unsafe_allow_html=True)

            # 2. PURCHASE ORDER (Buyer -> Seller)
            # FROM: Importer (USA), TO: Shipper (Canada), SHIP TO: Consignee (USA)
            with d_tab2:
                pdf_po = generate_pdf("PURCHASE ORDER", edited_df, inv_number, inv_date, 
                                      importer_txt, shipper_txt, consignee_txt, notes_txt, total_val)
                st.download_button("📥 Download PO", pdf_po, file_name=f"PO-{inv_number}.pdf", mime="application/pdf")
                
                b64_po = base64.b64encode(pdf_po).decode('utf-8')
                st.markdown(f'<iframe src="data:application/pdf;base64,{b64_po}" width="100%" height="600"></iframe>', unsafe_allow_html=True)

            # 3. SALES INVOICE (Seller -> Buyer)
            # FROM: Shipper (Canada), TO: Importer (USA)
            with d_tab3:
                pdf_si = generate_pdf("SALES INVOICE", edited_df, inv_number, inv_date, 
                                      shipper_txt, importer_txt, consignee_txt, notes_txt, total_val)
                st.download_button("📥 Download Invoice", pdf_si, file_name=f"SI-{inv_number}.pdf", mime="application/pdf")
                
                b64_si = base64.b64encode(pdf_si).decode('utf-8')
                st.markdown(f'<iframe src="data:application/pdf;base64,{b64_si}" width="100%" height="600"></iframe>', unsafe_allow_html=True)

        except Exception as e:
            st.error(f"Processing Error: {e}")

# ================= TAB 2: HISTORY =================
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
