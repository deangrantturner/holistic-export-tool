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
    # Check if exists to prevent duplicates (optional)
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

# --- HELPER: Extract Weight from Name ---
def estimate_weight_kg(variant_name):
    # Look for "300g" or "907g" or "1kg"
    variant_name = str(variant_name).lower()
    
    # Check grams
    grams = re.search(r'(\d+)\s*g', variant_name)
    if grams:
        return float(grams.group(1)) / 1000.0
    
    # Check kg
    kgs = re.search(r'(\d+(?:\.\d+)?)\s*kg', variant_name)
    if kgs:
        return float(kgs.group(1))
    
    return 0.0 # Default if not found

# --- Page Config ---
st.set_page_config(page_title="Holistic Roasters Export Hub", layout="wide")
st.title("☕ Holistic Roasters Export Hub")

# --- Tabs ---
tab_generate, tab_history = st.tabs(["📝 Generate Invoice", "VX Invoice History"])

# --- DEFAULT DATA (From your Sample CI) ---
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

DEFAULT_HTS = "0901.21.00.20" # Coffee Retail <2kg
DEFAULT_FDA = "31ADT01"

# --- PDF CLASS ---
class ProInvoice(FPDF):
    def header(self):
        # Title
        self.set_font('Helvetica', 'B', 20)
        self.cell(0, 10, 'COMMERCIAL INVOICE', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-25)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 5, "I declare that all information contained in this invoice to be true and correct.", 0, 1, 'C')
        self.cell(0, 5, "These goods are of Canadian origin (CUSMA/USMCA qualified).", 0, 1, 'C')
        self.ln(2)
        self.cell(0, 5, f'Page {self.page_no()}', 0, 0, 'C')

def generate_pro_pdf(df, inv_num, inv_date, shipper, consignee, importer, notes, total_val):
    pdf = ProInvoice()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=20)
    
    # --- INFO BLOCK ---
    pdf.set_font("Helvetica", '', 9)
    y_top = pdf.get_y()
    
    # Column 1: Shipper
    pdf.set_xy(10, y_top)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(60, 5, "SHIPPER / EXPORTER:", 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(60, 4, shipper)
    y_shipper_end = pdf.get_y()
    
    # Column 2: Consignee
    pdf.set_xy(75, y_top)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(60, 5, "CONSIGNEE (SHIP TO):", 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(60, 4, consignee)
    
    # Column 3: Invoice Details (Right Aligned)
    pdf.set_xy(140, y_top)
    pdf.set_font("Helvetica", 'B', 12)
    pdf.cell(50, 6, f"Invoice #: {inv_num}", 0, 1, 'R')
    pdf.set_font("Helvetica", '', 10)
    pdf.cell(50, 6, f"Date: {inv_date}", 0, 1, 'R')
    pdf.cell(50, 6, "Currency: USD", 0, 1, 'R')
    pdf.cell(50, 6, "Origin: CANADA", 0, 1, 'R')
    
    # --- ROW 2: Importer & Notes ---
    y_mid = max(y_shipper_end, pdf.get_y()) + 5
    
    # Importer
    pdf.set_xy(10, y_mid)
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(80, 5, "IMPORTER OF RECORD (SOLD TO):", 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(80, 4, importer)
    
    # Notes Box
    pdf.set_xy(100, y_mid)
    pdf.set_fill_color(245, 245, 245)
    pdf.rect(100, y_mid, 90, 30, 'F')
    pdf.set_xy(102, y_mid + 2)
    pdf.set_font("Helvetica", 'B', 9)
    pdf.cell(50, 5, "NOTES / BROKER / FDA:", 0, 1)
    pdf.set_font("Helvetica", '', 8)
    pdf.set_xy(102, pdf.get_y())
    pdf.multi_cell(85, 4, notes)
    
    pdf.set_y(y_mid + 35) # Move below everything

    # --- TABLE ---
    # Widths: QTY(15), DESC(65), WGT(20), HTS(25), FDA(25), PRICE(20), TOT(20)
    w = [15, 65, 20, 25, 25, 20, 20]
    h = 60
    
    pdf.set_font("Helvetica", 'B', 8)
    pdf.set_fill_color(220, 220, 220)
    headers = ["QTY", "DESCRIPTION", "NET KG", "HTS #", "FDA CODE", "UNIT ($)", "TOTAL ($)"]
    
    for i, head in enumerate(headers):
        pdf.cell(w[i], 8, head, 1, 0, 'C', fill=True)
    pdf.ln()
    
    # Rows
    pdf.set_font("Helvetica", '', 8)
    total_weight = 0
    
    for _, row in df.iterrows():
        # Data Prep
        qty = str(int(row['Quantity']))
        desc = str(row['Item variant'])[:45]
        
        # Net Weight Calculation
        weight_unit = row.get('Net Weight (kg)', 0)
        line_weight = weight_unit * row['Quantity']
        total_weight += line_weight
        wgt_str = f"{line_weight:.2f}"
        
        hts = str(row.get('HTS Code', ''))
        fda = str(row.get('FDA Code', ''))
        price = f"{row['Transfer Price (Unit)']:.2f}"
        tot = f"{row['Transfer Total']:.2f}"
        
        # Draw Cells
        # Save Y to ensure all cells are same height
        y_line = pdf.get_y()
        x_line = pdf.get_x()
        
        pdf.cell(w[0], 6, qty, 1, 0, 'C')
        pdf.cell(w[1], 6, desc, 1, 0, 'L')
        pdf.cell(w[2], 6, wgt_str, 1, 0, 'C')
        pdf.cell(w[3], 6, hts, 1, 0, 'C')
        pdf.cell(w[4], 6, fda, 1, 0, 'C')
        pdf.cell(w[5], 6, price, 1, 0, 'R')
        pdf.cell(w[6], 6, tot, 1, 1, 'R')

    # --- TOTALS ---
    pdf.ln(2)
    pdf.set_font("Helvetica", 'B', 10)
    
    # Weight Total
    pdf.cell(w[0]+w[1], 6, "TOTAL NET WEIGHT:", 0, 0, 'R')
    pdf.cell(w[2], 6, f"{total_weight:.2f} KG", 1, 1, 'C')
    
    # Value Total
    pdf.set_x(10)
    pdf.cell(sum(w[:-1]), 8, "TOTAL INVOICE VALUE (USD):", 0, 0, 'R')
    pdf.cell(w[-1], 8, f"${total_val:,.2f}", 1, 1, 'R')
    
    # Signature
    pdf.ln(15)
    pdf.cell(100, 0, "__________________________", 0, 1)
    pdf.cell(100, 5, "Authorized Signature", 0, 1)

    return bytes(pdf.output())

# ================= TAB 1: GENERATE =================
with tab_generate:
    # --- 1. SETTINGS & ADDRESSES ---
    with st.expander("📝 Invoice Details & Addresses", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            inv_number = st.text_input("Invoice #", value=f"INV-{date.today().strftime('%Y%m%d')}")
            inv_date = st.date_input("Date", value=date.today())
            discount_rate = st.slider("Discount %", 0, 100, 50)
        with c2:
            shipper_txt = st.text_area("Shipper / Exporter", value=DEFAULT_SHIPPER, height=120)
            importer_txt = st.text_area("Importer (Sold To)", value=DEFAULT_IMPORTER, height=100)
        with c3:
            consignee_txt = st.text_area("Consignee (Ship To)", value=DEFAULT_CONSIGNEE, height=120)
            notes_txt = st.text_area("Broker / Notes", value=DEFAULT_NOTES, height=100)
    
    # --- 2. UPLOAD & DATA ---
    st.subheader("Upload Orders")
    uploaded_file = st.file_uploader("Upload CSV", type=['csv'])

    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            
            # Filter US & Products
            us_shipments = df[df['Ship to country'] == 'United States'].copy()
            if 'Item type' in df.columns:
                us_shipments = us_shipments[us_shipments['Item type'] == 'product']
            
            # Logic: Select & Rename
            # We assume columns exist. If "Item variant" is missing, check your CSV
            processed = us_shipments[['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit']].copy()
            
            # Auto-Calculate Weight (The Magic Feature)
            processed['Net Weight (kg)'] = processed['Item variant'].apply(estimate_weight_kg)
            
            # Calculate Prices
            processed['Transfer Price (Unit)'] = processed['Price per unit'] * (1 - discount_rate/100.0)
            processed['Transfer Total'] = processed['Quantity'] * processed['Transfer Price (Unit)']
            
            # Consolidate
            consolidated = processed.groupby(['Variant code / SKU', 'Item variant']).agg({
                'Quantity': 'sum',
                'Net Weight (kg)': 'sum', # Sum weight per line
                'Transfer Price (Unit)': 'mean',
                'Transfer Total': 'sum'
            }).reset_index()
            
            # Fix Weight: If we grouped 5 items of 0.3kg, the sum is 1.5kg. 
            # But we want 'Unit Weight'. So we should take the mean of weight or re-calc.
            # Better approach: Keep Unit Weight separate.
            # Let's re-apply weight estimation to the consolidated rows to be safe.
            consolidated['Net Weight (kg)'] = consolidated['Item variant'].apply(estimate_weight_kg)
            
            # Add Codes
            consolidated['HTS Code'] = DEFAULT_HTS
            consolidated['FDA Code'] = DEFAULT_FDA
            
            # --- 3. EDITABLE TABLE ---
            st.info("👇 Click on any cell below to edit HTS codes, FDA codes, or Weights.")
            edited_df = st.data_editor(
                consolidated,
                column_config={
                    "Net Weight (kg)": st.column_config.NumberColumn("Unit Net Wgt (kg)", format="%.3f"),
                    "Transfer Price (Unit)": st.column_config.NumberColumn("Unit Price ($)", format="$%.2f"),
                    "Transfer Total": st.column_config.NumberColumn("Total ($)", format="$%.2f")
                },
                num_rows="dynamic"
            )
            
            total_val = edited_df['Transfer Total'].sum()
            
            # --- 4. PREVIEW & SAVE ---
            col_act1, col_act2 = st.columns([1, 2])
            
            # Generate PDF
            pdf_bytes = generate_pro_pdf(edited_df, inv_number, inv_date, shipper_txt, consignee_txt, importer_txt, notes_txt, total_val)
            
            with col_act1:
                st.download_button(
                    label="📄 Download PDF",
                    data=pdf_bytes,
                    file_name=f"{inv_number}.pdf",
                    mime="application/pdf"
                )
                if st.button("💾 Save to History"):
                    save_invoice_to_db(inv_number, total_val, pdf_bytes, importer_txt.split('\n')[0])
                    st.success(f"Saved {inv_number}!")
            
            with col_act2:
                # Preview
                b64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')
                pdf_display = f'<iframe src="data:application/pdf;base64,{b64_pdf}" width="100%" height="800" type="application/pdf"></iframe>'
                st.markdown(pdf_display, unsafe_allow_html=True)

        except Exception as e:
            st.error(f"Error processing file: {e}")

# ================= TAB 2: HISTORY =================
with tab_history:
    st.header("🗄️ Invoice Archive")
    history_df = get_history()
    st.dataframe(history_df, use_container_width=True)
    
    inv_list = history_df['invoice_number'].unique()
    if len(inv_list) > 0:
        col_h1, col_h2 = st.columns(2)
        with col_h1:
            selected_inv = st.selectbox("Select Invoice to View:", inv_list)
        with col_h2:
            st.write("") # Spacer
            st.write("")
            if st.button("Retrieve Document"):
                pdf_data = get_pdf_from_db(selected_inv)
                if pdf_data:
                    st.download_button(f"Download {selected_inv}", pdf_data, file_name=f"{selected_inv}.pdf", mime="application/pdf")
                else:
                    st.warning("Document not found.")
