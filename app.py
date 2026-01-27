import streamlit as st
import pandas as pd
import sqlite3
from fpdf import FPDF
from datetime import date, datetime
import io
import base64

# --- Database Setup (SQLite) ---
# This creates a local file 'invoices.db' to store history
def init_db():
    conn = sqlite3.connect('invoices.db')
    c = conn.cursor()
    # Create table if not exists
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

# Initialize DB on load
init_db()

# --- Page Config ---
st.set_page_config(page_title="Holistic Roasters Export Hub", layout="wide")
st.title("☕ Holistic Roasters Export Hub")

# --- Tabs for Navigation ---
tab_generate, tab_history = st.tabs(["📝 Generate New Invoice", "VX Invoice History"])

# --- DEFAULT DATA ---
DEFAULT_SELLER = "Holistic Roasters Canada\n123 Roastery Lane\nMontreal, QC, Canada"
DEFAULT_BUYER = "Holistic Roasters USA\n456 Warehouse Blvd\nNew York, NY, USA"
DEFAULT_SHIP_TO = "Holistic Roasters USA\n456 Warehouse Blvd\nNew York, NY, USA"
DEFAULT_HTS = "0901.21.0000" # Roasted Coffee Not Decaf
DEFAULT_FDA = "123-456-789"

# --- PDF GENERATOR (Professional Layout) ---
class ProInvoice(FPDF):
    def header(self):
        # Logo placeholder or Company Name
        self.set_font('Helvetica', 'B', 20)
        self.cell(0, 10, 'COMMERCIAL INVOICE', 0, 1, 'R')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

def generate_pro_pdf(df, inv_num, inv_date, seller, buyer, ship_to, total_val, currency="USD"):
    pdf = ProInvoice()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # --- TOP SECTION: DETAILS ---
    pdf.set_font("Helvetica", '', 10)
    
    # Left Block: Addresses
    y_start = pdf.get_y()
    
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(60, 5, "SELLER (EXPORTER):", 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(80, 5, seller)
    pdf.ln(3)
    
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(60, 5, "SOLD TO (IMPORTER):", 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(80, 5, buyer)
    pdf.ln(3)
    
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(60, 5, "SHIP TO:", 0, 1)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(80, 5, ship_to)
    
    # Right Block: Invoice Info
    x_right = 120
    pdf.set_xy(x_right, y_start)
    
    pdf.set_font("Helvetica", 'B', 12)
    pdf.cell(35, 8, "Invoice #:", 0, 0)
    pdf.set_font("Helvetica", '', 12)
    pdf.cell(40, 8, inv_num, 0, 1)
    
    pdf.set_xy(x_right, pdf.get_y())
    pdf.set_font("Helvetica", 'B', 12)
    pdf.cell(35, 8, "Date:", 0, 0)
    pdf.set_font("Helvetica", '', 12)
    pdf.cell(40, 8, str(inv_date), 0, 1)

    pdf.set_xy(x_right, pdf.get_y())
    pdf.set_font("Helvetica", 'B', 12)
    pdf.cell(35, 8, "Currency:", 0, 0)
    pdf.set_font("Helvetica", '', 12)
    pdf.cell(40, 8, currency, 0, 1)
    
    pdf.ln(20) # Space before table
    pdf.set_y(100) # Force table start position

    # --- TABLE HEADER ---
    pdf.set_font("Helvetica", 'B', 9)
    pdf.set_fill_color(230, 230, 230)
    
    # Define Column Widths
    w_sku = 25
    w_desc = 70
    w_hts = 25
    w_qty = 15
    w_price = 25
    w_total = 30
    
    pdf.cell(w_sku, 8, "SKU", 1, 0, 'C', fill=True)
    pdf.cell(w_desc, 8, "Description", 1, 0, 'C', fill=True)
    pdf.cell(w_hts, 8, "HTS / FDA", 1, 0, 'C', fill=True)
    pdf.cell(w_qty, 8, "Qty", 1, 0, 'C', fill=True)
    pdf.cell(w_price, 8, "Unit Price", 1, 0, 'C', fill=True)
    pdf.cell(w_total, 8, "Total Value", 1, 1, 'C', fill=True)

    # --- TABLE ROWS ---
    pdf.set_font("Helvetica", '', 8)
    for _, row in df.iterrows():
        sku = str(row['Variant code / SKU'])
        desc = str(row['Item variant'])[:45] # Truncate
        hts = f"{row.get('HTS Code', '')}\n{row.get('FDA Code', '')}"
        qty = str(int(row['Quantity']))
        price = f"${row['Transfer Price (Unit)']:.2f}"
        tot = f"${row['Transfer Total']:.2f}"
        
        # Calculate height needed
        h = 10 # slightly taller for HTS stack
        
        x_curr = pdf.get_x()
        y_curr = pdf.get_y()
        
        # Draw cells
        pdf.rect(x_curr, y_curr, w_sku, h)
        pdf.cell(w_sku, h, sku, 0, 0, 'L')
        
        pdf.rect(x_curr + w_sku, y_curr, w_desc, h)
        pdf.set_xy(x_curr + w_sku, y_curr)
        pdf.cell(w_desc, h, desc, 0, 0, 'L')
        
        pdf.rect(x_curr + w_sku + w_desc, y_curr, w_hts, h)
        pdf.set_xy(x_curr + w_sku + w_desc, y_curr + 2) # small padding top
        pdf.multi_cell(w_hts, 3, hts, 0, 'C')
        pdf.set_xy(x_curr + w_sku + w_desc + w_hts, y_curr) # Reset to right of HTS

        pdf.rect(x_curr + w_sku + w_desc + w_hts, y_curr, w_qty, h)
        pdf.cell(w_qty, h, qty, 0, 0, 'C')
        
        pdf.rect(x_curr + w_sku + w_desc + w_hts + w_qty, y_curr, w_price, h)
        pdf.cell(w_price, h, price, 0, 0, 'R')
        
        pdf.rect(x_curr + w_sku + w_desc + w_hts + w_qty + w_price, y_curr, w_total, h)
        pdf.cell(w_total, h, tot, 0, 1, 'R')

    # --- TOTALS ---
    pdf.ln(5)
    pdf.set_font("Helvetica", 'B', 12)
    pdf.cell(160, 10, "TOTAL INVOICE VALUE (USD):", 0, 0, 'R')
    pdf.cell(30, 10, f"${total_val:,.2f}", 0, 1, 'R')

    # --- DECLARATION ---
    pdf.ln(10)
    pdf.set_font("Helvetica", '', 9)
    pdf.multi_cell(0, 5, "I declare that all information contained in this invoice to be true and correct. These goods are of Canadian origin (CUSMA/USMCA qualified).")
    
    pdf.ln(15)
    pdf.cell(80, 0, "__________________________", 0, 1)
    pdf.cell(80, 5, "Authorized Signature", 0, 1)

    return bytes(pdf.output())

# ================= TAB 1: GENERATE =================
with tab_generate:
    col_left, col_mid, col_right = st.columns([1, 1, 1])
    
    with col_left:
        st.subheader("1. Configuration")
        # Editable Invoice #
        default_inv = f"INV-{date.today().strftime('%Y%m%d')}"
        inv_number = st.text_input("Invoice Number", value=default_inv)
        inv_date = st.date_input("Invoice Date", value=date.today())
        
        discount_rate = st.slider("Discount %", 0, 100, 50)
    
    with col_mid:
        st.subheader("2. Addresses")
        seller_info = st.text_area("Seller (Exporter)", value=DEFAULT_SELLER, height=100)
        buyer_info = st.text_area("Buyer (Importer)", value=DEFAULT_BUYER, height=100)
        ship_to_info = st.text_area("Ship To", value=DEFAULT_SHIP_TO, height=100)

    with col_right:
        st.subheader("3. Codes & Upload")
        default_hts_code = st.text_input("Default HTS Code", value=DEFAULT_HTS)
        default_fda_code = st.text_input("Default FDA Code", value=DEFAULT_FDA)
        uploaded_file = st.file_uploader("Upload CSV", type=['csv'])

    st.markdown("---")

    # --- Processing Logic ---
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            
            # Filter & Calc
            us_shipments = df[df['Ship to country'] == 'United States'].copy()
            if 'Item type' in df.columns:
                us_shipments = us_shipments[us_shipments['Item type'] == 'product']
            
            processed = us_shipments[['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit']].copy()
            processed['Transfer Price (Unit)'] = processed['Price per unit'] * (1 - discount_rate/100.0)
            processed['Transfer Total'] = processed['Quantity'] * processed['Transfer Price (Unit)']
            
            # Consolidate
            consolidated = processed.groupby(['Variant code / SKU', 'Item variant']).agg({
                'Quantity': 'sum',
                'Transfer Price (Unit)': 'mean',
                'Transfer Total': 'sum'
            }).reset_index()
            
            # Add HTS/FDA Columns (Editable in Data Editor)
            consolidated['HTS Code'] = default_hts_code
            consolidated['FDA Code'] = default_fda_code
            
            st.subheader("4. Review & Edit Line Items")
            st.info("You can edit HTS or FDA codes for specific items in the table below.")
            
            # EDITABLE DATAFRAME
            edited_df = st.data_editor(consolidated, num_rows="dynamic")
            
            total_val = edited_df['Transfer Total'].sum()
            st.metric("Total Invoice Value", f"${total_val:,.2f}")
            
            # --- ACTION BUTTONS ---
            col_act1, col_act2 = st.columns(2)
            
            # Generate PDF in memory
            pdf_bytes = generate_pro_pdf(edited_df, inv_number, inv_date, seller_info, buyer_info, ship_to_info, total_val)
            
            with col_act1:
                st.download_button(
                    label="📄 Download PDF Only",
                    data=pdf_bytes,
                    file_name=f"{inv_number}.pdf",
                    mime="application/pdf"
                )
            
            with col_act2:
                if st.button("💾 Save to Database & Finish"):
                    save_invoice_to_db(inv_number, total_val, pdf_bytes, buyer_info.split('\n')[0])
                    st.success(f"Invoice {inv_number} saved to history successfully!")
                    st.balloons()

            # Preview
            b64_pdf = base64.b64encode(pdf_bytes).decode('utf-8')
            pdf_display = f'<iframe src="data:application/pdf;base64,{b64_pdf}" width="100%" height="600" type="application/pdf"></iframe>'
            st.markdown("### Preview")
            st.markdown(pdf_display, unsafe_allow_html=True)

        except Exception as e:
            st.error(f"Error: {e}")

# ================= TAB 2: HISTORY =================
with tab_history:
    st.header("Invoice Archive")
    
    # Load Data
    history_df = get_history()
    
    # Display Table
    st.dataframe(history_df)
    
    st.subheader("Retrieve Document")
    search_inv = st.selectbox("Select Invoice Number to Download", history_df['invoice_number'].unique())
    
    if st.button("Fetch PDF"):
        pdf_data = get_pdf_from_db(search_inv)
        if pdf_data:
            st.download_button(
                label=f"Download {search_inv}",
                data=pdf_data,
                file_name=f"{search_inv}.pdf",
                mime="application/pdf"
            )
        else:
            st.error("File not found in database.")
