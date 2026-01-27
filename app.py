import streamlit as st
import pandas as pd
from fpdf import FPDF
from datetime import date

# --- Configuration ---
st.set_page_config(page_title="Holistic Roasters Export Tool", layout="centered")
st.title("☕ Holistic Roasters Export Tool")
st.markdown("Upload the daily Sales Order CSV to generate US Customs & Intercompany PDFs.")

# --- Sidebar ---
st.sidebar.header("Configuration")
uploaded_file = st.sidebar.file_uploader("Upload CSV File", type=['csv'])
discount_rate_percent = st.sidebar.slider("Intercompany Discount (%)", min_value=0, max_value=100, value=50, step=1)

# Document Addresses
SENDER_CA = "Holistic Roasters Canada\n123 Roastery Lane\nMontreal, QC, Canada"
ENTITY_US = "Holistic Roasters USA\n456 Warehouse Blvd\nNew York, NY, USA"

# --- PDF Generation Class ---
class PDF(FPDF):
    def header(self):
        self.set_font('Helvetica', 'B', 14)
        self.cell(0, 10, 'Holistic Roasters', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')

def create_pdf(dataframe, doc_title, sender_text, receiver_text, total_value):
    pdf = PDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    # Title & Date
    pdf.set_font("Helvetica", 'B', 16)
    pdf.cell(0, 10, doc_title, 0, 1, 'R')
    pdf.set_font("Helvetica", '', 10)
    pdf.cell(0, 5, f"Date: {date.today()}", 0, 1, 'R')
    pdf.ln(10)

    # Addresses (Side by Side)
    col_width = 90
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(col_width, 5, "FROM:", 0, 0)
    pdf.cell(col_width, 5, "TO:", 0, 1)
    
    pdf.set_font("Helvetica", '', 10)
    y_start = pdf.get_y()
    
    pdf.multi_cell(col_width, 5, sender_text) # Left
    y_end_left = pdf.get_y()
    
    pdf.set_xy(10 + col_width, y_start) # Move to Right
    pdf.multi_cell(col_width, 5, receiver_text)
    y_end_right = pdf.get_y()
    
    pdf.set_y(max(y_end_left, y_end_right) + 10)

    # Table Header
    pdf.set_font("Helvetica", 'B', 10)
    pdf.set_fill_color(220, 220, 220)
    pdf.cell(30, 8, "SKU", 1, 0, 'C', fill=True)
    pdf.cell(90, 8, "Description", 1, 0, 'C', fill=True)
    pdf.cell(20, 8, "Qty", 1, 0, 'C', fill=True)
    pdf.cell(25, 8, "Price", 1, 0, 'C', fill=True)
    pdf.cell(25, 8, "Total", 1, 1, 'C', fill=True)

    # Table Rows
    pdf.set_font("Helvetica", '', 10)
    for _, row in dataframe.iterrows():
        desc = str(row['Item variant'])
        if len(desc) > 50: desc = desc[:47] + "..." # Truncate long names
            
        pdf.cell(30, 8, str(row['Variant code / SKU']), 1)
        pdf.cell(90, 8, desc, 1)
        pdf.cell(20, 8, str(int(row['Quantity'])), 1, 0, 'C')
        pdf.cell(25, 8, f"${row['Transfer Price (Unit)']:.2f}", 1, 0, 'R')
        pdf.cell(25, 8, f"${row['Transfer Total']:.2f}", 1, 1, 'R')

    # Totals
    pdf.ln(5)
    pdf.set_font("Helvetica", 'B', 12)
    pdf.cell(165, 10, "Total (USD):", 0, 0, 'R')
    pdf.cell(25, 10, f"${total_value:.2f}", 0, 1, 'R')
    
    # --- THE FIX IS HERE: Convert to bytes ---
    return bytes(pdf.output())

# --- Logic ---
def process_data(df, discount_pct):
    # Filter US & Products
    us_shipments = df[df['Ship to country'] == 'United States'].copy()
    if 'Item type' in df.columns:
        us_shipments = us_shipments[us_shipments['Item type'] == 'product']

    # Select & Calculate
    cols = ['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit']
    processed = us_shipments[cols].copy()
    
    processed['Transfer Price (Unit)'] = processed['Price per unit'] * (1 - discount_pct/100.0)
    processed['Transfer Total'] = processed['Quantity'] * processed['Transfer Price (Unit)']
    
    # Consolidate
    consolidated = processed.groupby(['Variant code / SKU', 'Item variant']).agg({
        'Quantity': 'sum',
        'Transfer Price (Unit)': 'mean',
        'Transfer Total': 'sum'
    }).reset_index()
    
    return consolidated

# --- Main App Interface ---
if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
        
        if 'Ship to country' not in df.columns:
            st.error("Error: CSV missing 'Ship to country' column.")
        else:
            invoice_data = process_data(df, discount_rate_percent)
            total_val = invoice_data['Transfer Total'].sum()
            
            st.success("✅ Data Processed Successfully!")
            st.metric("Total Transfer Value", f"${total_val:,.2f}")
            
            # Show Preview
            with st.expander("See Invoice Data Preview"):
                st.dataframe(invoice_data.style.format({"Transfer Price (Unit)": "${:.2f}", "Transfer Total": "${:.2f}"}))

            # Generate PDFs in memory
            pdf_po = create_pdf(invoice_data, "PURCHASE ORDER", ENTITY_US, SENDER_CA, total_val)
            pdf_ci = create_pdf(invoice_data, "COMMERCIAL INVOICE", SENDER_CA, ENTITY_US, total_val)
            pdf_si = create_pdf(invoice_data, "SALES INVOICE", SENDER_CA, ENTITY_US, total_val)

            # --- Display Download Buttons (3 Separate Buttons) ---
            st.subheader("Download Documents")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.download_button(
                    label="📄 Purchase Order",
                    data=pdf_po,
                    file_name=f"Purchase_Order_{date.today()}.pdf",
                    mime="application/pdf"
                )
            
            with col2:
                st.download_button(
                    label="📄 Commercial Invoice",
                    data=pdf_ci,
                    file_name=f"Commercial_Invoice_{date.today()}.pdf",
                    mime="application/pdf"
                )

            with col3:
                st.download_button(
                    label="📄 Sales Invoice",
                    data=pdf_si,
                    file_name=f"Sales_Invoice_{date.today()}.pdf",
                    mime="application/pdf"
                )

    except Exception as e:
        st.error(f"An error occurred: {e}")
