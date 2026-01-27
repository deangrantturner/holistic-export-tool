import streamlit as st
import pandas as pd
from fpdf import FPDF
from datetime import date

# --- Configuration ---
st.set_page_config(page_title="Holistic Roasters Export Tool", layout="centered")
st.title("☕ Holistic Roasters Export Tool")
st.markdown("Upload the daily Sales Order CSV to generate US Customs & Intercompany documents.")

# --- Sidebar ---
st.sidebar.header("Configuration")
uploaded_file = st.sidebar.file_uploader("Upload CSV File", type=['csv'])
discount_rate_percent = st.sidebar.slider("Intercompany Discount (%)", min_value=0, max_value=100, value=50, step=1)

# Document Addresses
SENDER_CA = "Holistic Roasters Canada<br>123 Roastery Lane<br>Montreal, QC, Canada"
ENTITY_US = "Holistic Roasters USA<br>456 Warehouse Blvd<br>New York, NY, USA"
SENDER_CA_PDF = SENDER_CA.replace("<br>", "\n")
ENTITY_US_PDF = ENTITY_US.replace("<br>", "\n")

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

    # Addresses
    col_width = 90
    pdf.set_font("Helvetica", 'B', 10)
    pdf.cell(col_width, 5, "FROM:", 0, 0)
    pdf.cell(col_width, 5, "TO:", 0, 1)
    
    pdf.set_font("Helvetica", '', 10)
    y_start = pdf.get_y()
    
    pdf.multi_cell(col_width, 5, sender_text)
    y_end_left = pdf.get_y()
    
    pdf.set_xy(10 + col_width, y_start)
    pdf.multi_cell(col_width, 5, receiver_text)
    y_end_right = pdf.get_y()
    
    pdf.set_y(max(y_end_left, y_end_right) + 10)

    # Table
    pdf.set_font("Helvetica", 'B', 10)
    pdf.set_fill_color(220, 220, 220)
    pdf.cell(30, 8, "SKU", 1, 0, 'C', fill=True)
    pdf.cell(90, 8, "Description", 1, 0, 'C', fill=True)
    pdf.cell(20, 8, "Qty", 1, 0, 'C', fill=True)
    pdf.cell(25, 8, "Price", 1, 0, 'C', fill=True)
    pdf.cell(25, 8, "Total", 1, 1, 'C', fill=True)

    pdf.set_font("Helvetica", '', 10)
    for _, row in dataframe.iterrows():
        desc = str(row['Item variant'])
        if len(desc) > 50: desc = desc[:47] + "..."
        pdf.cell(30, 8, str(row['Variant code / SKU']), 1)
        pdf.cell(90, 8, desc, 1)
        pdf.cell(20, 8, str(int(row['Quantity'])), 1, 0, 'C')
        pdf.cell(25, 8, f"${row['Transfer Price (Unit)']:.2f}", 1, 0, 'R')
        pdf.cell(25, 8, f"${row['Transfer Total']:.2f}", 1, 1, 'R')

    pdf.ln(5)
    pdf.set_font("Helvetica", 'B', 12)
    pdf.cell(165, 10, "Total (USD):", 0, 0, 'R')
    pdf.cell(25, 10, f"${total_value:.2f}", 0, 1, 'R')
    
    return bytes(pdf.output())

# --- HTML Preview Generator (Browser Safe) ---
def create_html_preview(dataframe, title, sender, receiver, total_val):
    # Convert dataframe rows to HTML table rows
    rows_html = ""
    for _, row in dataframe.iterrows():
        rows_html += f"""
        <tr style="border-bottom: 1px solid #ddd;">
            <td style="padding: 8px;">{row['Variant code / SKU']}</td>
            <td style="padding: 8px;">{row['Item variant']}</td>
            <td style="padding: 8px; text-align: center;">{int(row['Quantity'])}</td>
            <td style="padding: 8px; text-align: right;">${row['Transfer Price (Unit)']:.2f}</td>
            <td style="padding: 8px; text-align: right;">${row['Transfer Total']:.2f}</td>
        </tr>
        """
    
    # Full HTML Template
    html = f"""
    <div style="font-family: Helvetica, Arial, sans-serif; border: 1px solid #ddd; padding: 20px; border-radius: 5px; background-color: white; color: black;">
        <div style="display: flex; justify-content: space-between; margin-bottom: 20px;">
            <h2 style="margin: 0;">{title}</h2>
            <div style="text-align: right;">
                <strong>Date:</strong> {date.today()}<br>
            </div>
        </div>
        
        <div style="display: flex; margin-bottom: 30px;">
            <div style="flex: 1;">
                <strong>FROM:</strong><br>
                {sender}
            </div>
            <div style="flex: 1;">
                <strong>TO:</strong><br>
                {receiver}
            </div>
        </div>

        <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px;">
            <thead>
                <tr style="background-color: #f2f2f2;">
                    <th style="padding: 10px; text-align: left;">SKU</th>
                    <th style="padding: 10px; text-align: left;">Description</th>
                    <th style="padding: 10px; text-align: center;">Qty</th>
                    <th style="padding: 10px; text-align: right;">Price</th>
                    <th style="padding: 10px; text-align: right;">Total</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>

        <div style="text-align: right; margin-top: 20px;">
            <h3>Total: ${total_val:,.2f}</h3>
        </div>
    </div>
    """
    return html

# --- Logic ---
def process_data(df, discount_pct):
    us_shipments = df[df['Ship to country'] == 'United States'].copy()
    if 'Item type' in df.columns:
        us_shipments = us_shipments[us_shipments['Item type'] == 'product']

    cols = ['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit']
    processed = us_shipments[cols].copy()
    
    processed['Transfer Price (Unit)'] = processed['Price per unit'] * (1 - discount_pct/100.0)
    processed['Transfer Total'] = processed['Quantity'] * processed['Transfer Price (Unit)']
    
    consolidated = processed.groupby(['Variant code / SKU', 'Item variant']).agg({
        'Quantity': 'sum',
        'Transfer Price (Unit)': 'mean',
        'Transfer Total': 'sum'
    }).reset_index()
    
    return consolidated

# --- Main App ---
if uploaded_file:
    try:
        df = pd.read_csv(uploaded_file)
        
        if 'Ship to country' not in df.columns:
            st.error("Error: CSV missing 'Ship to country' column.")
        else:
            invoice_data = process_data(df, discount_rate_percent)
            total_val = invoice_data['Transfer Total'].sum()
            
            st.success("✅ Data Processed Successfully!")
            
            # Create PDFs for Downloading (Uses SENDER_CA_PDF with \n)
            pdf_po = create_pdf(invoice_data, "PURCHASE ORDER", ENTITY_US_PDF, SENDER_CA_PDF, total_val)
            pdf_ci = create_pdf(invoice_data, "COMMERCIAL INVOICE", SENDER_CA_PDF, ENTITY_US_PDF, total_val)
            pdf_si = create_pdf(invoice_data, "SALES INVOICE", SENDER_CA_PDF, ENTITY_US_PDF, total_val)

            st.subheader("Document Preview & Download")
            tab1, tab2, tab3 = st.tabs(["Purchase Order", "Commercial Invoice", "Sales Invoice"])

            # Render HTML Previews (Uses SENDER_CA with <br>)
            with tab1:
                st.download_button("📥 Download PDF", pdf_po, file_name=f"Purchase_Order_{date.today()}.pdf", mime="application/pdf")
                st.markdown(create_html_preview(invoice_data, "PURCHASE ORDER", ENTITY_US, SENDER_CA, total_val), unsafe_allow_html=True)
            
            with tab2:
                st.download_button("📥 Download PDF", pdf_ci, file_name=f"Commercial_Invoice_{date.today()}.pdf", mime="application/pdf")
                st.markdown(create_html_preview(invoice_data, "COMMERCIAL INVOICE", SENDER_CA, ENTITY_US, total_val), unsafe_allow_html=True)

            with tab3:
                st.download_button("📥 Download PDF", pdf_si, file_name=f"Sales_Invoice_{date.today()}.pdf", mime="application/pdf")
                st.markdown(create_html_preview(invoice_data, "SALES INVOICE", SENDER_CA, ENTITY_US, total_val), unsafe_allow_html=True)

    except Exception as e:
        st.error(f"An error occurred: {e}")
