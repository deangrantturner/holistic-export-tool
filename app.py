import streamlit as st
from datetime import datetime
from fpdf import FPDF
import urllib.parse

def draw_intercompany_invoice():
    st.header("🏢 Intercompany Invoicing")
    st.write("Generate a monthly invoice for Holistic Roasters USA.")

    # 1. Automatic Unique Invoice Number Generator
    current_time = datetime.now()
    auto_inv_num = f"HR-{current_time.strftime('%Y%m%d-%H%M')}"
    
    invoice_number = st.text_input("Invoice Number", value=auto_inv_num)
    invoice_date = st.date_input("Invoice Date", value=current_time.date())

    st.subheader("Adjust Invoice Amounts (USD)")
    col1, col2, col3 = st.columns(3)
    marketing_amt = col1.number_input("Marketing Services", value=7300.00, step=100.0)
    brand_amt = col2.number_input("Brand License Fee", value=5200.00, step=100.0)
    admin_amt = col3.number_input("Management & Admin", value=2300.00, step=100.0)

    # Calculate Total
    total_amount = marketing_amt + brand_amt + admin_amt
    st.info(f"**Total Invoice Amount: ${total_amount:,.2f} USD**")

    if st.button("📄 Generate Invoice PDF"):
        # 2. Build the PDF
        pdf = FPDF()
        pdf.add_page()
        
        # Header
        pdf.set_font('Arial', 'B', 20)
        pdf.cell(0, 10, 'INVOICE', 0, 1, 'R')
        pdf.set_font('Arial', '', 10)
        pdf.cell(0, 6, f'Invoice #: {invoice_number}', 0, 1, 'R')
        pdf.cell(0, 6, f'Date: {invoice_date.strftime("%d/%m/%Y")}', 0, 1, 'R')
        pdf.cell(0, 6, 'Terms: Due on receipt', 0, 1, 'R')
        pdf.ln(10)

        # Addresses Headers
        pdf.set_font('Arial', 'B', 10)
        pdf.cell(95, 6, 'FROM:', 0, 0, 'L')
        pdf.cell(95, 6, 'BILL TO:', 0, 1, 'L')
        
        # Addresses
        pdf.set_font('Arial', '', 10)
        start_y = pdf.get_y()  # Remember the starting height
        
        pdf.set_xy(10, start_y)
        pdf.multi_cell(85, 5, "Holistic Roasters Inc.\n3780 Rue Saint-Patrick\nMontreal, QC Canada H4E 1A2")
        
        pdf.set_xy(105, start_y)  # Use the exact same starting height
        pdf.multi_cell(85, 5, "Holistic Roasters USA\n30 N Gould St, STE R\nSheridan, WY 82801\nUnited States")
        
        # --- THE FIX ---
        # Force the cursor to move down past the addresses and back to the left margin
        pdf.set_y(start_y + 25) 
        pdf.set_x(10)

        # Table Header
        pdf.set_font('Arial', 'B', 9)
        pdf.set_fill_color(230, 230, 230)
        pdf.cell(40, 8, 'ACTIVITY', 1, 0, 'L', True)
        pdf.cell(90, 8, 'DESCRIPTION', 1, 0, 'L', True)
        pdf.cell(30, 8, 'TAX', 1, 0, 'C', True)
        pdf.cell(30, 8, 'AMOUNT (USD)', 1, 1, 'R', True)

        # Table Rows
        pdf.set_font('Arial', '', 8)
        
        def add_row(activity, desc, amt):
            start_y_row = pdf.get_y()
            pdf.set_xy(50, start_y_row)
            pdf.multi_cell(90, 5, desc, 1)
            end_y = pdf.get_y()
            row_height = end_y - start_y_row
            
            pdf.set_xy(10, start_y_row)
            pdf.cell(40, row_height, activity, 1, 0, 'L')
            pdf.set_xy(140, start_y_row)
            pdf.cell(30, row_height, 'Zero-rated', 1, 0, 'C')
            pdf.cell(30, row_height, f"{amt:,.2f}", 1, 1, 'R')
            
            # Force the cursor to the bottom of the current row so they don't overlap
            pdf.set_y(end_y)
            pdf.set_x(10)

        add_row('Marketing Services', "Proportionate share of content creation, social media management, email marketing campaigns, website maintenance, and customer acquisition activities for U.S. market", marketing_amt)
        add_row('Brand License Fee', "License to use Holistic Roasters trademarks, packaging designs, and brand assets in the U.S. market per Brand License Agreement", brand_amt)
        add_row('Management & Admin', "Executive oversight, financial reporting, accounting support, vendor coordination, and intercompany administration", admin_amt)

        # Totals
        pdf.ln(5)
        pdf.set_font('Arial', 'B', 10)
        pdf.cell(160, 8, 'SUBTOTAL', 0, 0, 'R')
        pdf.cell(30, 8, f"{total_amount:,.2f}", 1, 1, 'R')
        pdf.cell(160, 8, 'GST @ 0%', 0, 0, 'R')
        pdf.cell(30, 8, '0.00', 1, 1, 'R')
        pdf.cell(160, 8, 'BALANCE DUE (USD)', 0, 0, 'R')
        pdf.cell(30, 8, f"${total_amount:,.2f}", 1, 1, 'R')

        # 3. Output to Streamlit & Provide Gmail Link
        pdf_bytes = bytes(pdf.output())
        
        st.success("PDF Generated Successfully!")
        
        col_dl, col_email = st.columns(2)
        
        with col_dl:
            st.download_button(
                label="⬇️ Download Invoice PDF",
                data=pdf_bytes,
                file_name=f"{invoice_number}.pdf",
                mime="application/pdf"
            )
            
        with col_email:
            # Create a special link that opens Gmail in your browser
            email_to = "gkalinin@biodynamic.coffee"
            email_subject = f"Invoice {invoice_number} - Holistic Roasters Inc."
            email_body = f"Hello,\n\nPlease find attached our latest invoice ({invoice_number}) for ${total_amount:,.2f} USD.\n\nThank you!"
            
            gmail_link = f"https://mail.google.com/mail/?view=cm&fs=1&to={email_to}&su={urllib.parse.quote(email_subject)}&body={urllib.parse.quote(email_body)}"
            
            st.markdown(f'<a href="{gmail_link}" target="_blank"><button style="background-color:#ea4335; color:white; border:none; padding:8px 16px; border-radius:4px; cursor:pointer;">✉️ Compose in Gmail</button></a>', unsafe_allow_html=True)
            st.caption("*Download the PDF first, then click here to compose your email and attach it.*")

# Add this line at the bottom of your app to display the module:
draw_intercompany_invoice()
