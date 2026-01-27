import streamlit as st
import pandas as pd
import io
from datetime import date

# --- Page Setup ---
st.set_page_config(page_title="Holistic Roasters Export Tool", layout="centered")
st.title("☕ Holistic Roasters Export Tool")
st.markdown("Upload the daily Sales Order CSV to generate US Customs & Intercompany documents.")

# --- Sidebar: Inputs ---
st.sidebar.header("Configuration")
uploaded_file = st.sidebar.file_uploader("Upload CSV File", type=['csv'])
discount_rate_percent = st.sidebar.slider("Intercompany Discount (%)", min_value=0, max_value=100, value=50, step=1)

# --- Processing Logic ---
def process_data(df, discount_pct):
    # Filter for United States
    # We use strict matching. Ensure your CSV always uses "United States"
    us_shipments = df[df['Ship to country'] == 'United States'].copy()

    # Filter for Products (exclude shipping fees/services if listed as non-products)
    if 'Item type' in df.columns:
        us_shipments = us_shipments[us_shipments['Item type'] == 'product']

    # Select columns relevant for invoicing
    # Adjust these names if your CSV headers change in the future
    cols_to_keep = ['Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit']
    processed = us_shipments[cols_to_keep].copy()

    # Calculations
    discount_decimal = discount_pct / 100.0
    processed['Retail Total'] = processed['Quantity'] * processed['Price per unit']
    processed['Transfer Price (Unit)'] = processed['Price per unit'] * (1 - discount_decimal)
    processed['Transfer Total'] = processed['Quantity'] * processed['Transfer Price (Unit)']

    # Consolidate by SKU (Combine multiple orders of the same coffee)
    consolidated = processed.groupby(['Variant code / SKU', 'Item variant']).agg({
        'Quantity': 'sum',
        'Transfer Price (Unit)': 'mean', # Average to handle potential minor price variances
        'Transfer Total': 'sum'
    }).reset_index()

    # Rounding for clean currency display
    consolidated['Transfer Price (Unit)'] = consolidated['Transfer Price (Unit)'].round(2)
    consolidated['Transfer Total'] = consolidated['Transfer Total'].round(2)

    return consolidated, us_shipments

# --- App Execution ---
if uploaded_file is not None:
    try:
        df = pd.read_csv(uploaded_file)

        # Check if required columns exist before crashing
        required_cols = ['Ship to country', 'Variant code / SKU', 'Item variant', 'Quantity', 'Price per unit']
        if not all(col in df.columns for col in required_cols):
            st.error(f"Error: The uploaded CSV is missing one of these required columns: {required_cols}")
        else:
            # Run the processor
            invoice_data, raw_log = process_data(df, discount_rate_percent)

            # Show results on screen
            st.success(f"Success! Found {len(raw_log)} line items for the United States.")

            # Metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Qty", int(invoice_data['Quantity'].sum()))
            col2.metric("Transfer Value (USD)", f"${invoice_data['Transfer Total'].sum():,.2f}")
            col3.metric("Discount Used", f"{discount_rate_percent}%")

            st.subheader("Preview: Consolidated Invoice Data")
            st.dataframe(invoice_data.style.format({"Transfer Price (Unit)": "${:.2f}", "Transfer Total": "${:.2f}"}))

            # Generate Excel File in memory
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                workbook = writer.book
                money_fmt = workbook.add_format({'num_format': '$#,##0.00'})

                # Sheet 1: Commercial Invoice
                ci_sheet = invoice_data.copy()
                ci_sheet['Country of Origin'] = 'Canada' 
                ci_sheet.to_excel(writer, sheet_name='Commercial Invoice', index=False)
                writer.sheets['Commercial Invoice'].set_column('C:E', 18, money_fmt)

                # Sheet 2: Purchase Order
                invoice_data.to_excel(writer, sheet_name='Purchase Order', index=False)

                # Sheet 3: Sales Invoice
                invoice_data.to_excel(writer, sheet_name='Sales Invoice', index=False)

                # Sheet 4: Raw Data (Audit Trail)
                raw_log.to_excel(writer, sheet_name='Audit Trail', index=False)

            # Download Button
            st.download_button(
                label="📥 Download Completed Excel File",
                data=output.getvalue(),
                file_name=f"Holistic_US_Transfer_{date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    except Exception as e:
        st.error(f"An error occurred processing the file: {e}")
else:
    st.info("👈 Please upload your CSV file in the sidebar to begin.")
