"""
Script to merge Vehicle Renewal Payments with GovPay data using amount as the key.
Left join: Vehicle data (left) merged with GovPay data (right) on amount.
Output: Excel file (.xlsx)
"""

import csv
from openpyxl import Workbook

# Define file paths
VEHICLE_FILE = '/Users/user/Desktop/Korlie_Data/Vechile_renewal/clean_paid_vehicle_renewal_payments.csv'
GOVPAY_FILE = '/Users/user/Desktop/Korlie_Data/GovPay/GOVPAY-SLRSA_Dec19-31_2025.csv'
OUTPUT_FILE = '/Users/user/Desktop/Korlie_Data/Merged_Data/vehicle_govpay_merged_by_amount.xlsx'

def read_csv(filepath):
    """Read CSV file and return header and rows."""
    with open(filepath, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        rows = list(reader)
    return header, rows

def main():
    print("=" * 60)
    print("MERGING VEHICLE RENEWAL DATA WITH GOVPAY DATA (EXCEL OUTPUT)")
    print("=" * 60)
    
    # Read Vehicle data (left table)
    print(f"\nReading Vehicle file: {VEHICLE_FILE}")
    vehicle_header, vehicle_rows = read_csv(VEHICLE_FILE)
    print(f"  - Columns: {len(vehicle_header)}")
    print(f"  - Rows: {len(vehicle_rows)}")
    
    # Read GovPay data (right table)
    print(f"\nReading GovPay file: {GOVPAY_FILE}")
    govpay_header, govpay_rows = read_csv(GOVPAY_FILE)
    print(f"  - Columns: {len(govpay_header)}")
    print(f"  - Rows: {len(govpay_rows)}")
    
    # Find the amount column index in Vehicle data
    try:
        vehicle_amount_idx = vehicle_header.index('amount')
    except ValueError:
        print("Error: 'amount' column not found in Vehicle file.")
        return
    
    # Find the deposit column index in GovPay data (this is the amount equivalent)
    try:
        govpay_amount_idx = govpay_header.index('deposit')
    except ValueError:
        print("Error: 'deposit' column not found in GovPay file.")
        return
    
    print(f"\n  - Vehicle 'amount' column index: {vehicle_amount_idx}")
    print(f"  - GovPay 'deposit' column index: {govpay_amount_idx}")
    
    # Create a lookup dictionary for GovPay data keyed by deposit (amount)
    govpay_by_amount = {}
    for row in govpay_rows:
        if len(row) > govpay_amount_idx:
            amount = row[govpay_amount_idx].strip()
            try:
                amount_float = float(amount)
                amount_key = str(int(amount_float)) if amount_float == int(amount_float) else str(amount_float)
            except ValueError:
                amount_key = amount
            
            if amount_key not in govpay_by_amount:
                govpay_by_amount[amount_key] = []
            govpay_by_amount[amount_key].append(row)
    
    print(f"\n  - Unique amounts in GovPay: {len(govpay_by_amount)}")
    
    # Create merged header (Vehicle columns + GovPay columns with prefix)
    govpay_header_prefixed = [f"govpay_{col}" for col in govpay_header]
    merged_header = vehicle_header + govpay_header_prefixed
    
    # Perform LEFT JOIN
    merged_rows = []
    matched_count = 0
    unmatched_count = 0
    
    for vehicle_row in vehicle_rows:
        if len(vehicle_row) > vehicle_amount_idx:
            amount = vehicle_row[vehicle_amount_idx].strip()
            try:
                amount_float = float(amount)
                amount_key = str(int(amount_float)) if amount_float == int(amount_float) else str(amount_float)
            except ValueError:
                amount_key = amount
            
            if amount_key in govpay_by_amount:
                for govpay_row in govpay_by_amount[amount_key]:
                    merged_row = vehicle_row + govpay_row
                    merged_rows.append(merged_row)
                matched_count += 1
            else:
                empty_govpay = [''] * len(govpay_header)
                merged_row = vehicle_row + empty_govpay
                merged_rows.append(merged_row)
                unmatched_count += 1
    
    print(f"\n--- Merge Results ---")
    print(f"  - Vehicle rows with GovPay match: {matched_count}")
    print(f"  - Vehicle rows without match: {unmatched_count}")
    print(f"  - Total merged rows: {len(merged_rows)}")
    
    # Write merged data to Excel
    print(f"\nWriting merged data to Excel: {OUTPUT_FILE}")
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Merged Data"
    
    # Write header
    ws.append(merged_header)
    
    # Write data rows
    for row in merged_rows:
        ws.append(row)
    
    # Auto-adjust column widths (approximate)
    for col_idx, col_name in enumerate(merged_header, 1):
        ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = max(12, len(col_name) + 2)
    
    wb.save(OUTPUT_FILE)
    wb.close()
    
    print(f"\n{'=' * 60}")
    print(f"SUCCESS! Excel file saved: {OUTPUT_FILE}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
