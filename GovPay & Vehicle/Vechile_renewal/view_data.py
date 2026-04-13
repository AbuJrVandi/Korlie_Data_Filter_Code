"""
Script to get total data points from clean_paid_vehicle_renewal_payments.csv
"""

import csv

# Define file path
INPUT_FILE = '/Users/user/Desktop/Korlie_Data/Vechile_renewal/clean_paid_vehicle_renewal_payments.csv'

def main():
    print(f"Reading CSV file: {INPUT_FILE}")
    
    with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        
        # Get header
        header = next(reader)
        
        # Find the index of the 'amount' column
        try:
            amount_index = header.index('amount')
        except ValueError:
            print("Error: 'amount' column not found in the CSV file.")
            print(f"Available columns: {header}")
            return
        
        # Collect all rows and unique amounts
        rows = list(reader)
        row_count = len(rows)
        
        # Get unique values from the amount column
        unique_amounts = set()
        for row in rows:
            if len(row) > amount_index:
                unique_amounts.add(row[amount_index])
    
    print(f"\n--- Data Summary ---")
    print(f"Number of columns: {len(header)}")
    print(f"Column names: {header}")
    print(f"Total data rows (excluding header): {row_count}")
    print(f"Total data points: {row_count}")
    
    print(f"\n--- Unique Values in 'amount' Column ---")
    print(f"Total unique values: {len(unique_amounts)}")
    print(f"Unique amounts: {sorted(unique_amounts)}")

if __name__ == "__main__":
    main()
