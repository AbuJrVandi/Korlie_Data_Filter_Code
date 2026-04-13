"""
Script to filter GOVPAY-SLRSA.csv by date range: December 19 - December 31, 2025
Uses only built-in Python modules (no pandas required)
"""

import csv
from datetime import datetime

# Define file paths
INPUT_FILE = '/Users/user/Desktop/Korlie_Data/GovPay/GOVPAY-SLRSA.csv'
OUTPUT_FILE = '/Users/user/Desktop/Korlie_Data/GovPay/GOVPAY-SLRSA_Dec19-31_2025.csv'

# Define the date range
START_DATE = datetime(2025, 12, 19, 0, 0, 0)
END_DATE = datetime(2025, 12, 31, 23, 59, 59)

def parse_datetime(date_str):
    """Parse datetime string in format 'YYYY-MM-DD HH:MM:SS'"""
    try:
        return datetime.strptime(date_str.strip(), '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None

def main():
    print(f"Reading CSV file: {INPUT_FILE}")
    
    total_rows = 0
    filtered_rows = 0
    
    with open(INPUT_FILE, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        
        # Find the column index for 'created_at'
        if 'created_at' not in fieldnames:
            print("Error: 'created_at' column not found in CSV!")
            return
        
        # Collect filtered rows
        filtered_data = []
        
        for row in reader:
            total_rows += 1
            created_at = row.get('created_at', '')
            
            dt = parse_datetime(created_at)
            if dt and START_DATE <= dt <= END_DATE:
                filtered_data.append(row)
                filtered_rows += 1
    
    print(f"Total rows in original file: {total_rows}")
    print(f"Rows matching date range (Dec 19-31, 2025): {filtered_rows}")
    
    # Write filtered data to output file
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered_data)
    
    print(f"Filtered data saved to: {OUTPUT_FILE}")
    
    # Display summary
    if filtered_data:
        print("\n--- Summary ---")
        dates = [parse_datetime(row['created_at']) for row in filtered_data]
        print(f"First row date: {min(dates)}")
        print(f"Last row date: {max(dates)}")
        print(f"\nFirst 5 rows of filtered data (created_at column):")
        for row in filtered_data[:5]:
            print(f"  {row['created_at']} - Trans ID: {row.get('trans_id', 'N/A')}")
    else:
        print("\nNo rows found in the specified date range.")

if __name__ == "__main__":
    main()
