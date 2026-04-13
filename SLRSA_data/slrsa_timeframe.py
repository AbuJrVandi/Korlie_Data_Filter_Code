"""
SLRSA Client Transactions Filter Script
========================================
Purpose: Extract transactions that occurred between
         January 30, 2026 and February 1, 2026 (inclusive)
         from the SLRSA client transactions vehicle renewals dataset.

Input:  slrsa_client_transactions_vehicle_renewals-02-02-2026-morning.csv
Output: slrsa_client_transactions_vehicle_renewals-01-30_to_02-01-2026_filtered.csv

Author: Data Processing Script
Date: February 2, 2026
"""

import pandas as pd
from datetime import datetime

# Define file paths
INPUT_FILE = '/Users/user/Desktop/Korlie_Data/SLRSA_data/slrsa-govpay-transactions-04-07-2026-morning.csv'
OUTPUT_FILE = '/Users/user/Desktop/Korlie_Data/SLRSA_data/2nd_Apr_to_6th_Apr_slrsa_client_transactions_2026-04-06_filtered.csv'

# Date range for filtering
START_DATE = '2026-04-02'
END_DATE   = '2026-04-06'

def main():
    print("=" * 70)
    print("SLRSA Transaction Date-Range Filtering Process")
    print("=" * 70)

    # Step 1: Load dataset
    print(f"\n[1] Loading data from:\n    {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"    ✓ Total records loaded: {len(df):,}")

    # Step 2: Dataset info
    print(f"\n[2] Dataset Information:")
    print(f"    - Total columns: {len(df.columns)}")
    print(f"    - Date column identified: 'created_at'")

    # Step 3: Convert 'created_at' to datetime
    print(f"\n[3] Converting 'created_at' column to datetime format...")
    df['created_at'] = pd.to_datetime(df['created_at'], format='mixed', errors='coerce')
    print(f"    ✓ Conversion complete")

    # Step 4: Extract date and filter range
    print(f"\n[4] Filtering transactions from {START_DATE} to {END_DATE}")
    df['transaction_date'] = df['created_at'].dt.date

    start_date = datetime.strptime(START_DATE, '%Y-%m-%d').date()
    end_date   = datetime.strptime(END_DATE, '%Y-%m-%d').date()

    filtered_df = df[
        df['transaction_date'].between(start_date, end_date)
    ].copy()

    print(f"    ✓ Records found in date range: {len(filtered_df):,}")

    # Step 5: Drop helper column
    filtered_df.drop(columns=['transaction_date'], inplace=True)

    # Step 6: Summary statistics
    print(f"\n[5] Summary of Filtered Data:")
    print(f"    - Total transactions: {len(filtered_df):,}")

    if not filtered_df.empty:

        if 'deposit' in filtered_df.columns:
            total_deposits = pd.to_numeric(filtered_df['deposit'], errors='coerce').sum()
            print(f"    - Total deposit amount: {total_deposits:,.2f}")

        if 'payment_vendor' in filtered_df.columns:
            print(f"    - Payment vendors breakdown:")
            for vendor, count in filtered_df['payment_vendor'].value_counts().items():
                print(f"        • {vendor}: {count:,}")

        if 'service_name' in filtered_df.columns:
            print(f"    - Service types breakdown:")
            for service, count in filtered_df['service_name'].value_counts().head(5).items():
                print(f"        • {service}: {count:,}")

    # Step 7: Save output
    print(f"\n[6] Saving filtered data to:\n    {OUTPUT_FILE}")
    filtered_df.to_csv(OUTPUT_FILE, index=False)
    print(f"    ✓ File saved successfully!")

    # Final summary
    print("\n" + "=" * 70)
    print("FILTERING PROCESS COMPLETE")
    print("=" * 70)
    print(f"📊 Original dataset: {len(df):,} records")
    print(f"📋 Filtered dataset: {len(filtered_df):,} records")
    print(f"📅 Date range: {START_DATE} → {END_DATE}")
    print(f"💾 Output saved to: {OUTPUT_FILE}")
    print("=" * 70)

    return filtered_df

if __name__ == "__main__":
    filtered_data = main()
