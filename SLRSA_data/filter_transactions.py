"""
SLRSA Client Transactions Filter Script
========================================
Purpose: Extract transactions that occurred on January 24th, 2026 from the 
         SLRSA client transactions vehicle renewals dataset.

Input:  slrsa_client_transactions_vehicle_renewals-02-05-2026-morning.csv
Output: slrsa_client_transactions_vehicle_renewals-04-04-2026-morning_filtered.csv

Author: Data Processing Script
Date: Mar 17, 2026
"""

import pandas as pd
from datetime import datetime

# Define file paths
INPUT_FILE = '/Users/user/Desktop/Korlie_Data/SLRSA_data/slrsa-govpay-transaction.csv'
OUTPUT_FILE = '/Users/user/Desktop/Korlie_Data/SLRSA_data/9th_Apr_slrsa_client_transaction.csv'

# Target date for filtering
TARGET_DATE = '2026-04-09' 

def main():
    print("=" * 70)
    print("SLRSA Transaction Filtering Process")
    print("=" * 70)
    
    # Step 1: Load the dataset
    print(f"\n[1] Loading data from:\n    {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"    ✓ Total records loaded: {len(df):,}")
    
    # Step 2: Display initial data info
    print(f"\n[2] Dataset Information:")
    print(f"    - Total columns: {len(df.columns)}")
    print(f"    - Date column identified: 'created_at'")
    
    # Step 3: Convert 'created_at' to datetime
    print(f"\n[3] Converting 'created_at' column to datetime format...")
    df['created_at'] = pd.to_datetime(df['created_at'], format='mixed', errors='coerce')
    print(f"    ✓ Conversion complete")
    
    # Step 4: Extract date component and filter for target date
    print(f"\n[4] Filtering transactions for date: {TARGET_DATE}")
    df['transaction_date'] = df['created_at'].dt.date
    target_date = datetime.strptime(TARGET_DATE, '%Y-%m-%d').date()
    
    # Filter records for the target date
    filtered_df = df[df['transaction_date'] == target_date].copy()
    print(f"    ✓ Records found for {TARGET_DATE}: {len(filtered_df):,}")
    
    # Step 5: Drop the temporary 'transaction_date' column
    filtered_df = filtered_df.drop(columns=['transaction_date'])
    
    # Step 6: Generate summary statistics
    print(f"\n[5] Summary of Filtered Data:")
    print(f"    - Total transactions on {TARGET_DATE}: {len(filtered_df):,}")
    
    if len(filtered_df) > 0:
        # Calculate additional statistics if data exists
        if 'deposit' in filtered_df.columns:
            total_deposits = pd.to_numeric(filtered_df['deposit'], errors='coerce').sum()
            print(f"    - Total deposit amount: {total_deposits:,.2f}")
        
        if 'payment_vendor' in filtered_df.columns:
            vendors = filtered_df['payment_vendor'].value_counts()
            print(f"    - Payment vendors breakdown:")
            for vendor, count in vendors.items():
                print(f"        • {vendor}: {count:,}")
        
        if 'service_name' in filtered_df.columns:
            services = filtered_df['service_name'].value_counts()
            print(f"    - Service types breakdown:")
            for service, count in services.head(5).items():
                print(f"        • {service}: {count:,}")
    
    # Step 7: Save the filtered data
    print(f"\n[6] Saving filtered data to:\n    {OUTPUT_FILE}")
    filtered_df.to_csv(OUTPUT_FILE, index=False)
    print(f"    ✓ File saved successfully!")
    
    # Final summary
    print("\n" + "=" * 70)
    print("FILTERING PROCESS COMPLETE")
    print("=" * 70)
    print(f"\n📊 Original dataset:  {len(df):,} records")
    print(f"📋 Filtered dataset:  {len(filtered_df):,} records (transactions on {TARGET_DATE})")
    print(f"💾 Output saved to:   {OUTPUT_FILE}")
    print("\n" + "=" * 70)
    
    return filtered_df

if __name__ == "__main__":
    filtered_data = main()
