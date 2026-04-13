"""
Transaction Filtering Script
=============================
Purpose: Extract transactions from a specific date (January 15, 2026) from the 
         vehicle renewal dataset and save the filtered results to a new CSV file.

Source File: renewal_vehicle.csv
Output File: renewal_vehicle_filtered.csv

Author: AI Agent
Date: January 16, 2026
"""

import pandas as pd
from datetime import datetime

# Define file paths
INPUT_FILE = "/Users/user/Desktop/Korlie_Data/GovPay & Vehicle/Vechile_renewal/renewal_vehicle.csv"
OUTPUT_FILE = "/Users/user/Desktop/Korlie_Data/GovPay & Vehicle/Vechile_renewal/new_renewal.csv"

# Define the target date for filtering
TARGET_DATE = "2026-01-15"

def filter_transactions_by_date(input_path: str, output_path: str, target_date: str) -> None:
    """
    Filter transactions from a CSV file based on a specific date.
    
    Parameters:
    -----------
    input_path : str
        Path to the source CSV file containing transaction data.
    output_path : str
        Path where the filtered CSV file will be saved.
    target_date : str
        The date to filter by in 'YYYY-MM-DD' format.
    
    Returns:
    --------
    None
    """
    print("=" * 60)
    print("TRANSACTION FILTERING REPORT")
    print("=" * 60)
    
    # Step 1: Load the dataset
    print(f"\n[1] Loading data from: {input_path}")
    df = pd.read_csv(input_path)
    total_records = len(df)
    print(f"    Total records loaded: {total_records:,}")
    
    # Step 2: Convert 'paid_at' column to datetime format
    print(f"\n[2] Converting 'paid_at' column to datetime format...")
    df['paid_at'] = pd.to_datetime(df['paid_at'])
    
    # Step 3: Extract the date portion for filtering
    print(f"\n[3] Analyzing date range...")
    df['transaction_date'] = df['paid_at'].dt.date
    min_date = df['transaction_date'].min()
    max_date = df['transaction_date'].max()
    print(f"    Date Range: {min_date} to {max_date}")

    print(f"\n[4] Filtering transactions for date: {target_date}")
    
    # Convert target date string to date object for comparison
    target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
    
    # Check if target date is within range
    if not (min_date <= target_date_obj <= max_date):
        print(f"    WARNING: Target date {target_date} is outside the dataset range!")

    # Filter transactions for the target date
    filtered_df = df[df['transaction_date'] == target_date_obj].copy()
    filtered_count = len(filtered_df)
    
    print(f"    Transactions found for {target_date}: {filtered_count:,}")
    
    # Step 4: Remove the helper column before saving
    filtered_df = filtered_df.drop(columns=['transaction_date'])
    
    # Step 5: Save the filtered data to CSV
    # Step 5: Save the filtered data to CSV
    print(f"\n[5] Saving filtered data to: {output_path}")
    filtered_df.to_csv(output_path, index=False)
    print(f"    File saved successfully!")
    
    # Step 6: Display summary statistics
    print("\n" + "=" * 60)
    print("FILTERING SUMMARY")
    print("=" * 60)
    print(f"  - Source File:           {input_path.split('/')[-1]}")
    print(f"  - Data Date Range:       {min_date} to {max_date}")
    print(f"  - Target Date:           {target_date}")
    print(f"  - Total Records (Source): {total_records:,}")
    print(f"  - Filtered Records:      {filtered_count:,}")
    
    if filtered_count > 0:
        print(f"  - Output File:           {output_path.split('/')[-1]}")
        print(f"\n[PREVIEW] First 5 filtered transactions:")
        print("-" * 60)
        preview_cols = ['transaction_id', 'application_number', 'amount', 'paid_at']
        print(filtered_df[preview_cols].head().to_string(index=False))
        
        # Additional statistics
        print(f"\n[STATISTICS]")
        print(f"  - Total Amount:          {filtered_df['amount'].sum():,.2f}")
        print(f"  - Average Amount:        {filtered_df['amount'].mean():,.2f}")
        print(f"  - Min Amount:            {filtered_df['amount'].min():,.2f}")
        print(f"  - Max Amount:            {filtered_df['amount'].max():,.2f}")
    else:
        print(f"\n[INFO] No transactions found for the specified date: {target_date}")
        print(f"[INFO] The dataset appears to end on {max_date}.")
    
    print("\n" + "=" * 60)
    print("PROCESS COMPLETED SUCCESSFULLY")
    print("=" * 60)


if __name__ == "__main__":
    filter_transactions_by_date(INPUT_FILE, OUTPUT_FILE, TARGET_DATE)
