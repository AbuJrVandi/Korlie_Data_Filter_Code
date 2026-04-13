import pandas as pd
from datetime import datetime


def filter_transactions_for_date():
    # File paths
    input_file = '/Users/user/Desktop/Korlie_Data/GovPay_data/govpay-transactions-04-10-2026-morning.csv'
    output_file = '/Users/user/Desktop/Korlie_Data/GovPay_data/9th_Apr_govpay_transactions_2026-04-09.csv'

    # Date for filtering (single day)
    TARGET_DATE = '2026-04-09'

    # Read the CSV file
    df = pd.read_csv(input_file)

    # Convert created_at to datetime and extract date
    df['created_at'] = pd.to_datetime(df['created_at'], format='mixed', errors='coerce')
    df['transaction_date'] = df['created_at'].dt.date

    # Parse target date and filter
    target_date = datetime.strptime(TARGET_DATE, '%Y-%m-%d').date()
    filtered_df = df[df['transaction_date'] == target_date].copy()

    # Drop helper column
    filtered_df.drop(columns=['transaction_date'], inplace=True)

    # Save the filtered data to a new CSV file
    filtered_df.to_csv(output_file, index=False)

    # Terminal summary output
    print("=" * 70)
    print("FILTERING PROCESS COMPLETE")
    print("=" * 70)
    print(f"Original dataset:  {len(df):,} records")
    print(f"Filtered dataset:  {len(filtered_df):,} records")
    print(f"Filter date:       {TARGET_DATE}")
    print(f"Output saved to:   {output_file}")


if __name__ == "__main__":
    filter_transactions_for_date()
