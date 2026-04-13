import pandas as pd
from datetime import datetime

def filter_transactions():
    # File paths
    input_file = '/Users/user/Desktop/Korlie_Data/GovPay_data/govpay-transactions-04-07-2026-morning.csv'
    output_file = '/Users/user/Desktop/Korlie_Data/GovPay_data/2nd Apr_to_6th Apr_govpay_paidto_filtered.csv'

    # Date range for filtering
    START_DATE = '2026-04-02'
    END_DATE   = '2026-04-06'

    # Read the CSV file
    df = pd.read_csv(input_file)

    # Convert created_at to datetime and extract date
    df['created_at'] = pd.to_datetime(df['created_at'], format='mixed', errors='coerce')
    df['transaction_date'] = df['created_at'].dt.date

    # Parse date range and filter (inclusive)
    start_date = datetime.strptime(START_DATE, '%Y-%m-%d').date()
    end_date = datetime.strptime(END_DATE, '%Y-%m-%d').date()
    filtered_df = df[df['transaction_date'].between(start_date, end_date)].copy()

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
    print(f"Date range:        {START_DATE} to {END_DATE}")
    print(f"Output saved to:   {output_file}")

if __name__ == "__main__":
    filter_transactions()
