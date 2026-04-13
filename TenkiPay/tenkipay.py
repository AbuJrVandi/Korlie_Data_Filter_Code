import pandas as pd

# Define the file paths
file_path = '/Users/user/Desktop/Korlie_Data/TenkiPay/tenkipay-transactions-04-10-2026-morning.csv'
output_path = '/Users/user/Desktop/Korlie_Data/TenkiPay/9th Apr_tenkipay-transactions_filtered.csv'

# Read the CSV file
df = pd.read_csv(file_path)


# Convert created_at to datetime
df['created_at'] = pd.to_datetime(df['created_at'])

# Target date: 29 January 2026
target_date = pd.to_datetime('2026-04-09').date()

# Filter for the specific date
filtered_df = df[df['created_at'].dt.date == target_date]

# Save the filtered data to a new CSV file
filtered_df.to_csv(output_path, index=False)

# Display a summary
print(f"Filtered {len(filtered_df)} transactions for {target_date}.")
print(f"Saved to: {output_path}")

# Terminal summary output
print("=" * 70)
print("FILTERING PROCESS COMPLETE")
print("=" * 70)
print()
print(f"📊 Original dataset:  {len(df):,} records")
print(f"📋 Filtered dataset:  {len(filtered_df):,} records (transactions on {target_date})")
print(f"💾 Output saved to:   {output_path}")
print 