import pandas as pd

INPUT_FILE = '/Users/user/Desktop/Korlie_Data/Vechile_renewal/clean_paid_vehicle_renewal_payments.csv'
OUTPUT_CSV = '/Users/user/Desktop/Korlie_Data/Vechile_renewal/vehicle_payments_nra_filtered.csv'
OUTPUT_EXCEL = '/Users/user/Desktop/Korlie_Data/Vechile_renewal/vehicle_payments_nra_filtered.xlsx'

df = pd.read_csv(INPUT_FILE)

print(f"Original rows: {len(df)}")

# Drop rows where nra_payment is 0
df_filtered = df[df['nra_payment'] != 0]

print(f"Rows after dropping NRA = 0: {len(df_filtered)}")
print(f"Rows dropped: {len(df) - len(df_filtered)}")

# Save to CSV
df_filtered.to_csv(OUTPUT_CSV, index=False)
print(f"\nSaved CSV: {OUTPUT_CSV}")

# Save to Excel
df_filtered.to_excel(OUTPUT_EXCEL, index=False)
print(f"Saved Excel: {OUTPUT_EXCEL}")
