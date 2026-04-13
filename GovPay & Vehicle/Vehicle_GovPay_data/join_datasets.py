import pandas as pd
import os

# Define file paths
vehicle_renewal_file = "/Users/user/Desktop/Korlie_Data/Vechile_renewal/renewal_vehicle.csv"
govpay_file = "/Users/user/Desktop/Korlie_Data/GovPay/GOVPAY-SLRSA.csv"
output_folder = "/Users/user/Desktop/Korlie_Data/Joined_Data"
output_file = os.path.join(output_folder, "joined_vehicle_govpay_data.csv")

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Read the datasets
print("Reading Vehicle Renewal data...")
vehicle_df = pd.read_csv(vehicle_renewal_file)
print(f"Vehicle Renewal shape: {vehicle_df.shape}")
print(f"Vehicle Renewal columns: {vehicle_df.columns.tolist()}")

print("\nReading GovPay data...")
govpay_df = pd.read_csv(govpay_file)
print(f"GovPay shape: {govpay_df.shape}")
print(f"GovPay columns: {govpay_df.columns.tolist()}")

# Check for common columns
common_cols = set(vehicle_df.columns) & set(govpay_df.columns)
print(f"\nCommon columns: {common_cols}")

# The join key appears to be application_number (customer_reference in GovPay matches application_number in Vehicle)
# Let's prepare the merge
print("\nJoining on application_number (customer_reference in GovPay)...")

# Rename GovPay columns to have gp_ prefix for clarity (after the merge)
# First, perform the merge
merged_df = vehicle_df.merge(
    govpay_df,
    left_on='application_number',
    right_on='customer_reference',
    how='left',
    suffixes=('', '_govpay')
)

# Now rename the GovPay columns for clarity
govpay_cols_to_rename = {}
for col in govpay_df.columns:
    if col != 'customer_reference':
        # If column exists in merged_df and came from govpay
        if col in merged_df.columns or f'{col}_govpay' in merged_df.columns:
            # Handle suffix cases
            if f'{col}_govpay' in merged_df.columns:
                govpay_cols_to_rename[f'{col}_govpay'] = f'gp_{col}'
            elif col in merged_df.columns and col not in vehicle_df.columns:
                govpay_cols_to_rename[col] = f'gp_{col}'

merged_df = merged_df.rename(columns=govpay_cols_to_rename)

print(f"\nMerged dataset shape: {merged_df.shape}")
print(f"Merged dataset columns: {merged_df.columns.tolist()}")

# Save the merged data
print(f"\nSaving merged data to: {output_file}")
merged_df.to_csv(output_file, index=False)
print("Done!")

# Generate summary report
summary = {
    "Vehicle Renewal Records": len(vehicle_df),
    "GovPay Records": len(govpay_df),
    "Merged Records": len(merged_df),
    "Matched Records": merged_df['gp_id'].notna().sum() if 'gp_id' in merged_df.columns else 0,
    "Unmatched Records": merged_df['gp_id'].isna().sum() if 'gp_id' in merged_df.columns else 0
}

print("\n" + "="*50)
print("MERGE SUMMARY")
print("="*50)
for key, value in summary.items():
    print(f"{key}: {value}")
print("="*50)

# Save summary to text file
summary_file = os.path.join(output_folder, "merge_summary.txt")
with open(summary_file, 'w') as f:
    f.write("MERGE SUMMARY\n")
    f.write("="*50 + "\n")
    for key, value in summary.items():
        f.write(f"{key}: {value}\n")
    f.write("="*50 + "\n")
    f.write(f"\nOutput file: {output_file}\n")

print(f"\nSummary saved to: {summary_file}")
