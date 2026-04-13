#!/usr/bin/env python3
"""
Add Application ID to Final_Vehicle_govpay.csv
==============================================
Adds the missing 'application_id' column from GovPay source data.

Author: Data Enhancement Script
Date: 2026-01-13
"""

import pandas as pd
import os

# File paths
CURRENT_FILE = '/Users/user/Desktop/Korlie_Data/Vehicle_GovPay_data/Final_Vehicle_govpay.csv'
GOVPAY_FILE = '/Users/user/Desktop/Korlie_Data/GovPay/Final_GovPay.csv'
OUTPUT_FILE = '/Users/user/Desktop/Korlie_Data/Vehicle_GovPay_data/FINAL_Vehicle_Govpay.csv'

print("=" * 100)
print("ADDING APPLICATION ID TO VEHICLE-GOVPAY MERGED DATA")
print("=" * 100)
print()

# Load current merged file
print("📂 Loading current merged file...")
current_df = pd.read_csv(CURRENT_FILE)
print(f"   ✓ Loaded: {len(current_df)} rows, {len(current_df.columns)} columns")

# Load GovPay source for application_id lookup
print("\n📂 Loading GovPay source...")
govpay_df = pd.read_csv(GOVPAY_FILE)
print(f"   ✓ Loaded: {len(govpay_df)} rows, {len(govpay_df.columns)} columns")

# Create lookup dictionary: application_number -> id
print("\n🔍 Creating application_number → application_id lookup...")
# Prepare GovPay lookup: select id, application_number, and amount/deposit for matching
govpay_lookup = govpay_df[['id', 'application_number', 'deposit', 'trans_id']].copy()
govpay_lookup = govpay_lookup.rename(columns={'id': 'application_id', 'deposit': 'govpay_amount'})

# Normalize amounts for matching
current_df['amount_normalized'] = current_df['amount'].astype(str).str.replace('.0', '', regex=False)
govpay_lookup['govpay_amount_normalized'] = govpay_lookup['govpay_amount'].astype(str).str.replace('.0', '', regex=False)

print("   ✓ Lookup table created")

# Method 1: Try to match on application_number first
print("\n🔀 Method 1: Matching on application_number...")
merged_by_app_num = pd.merge(
    current_df,
    govpay_lookup[['application_id', 'application_number', 'trans_id']].drop_duplicates(),
    on='application_number',
    how='left',
    suffixes=('', '_from_govpay')
)

matches_by_app_num = merged_by_app_num['application_id'].notna().sum()
print(f"   ✓ Matched by application_number: {matches_by_app_num} rows")

# Method 2: For unmatched rows, try matching on amount
print("\n🔀 Method 2: Matching remaining rows by amount...")
unmatched_mask = merged_by_app_num['application_id'].isna()
unmatched_count_before = unmatched_mask.sum()

if unmatched_count_before > 0:
    # For unmatched rows, try matching by amount
    unmatched_rows = current_df[unmatched_mask].copy()
    
    # Match on amount
    amount_matched = pd.merge(
        unmatched_rows,
        govpay_lookup[['application_id', 'govpay_amount_normalized', 'trans_id']].drop_duplicates(subset=['govpay_amount_normalized']),
        left_on='amount_normalized',
        right_on='govpay_amount_normalized',
        how='left'
    )
    
    # Update the merged dataframe with amount matches
    merged_by_app_num.loc[unmatched_mask, 'application_id'] = amount_matched['application_id'].values
    merged_by_app_num.loc[unmatched_mask, 'trans_id'] = amount_matched['trans_id'].values
    
    matches_by_amount = merged_by_app_num['application_id'].notna().sum() - matches_by_app_num
    print(f"   ✓ Additional matches by amount: {matches_by_amount} rows")

# Final statistics
total_matched = merged_by_app_num['application_id'].notna().sum()
total_unmatched = merged_by_app_num['application_id'].isna().sum()

print("\n📊 Matching Summary:")
print(f"   - Total rows: {len(merged_by_app_num)}")
print(f"   - Rows with application_id: {total_matched} ({100*total_matched/len(merged_by_app_num):.1f}%)")
print(f"   - Rows without application_id: {total_unmatched} ({100*total_unmatched/len(merged_by_app_num):.1f}%)")

# Prepare final output
print("\n🏗️  Building final output...")

# Drop temporary columns
final_df = merged_by_app_num.drop(columns=['amount_normalized', 'trans_id'], errors='ignore')

# Reorder columns: application_id first, then the rest
columns_order = ['application_id'] + [col for col in current_df.columns if col not in ['amount_normalized']]

final_df = final_df[columns_order]

# Verify required columns
print("\n✅ Verifying required columns:")
required = ['application_id', 'application_number', 'paid_at']
for col in required:
    if col in final_df.columns:
        non_null = final_df[col].notna().sum()
        print(f"   ✓ {col}: Present ({non_null}/{len(final_df)} non-null)")
    else:
        print(f"   ✗ {col}: MISSING")

# Save to new file
print(f"\n💾 Saving enhanced data...")
final_df.to_csv(OUTPUT_FILE, index=False)
print(f"   ✓ Saved to: {OUTPUT_FILE}")
print(f"   ✓ Total rows: {len(final_df)}")
print(f"   ✓ Total columns: {len(final_df.columns)}")

# Show sample
print("\n📋 Sample Data (First 3 rows):")
sample_cols = ['application_id', 'application_number', 'amount', 'paid_at', 'applicant_fullname']
print(final_df[sample_cols].head(3).to_string(index=False))

print("\n" + "=" * 100)
print("✅ ENHANCEMENT COMPLETE!")
print("=" * 100)
print(f"\nNew file created: FINAL_Vehicle_Govpay.csv")
print(f"Location: /Users/user/Desktop/Korlie_Data/Vehicle_GovPay_data/")
print()
