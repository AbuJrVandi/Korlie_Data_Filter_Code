"""
=================================================================================
 FINAL VEHICLE-GOVPAY DATA TRANSFORMATION SCRIPT
=================================================================================
 Purpose: Transform merged vehicle-govpay data with specified column mappings
          
 Transformation Rules:
   1. application_number <- gp_customer_reference
   2. transaction_id     <- gp_transaction_id  
   3. Replace all "mohamed james" with "wangov wangov" (case insensitive)
 
 Output: Final_Vehicle_govpay.csv
 
 Author: Data Science Team
 Date: January 13, 2026
=================================================================================
"""

import pandas as pd
import os
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

INPUT_FILE = "/Users/user/Desktop/Korlie_Data/Vehicle_GovPay_data/merged_vehicle_govpay_data.csv"
OUTPUT_DIR = "/Users/user/Desktop/Korlie_Data/Vehicle_GovPay_data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "Final_Vehicle_govpay.csv")


def main():
    """
    Main function to transform the vehicle-govpay data.
    """
    print("\n" + "=" * 70)
    print("  FINAL VEHICLE-GOVPAY DATA TRANSFORMATION")
    print("=" * 70)
    
    # =========================================================================
    # STEP 1: Load the merged data
    # =========================================================================
    print("\n📂 Loading merged data...")
    df = pd.read_csv(INPUT_FILE)
    print(f"   ✅ Loaded {len(df):,} records")
    
    # =========================================================================
    # STEP 2: Create transformed DataFrame with specified columns
    # =========================================================================
    print("\n📝 Applying transformations...")
    
    # Create new DataFrame with mapped columns
    final_df = pd.DataFrame()
    
    # Rule 1: transaction_id <- gp_transaction_id
    final_df['transaction_id'] = df['gp_transaction_id']
    print("   ✅ transaction_id <- gp_transaction_id")
    
    # Rule 2: application_number <- gp_customer_reference
    final_df['application_number'] = df['gp_customer_reference']
    print("   ✅ application_number <- gp_customer_reference")
    
    # Map remaining columns
    final_df['billing_status'] = df['vp_billing_status']
    final_df['auto_spec_payment'] = df['vp_auto_spec_payment']
    final_df['slrsa_payment'] = df['vp_slrsa_payment']
    final_df['nra_payment'] = df['vp_nra_payment']
    final_df['amount'] = df['amount']
    final_df['payment_vendor'] = df['gp_payment_vendor']
    final_df['service_name'] = df['vp_service_name']
    final_df['applicant_fullname'] = df['vp_applicant_name']
    final_df['applicant_phonenumber'] = df['vp_applicant_phone']
    final_df['paid_at'] = df['gp_transaction_datetime']
    
    # =========================================================================
    # STEP 3: Replace "mohamed james" with "wangov wangov"
    # =========================================================================
    print("\n📝 Replacing 'mohamed james' with 'wangov wangov'...")
    
    # Count before replacement
    before_count = final_df['applicant_fullname'].str.lower().str.contains(
        'mohamed james', na=False
    ).sum()
    
    # Replace (case insensitive)
    final_df['applicant_fullname'] = final_df['applicant_fullname'].str.replace(
        r'(?i)mohamed james',
        'wangov wangov',
        regex=True
    )
    
    # Count after replacement
    after_count = final_df['applicant_fullname'].str.lower().str.contains(
        'mohamed james', na=False
    ).sum()
    
    print(f"   ✅ Replaced {before_count:,} occurrences")
    print(f"   📊 'mohamed james' remaining: {after_count}")
    
    # =========================================================================
    # STEP 4: Save to Final_Vehicle_govpay.csv
    # =========================================================================
    print("\n💾 Saving final data...")
    final_df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"   ✅ Saved to: {OUTPUT_FILE}")
    print(f"   📊 Total Records: {len(final_df):,}")
    print(f"   📋 Total Columns: {len(final_df.columns)}")
    
    # =========================================================================
    # STEP 5: Display Summary
    # =========================================================================
    print("\n" + "=" * 70)
    print("  TRANSFORMATION SUMMARY")
    print("=" * 70)
    print(f"""
    Input File:  merged_vehicle_govpay_data.csv
    Output File: Final_Vehicle_govpay.csv
    
    Column Mappings:
    ─────────────────────────────────────────────────────
    transaction_id      <- gp_transaction_id
    application_number  <- gp_customer_reference
    billing_status      <- vp_billing_status
    auto_spec_payment   <- vp_auto_spec_payment
    slrsa_payment       <- vp_slrsa_payment
    nra_payment         <- vp_nra_payment
    amount              <- amount
    payment_vendor      <- gp_payment_vendor
    service_name        <- vp_service_name
    applicant_fullname  <- vp_applicant_name (with replacement)
    applicant_phonenumber <- vp_applicant_phone
    paid_at             <- gp_transaction_datetime
    
    Name Replacement:
    ─────────────────────────────────────────────────────
    'mohamed james' -> 'wangov wangov' (case insensitive)
    Occurrences replaced: {before_count:,}
    """)
    
    # Preview
    print("\n📋 Preview (first 5 rows):")
    print("-" * 70)
    print(final_df.head().to_string())
    
    print("\n" + "=" * 70)
    print("  ✅ TRANSFORMATION COMPLETE!")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
