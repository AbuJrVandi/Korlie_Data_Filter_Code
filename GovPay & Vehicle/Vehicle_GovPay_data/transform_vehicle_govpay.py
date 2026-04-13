"""
=================================================================================
 VEHICLE-GOVPAY DATA TRANSFORMATION SCRIPT
=================================================================================
 Purpose: Transform merged vehicle-govpay data with proper column mapping
          and name replacement.
 
 Transformations:
   - Use gp_transaction_id as transaction_id
   - Use gp_customer_reference as application_number  
   - Replace all "mohamed james" with "wangov wangov"
   - Select and organize specified output columns
 
 Author: Data Science Team
 Date: January 13, 2026
=================================================================================
"""

import pandas as pd
import os
from datetime import datetime

# =============================================================================
# FILE PATHS CONFIGURATION
# =============================================================================

# Input file (merged data)
INPUT_FILE = "/Users/user/Desktop/Korlie_Data/Vehicle_GovPay_data/merged_vehicle_govpay_data.csv"

# Output directory and file
OUTPUT_DIR = "/Users/user/Desktop/Korlie_Data/Vehicle_GovPay_data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "transformed_vehicle_govpay_data.csv")
SUMMARY_FILE = os.path.join(OUTPUT_DIR, "transformation_summary_report.txt")


def load_merged_data():
    """
    Load the merged vehicle-govpay data CSV file.
    """
    print("\n" + "="*70)
    print(" LOADING MERGED DATA")
    print("="*70)
    
    print(f"\n📂 Loading merged data from:\n   {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE)
    print(f"   ✅ Loaded {len(df):,} records")
    print(f"   📋 Columns: {len(df.columns)}")
    
    return df


def transform_data(df):
    """
    Transform the data according to specifications:
    1. Map gp_transaction_id to transaction_id
    2. Map gp_customer_reference to application_number
    3. Replace 'mohamed james' with 'wangov wangov'
    4. Select required columns
    """
    print("\n" + "="*70)
    print(" TRANSFORMING DATA")
    print("="*70)
    
    # Create a copy to avoid modifying the original
    transformed_df = df.copy()
    
    # -------------------------------------------------------------------------
    # Step 1: Create new columns with proper mapping
    # -------------------------------------------------------------------------
    print("\n📝 Creating mapped columns...")
    
    # transaction_id from gp_transaction_id
    transformed_df['transaction_id'] = transformed_df['gp_transaction_id']
    print("   ✅ transaction_id <- gp_transaction_id")
    
    # application_number from gp_customer_reference
    transformed_df['application_number'] = transformed_df['gp_customer_reference']
    print("   ✅ application_number <- gp_customer_reference")
    
    # Map other columns from their vp_ prefixed versions
    transformed_df['billing_status'] = transformed_df['vp_billing_status']
    transformed_df['auto_spec_payment'] = transformed_df['vp_auto_spec_payment']
    transformed_df['slrsa_payment'] = transformed_df['vp_slrsa_payment']
    transformed_df['nra_payment'] = transformed_df['vp_nra_payment']
    # amount is already present without prefix
    transformed_df['payment_vendor'] = transformed_df['gp_payment_vendor']  # Use gp payment vendor
    transformed_df['service_name'] = transformed_df['vp_service_name']
    transformed_df['applicant_fullname'] = transformed_df['vp_applicant_name']
    transformed_df['applicant_phonenumber'] = transformed_df['vp_applicant_phone']
    transformed_df['paid_at'] = transformed_df['gp_transaction_datetime']  # Use gp datetime
    
    print("   ✅ All column mappings completed")
    
    # -------------------------------------------------------------------------
    # Step 2: Replace 'mohamed james' with 'wangov wangov' (case insensitive)
    # -------------------------------------------------------------------------
    print("\n📝 Replacing 'mohamed james' with 'wangov wangov'...")
    
    # Count occurrences before replacement
    if 'applicant_fullname' in transformed_df.columns:
        # Case insensitive count
        original_count = transformed_df['applicant_fullname'].str.lower().str.contains('mohamed james', na=False).sum()
        
        # Replace (case insensitive)
        transformed_df['applicant_fullname'] = transformed_df['applicant_fullname'].replace(
            to_replace=r'(?i)mohamed james', 
            value='WANGOV WANGOV', 
            regex=True
        )
        
        # Verify replacement
        after_count = transformed_df['applicant_fullname'].str.lower().str.contains('mohamed james', na=False).sum()
        
        print(f"   ✅ Replaced {original_count - after_count:,} occurrences")
        print(f"   📊 Before: {original_count:,} 'mohamed james' entries")
        print(f"   📊 After: {after_count:,} 'mohamed james' entries")
    
    # -------------------------------------------------------------------------
    # Step 3: Select only the required output columns
    # -------------------------------------------------------------------------
    print("\n📝 Selecting required output columns...")
    
    output_columns = [
        'transaction_id',
        'application_number',
        'billing_status',
        'auto_spec_payment',
        'slrsa_payment',
        'nra_payment',
        'amount',
        'payment_vendor',
        'service_name',
        'applicant_fullname',
        'applicant_phonenumber',
        'paid_at'
    ]
    
    # Select only the specified columns
    final_df = transformed_df[output_columns].copy()
    
    print(f"   ✅ Selected {len(output_columns)} columns for output")
    for col in output_columns:
        print(f"      • {col}")
    
    return final_df


def generate_summary(original_df, transformed_df):
    """
    Generate a summary report of the transformation.
    """
    print("\n" + "="*70)
    print(" GENERATING SUMMARY REPORT")
    print("="*70)
    
    # Calculate statistics
    total_records = len(transformed_df)
    null_transaction_id = transformed_df['transaction_id'].isna().sum()
    null_application_number = transformed_df['application_number'].isna().sum()
    wangov_count = transformed_df['applicant_fullname'].str.lower().str.contains('wangov', na=False).sum()
    
    # Unique values
    unique_amounts = transformed_df['amount'].nunique()
    unique_vendors = transformed_df['payment_vendor'].dropna().nunique()
    
    report = f"""
{'='*80}
             VEHICLE-GOVPAY DATA TRANSFORMATION SUMMARY
{'='*80}

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SOURCE FILE
{'='*80}
• Input: merged_vehicle_govpay_data.csv
• Original Records: {len(original_df):,}

TRANSFORMATION RESULTS
{'='*80}
• Total Transformed Records:          {total_records:>10,}
• Records with valid transaction_id:  {total_records - null_transaction_id:>10,}
• Records with valid application_no:  {total_records - null_application_number:>10,}
• Records with missing values:        {null_transaction_id:>10,}

COLUMN MAPPING
{'='*80}
• transaction_id     <- gp_transaction_id
• application_number <- gp_customer_reference
• billing_status     <- vp_billing_status
• auto_spec_payment  <- vp_auto_spec_payment
• slrsa_payment      <- vp_slrsa_payment
• nra_payment        <- vp_nra_payment
• amount             <- amount (unchanged)
• payment_vendor     <- gp_payment_vendor
• service_name       <- vp_service_name
• applicant_fullname <- vp_applicant_name (with 'WANGOV WANGOV' replacement)
• applicant_phonenumber <- vp_applicant_phone
• paid_at            <- gp_transaction_datetime

NAME REPLACEMENT
{'='*80}
• Pattern Replaced: 'mohamed james' -> 'WANGOV WANGOV' (case insensitive)
• Records with 'WANGOV WANGOV': {wangov_count:,}

DATA QUALITY SUMMARY
{'='*80}
• Unique Payment Amounts: {unique_amounts:,}
• Unique Payment Vendors: {unique_vendors:,}

OUTPUT FILES
{'='*80}
• Transformed Data: transformed_vehicle_govpay_data.csv
• Summary Report: transformation_summary_report.txt

{'='*80}
                         END OF REPORT
{'='*80}
"""
    
    # Save report to file
    with open(SUMMARY_FILE, 'w') as f:
        f.write(report)
    
    print(report)
    print(f"\n📄 Summary report saved to:\n   {SUMMARY_FILE}")
    
    return report


def save_transformed_data(df):
    """
    Save the transformed DataFrame to CSV file.
    """
    print("\n" + "="*70)
    print(" SAVING TRANSFORMED DATA")
    print("="*70)
    
    # Save to CSV
    df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"\n💾 Transformed data saved successfully!")
    print(f"   📁 File: {OUTPUT_FILE}")
    print(f"   📊 Total Records: {len(df):,}")
    print(f"   📋 Total Columns: {len(df.columns)}")
    
    # Preview first few rows
    print("\n📋 Preview of transformed data (first 5 rows):")
    print("-" * 70)
    print(df.head().to_string())


def main():
    """
    Main execution function for the data transformation.
    """
    print("\n")
    print("█" * 70)
    print("█" + " " * 68 + "█")
    print("█" + "  VEHICLE-GOVPAY DATA TRANSFORMATION SYSTEM  ".center(68) + "█")
    print("█" + " " * 68 + "█")
    print("█" * 70)
    
    try:
        # Step 1: Load merged data
        original_df = load_merged_data()
        
        # Step 2: Transform data
        transformed_df = transform_data(original_df)
        
        # Step 3: Save transformed data
        save_transformed_data(transformed_df)
        
        # Step 4: Generate summary report
        generate_summary(original_df, transformed_df)
        
        print("\n" + "="*70)
        print(" ✅ DATA TRANSFORMATION COMPLETED SUCCESSFULLY!")
        print("="*70)
        print(f"\n📁 Output Location: {OUTPUT_DIR}")
        print("\n" + "█" * 70 + "\n")
        
    except FileNotFoundError as e:
        print(f"\n❌ ERROR: File not found - {e}")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        raise


if __name__ == "__main__":
    main()
