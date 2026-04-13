"""
=================================================================================
 VEHICLE PAYMENT & GOVPAY DATA MERGE SCRIPT
=================================================================================
 Purpose: Left join merge Vehicle Payment data with GovPay transaction data
          using the payment amount as the matching key.
 
 Data Flow: GovPay -> Payment Gateway -> Vehicle Registration System
            This script maps GovPay transactions to their corresponding
            vehicle registration payments.
 
 Author: Data Integration Team
 Date: January 13, 2026
=================================================================================
"""

import pandas as pd
import os
from datetime import datetime

# =============================================================================
# FILE PATHS CONFIGURATION
# =============================================================================

# Input files
GOVPAY_FILE = "/Users/user/Desktop/Korlie_Data/GovPay/GOVPAY-SLRSA_Dec19-31_2025.csv"
VEHICLE_FILE = "/Users/user/Desktop/Korlie_Data/Vechile_renewal/vehicle_payments_nra_filtered.csv"

# Output directory and file
OUTPUT_DIR = "/Users/user/Desktop/Korlie_Data/Vehicle_GovPay_data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "merged_vehicle_govpay_data.csv")
SUMMARY_FILE = os.path.join(OUTPUT_DIR, "merge_summary_report.txt")


def load_and_prepare_data():
    """
    Load and prepare both datasets for merging.
    Returns prepared DataFrames with properly renamed columns.
    """
    print("\n" + "="*70)
    print(" LOADING DATA FILES")
    print("="*70)
    
    # Load GovPay data
    print(f"\n📂 Loading GovPay data from:\n   {GOVPAY_FILE}")
    govpay_df = pd.read_csv(GOVPAY_FILE)
    print(f"   ✅ Loaded {len(govpay_df):,} GovPay records")
    
    # Load Vehicle Payment data
    print(f"\n📂 Loading Vehicle Payment data from:\n   {VEHICLE_FILE}")
    vehicle_df = pd.read_csv(VEHICLE_FILE)
    print(f"   ✅ Loaded {len(vehicle_df):,} Vehicle Payment records")
    
    return govpay_df, vehicle_df


def rename_columns_professionally(govpay_df, vehicle_df):
    """
    Rename columns with professional naming convention to clearly
    distinguish between GovPay and Vehicle Payment data.
    """
    print("\n" + "="*70)
    print(" RENAMING COLUMNS FOR CLARITY")
    print("="*70)
    
    # Rename GovPay columns with 'gp_' prefix (Payment Gateway data)
    govpay_columns = {
        'id': 'gp_record_id',
        'trans_id': 'gp_transaction_id',
        'customer_accountnumber': 'gp_customer_phone',
        'customer_reference': 'gp_customer_reference',
        'client_accountnumber': 'gp_client_account',
        'client_name': 'gp_client_name',
        'client_id': 'gp_client_id',
        'client_type': 'gp_client_type',
        'client_user_name': 'gp_client_username',
        'client_email': 'gp_client_email',
        'payment_vendor': 'gp_payment_vendor',
        'deposit': 'amount',  # Key column for merging
        'payout': 'gp_payout',
        'balance': 'gp_balance',
        'created_at': 'gp_transaction_datetime',
        'updated_at': 'gp_updated_datetime',
        'transaction_type': 'gp_transaction_type',
        'govpay_trans_id': 'gp_govpay_id',
        'transaction_status': 'gp_status'
    }
    
    # Rename Vehicle Payment columns with 'vp_' prefix (Vehicle Payment data)
    vehicle_columns = {
        'transaction_id': 'vp_transaction_id',
        'application_number': 'vp_application_number',
        'billing_status': 'vp_billing_status',
        'auto_spec_payment': 'vp_auto_spec_payment',
        'slrsa_payment': 'vp_slrsa_payment',
        'nra_payment': 'vp_nra_payment',
        'amount': 'amount',  # Key column for merging
        'payment_vendor': 'vp_payment_vendor',
        'service_name': 'vp_service_name',
        'applicant_fullname': 'vp_applicant_name',
        'applicant_phonenumber': 'vp_applicant_phone',
        'paid_at': 'vp_payment_datetime'
    }
    
    # Apply renaming
    govpay_renamed = govpay_df.rename(columns=govpay_columns)
    vehicle_renamed = vehicle_df.rename(columns=vehicle_columns)
    
    print("\n📝 GovPay columns renamed with 'gp_' prefix (Gateway Provider)")
    print("📝 Vehicle Payment columns renamed with 'vp_' prefix (Vehicle Payment)")
    print("📝 'amount' column retained for merge key in both datasets")
    
    return govpay_renamed, vehicle_renamed


def perform_left_join_merge(vehicle_df, govpay_df):
    """
    Perform LEFT JOIN merge using amount as the merge key.
    Vehicle Payment data is the LEFT table (primary dataset).
    """
    print("\n" + "="*70)
    print(" PERFORMING LEFT JOIN MERGE")
    print("="*70)
    
    print("\n🔗 Merge Strategy:")
    print("   • LEFT Table: Vehicle Payment Data (primary dataset)")
    print("   • RIGHT Table: GovPay Transaction Data")
    print("   • Merge Key: amount (payment amount)")
    print("   • Join Type: LEFT JOIN (keep all vehicle payments)")
    
    # Perform the left join merge
    merged_df = pd.merge(
        vehicle_df,
        govpay_df,
        on='amount',
        how='left',
        suffixes=('', '_govpay')
    )
    
    print(f"\n✅ Merge completed successfully!")
    print(f"   • Total merged records: {len(merged_df):,}")
    
    return merged_df


def organize_columns(merged_df):
    """
    Organize columns in a logical order for better readability.
    """
    print("\n" + "="*70)
    print(" ORGANIZING COLUMNS")
    print("="*70)
    
    # Define column order for professional organization
    # 1. Key/Identifier columns
    # 2. Amount/Financial data
    # 3. Vehicle Payment details
    # 4. GovPay transaction details
    
    priority_columns = [
        # Merge Key
        'amount',
        
        # Vehicle Payment Identifiers
        'vp_application_number',
        'vp_transaction_id',
        
        # GovPay Identifiers
        'gp_record_id',
        'gp_transaction_id',
        'gp_govpay_id',
        
        # Vehicle Payment Financial Details
        'vp_auto_spec_payment',
        'vp_slrsa_payment',
        'vp_nra_payment',
        
        # Vehicle Payment Information
        'vp_billing_status',
        'vp_service_name',
        'vp_applicant_name',
        'vp_applicant_phone',
        'vp_payment_vendor',
        'vp_payment_datetime',
        
        # GovPay Transaction Details
        'gp_customer_phone',
        'gp_customer_reference',
        'gp_payment_vendor',
        'gp_transaction_type',
        'gp_balance',
        'gp_payout',
        'gp_transaction_datetime',
        'gp_updated_datetime',
        'gp_status',
        
        # GovPay Client Information
        'gp_client_account',
        'gp_client_name',
        'gp_client_id',
        'gp_client_type',
        'gp_client_username',
        'gp_client_email'
    ]
    
    # Get columns that exist in merged dataframe
    existing_columns = [col for col in priority_columns if col in merged_df.columns]
    
    # Add any remaining columns not in priority list
    remaining_columns = [col for col in merged_df.columns if col not in existing_columns]
    
    final_columns = existing_columns + remaining_columns
    
    organized_df = merged_df[final_columns]
    
    print(f"📊 Organized {len(final_columns)} columns into logical groups:")
    print("   1. Merge Key (amount)")
    print("   2. Vehicle Payment Identifiers (vp_*)")
    print("   3. GovPay Transaction Identifiers (gp_*)")
    print("   4. Financial Details")
    print("   5. Payment Information")
    print("   6. Client Details")
    
    return organized_df


def generate_summary_report(vehicle_df, govpay_df, merged_df):
    """
    Generate a comprehensive summary report of the merge operation.
    """
    print("\n" + "="*70)
    print(" GENERATING SUMMARY REPORT")
    print("="*70)
    
    # Calculate statistics
    total_vehicle_records = len(vehicle_df)
    total_govpay_records = len(govpay_df)
    total_merged_records = len(merged_df)
    
    # Count matched vs unmatched
    matched_records = merged_df['gp_record_id'].notna().sum()
    unmatched_records = merged_df['gp_record_id'].isna().sum()
    match_rate = (matched_records / total_vehicle_records) * 100 if total_vehicle_records > 0 else 0
    
    # Amount statistics
    unique_amounts_vehicle = vehicle_df['amount'].nunique()
    unique_amounts_govpay = govpay_df['amount'].nunique()
    
    report = f"""
{'='*80}
             VEHICLE-GOVPAY DATA MERGE SUMMARY REPORT
{'='*80}

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SOURCE FILES
{'='*80}
• GovPay Data: GOVPAY-SLRSA_Dec19-31_2025.csv
• Vehicle Payment Data: vehicle_payments_nra_filtered.csv

INPUT DATA STATISTICS
{'='*80}
• Total Vehicle Payment Records:    {total_vehicle_records:>10,}
• Total GovPay Records:             {total_govpay_records:>10,}
• Unique Amounts (Vehicle):         {unique_amounts_vehicle:>10,}
• Unique Amounts (GovPay):          {unique_amounts_govpay:>10,}

MERGE RESULTS
{'='*80}
• Total Merged Records:             {total_merged_records:>10,}
• Successfully Matched:             {matched_records:>10,}
• Unmatched Records:                {unmatched_records:>10,}
• Match Rate:                       {match_rate:>10.2f}%

MERGE DETAILS
{'='*80}
• Merge Type: LEFT JOIN
• Merge Key: amount (payment amount)
• Left Table: Vehicle Payment Data (retained all records)
• Right Table: GovPay Data (matched by amount)

COLUMN NAMING CONVENTION
{'='*80}
• vp_* : Vehicle Payment columns (e.g., vp_application_number)
• gp_* : GovPay transaction columns (e.g., gp_transaction_id)
• amount : Common merge key (shared between both datasets)

OUTPUT FILES
{'='*80}
• Merged Data: merged_vehicle_govpay_data.csv
• Summary Report: merge_summary_report.txt

NOTE
{'='*80}
The merge was performed using LEFT JOIN, meaning ALL vehicle payment 
records are preserved. Records with NULL values in gp_* columns indicate 
vehicle payments that could not be matched to a GovPay transaction 
based on the amount.

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


def save_merged_data(merged_df):
    """
    Save the merged DataFrame to CSV file.
    """
    print("\n" + "="*70)
    print(" SAVING MERGED DATA")
    print("="*70)
    
    # Save to CSV
    merged_df.to_csv(OUTPUT_FILE, index=False)
    
    print(f"\n💾 Merged data saved successfully!")
    print(f"   📁 File: {OUTPUT_FILE}")
    print(f"   📊 Total Records: {len(merged_df):,}")
    print(f"   📋 Total Columns: {len(merged_df.columns)}")


def main():
    """
    Main execution function for the Vehicle-GovPay data merge.
    """
    print("\n")
    print("█" * 70)
    print("█" + " " * 68 + "█")
    print("█" + "  VEHICLE PAYMENT & GOVPAY DATA INTEGRATION SYSTEM  ".center(68) + "█")
    print("█" + " " * 68 + "█")
    print("█" * 70)
    
    try:
        # Step 1: Load data
        govpay_df, vehicle_df = load_and_prepare_data()
        
        # Step 2: Rename columns professionally
        govpay_renamed, vehicle_renamed = rename_columns_professionally(govpay_df, vehicle_df)
        
        # Step 3: Perform left join merge
        merged_df = perform_left_join_merge(vehicle_renamed, govpay_renamed)
        
        # Step 4: Organize columns
        organized_df = organize_columns(merged_df)
        
        # Step 5: Save merged data
        save_merged_data(organized_df)
        
        # Step 6: Generate summary report
        generate_summary_report(vehicle_renamed, govpay_renamed, organized_df)
        
        print("\n" + "="*70)
        print(" ✅ DATA MERGE COMPLETED SUCCESSFULLY!")
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
