import pandas as pd
import os

def filter_transactions():
    # File paths
    input_file = '/Users/user/Desktop/Korlie_Data/TenkiPay/9th Apr_tenkipay-transactions_filtered.csv'
    output_file = '/Users/user/Desktop/Korlie_Data/TenkiPay/09th_Apr_tenkipay-transactions_filtered_wango.csv'

    print(f"Loading data from: {input_file}...")    
    
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        # Display initial stats
        print(f"Total transactions in file: {len(df)}")
        
        # Filter by transaction type (paidto column)
        # Based on the dataset analysis, 'WANGOV' represents the primary Paidto merchant type
        filtered_df = df[df['paidto'] == 'WANGOV']
        
        # Save the filtered output
        filtered_df.to_csv(output_file, index=False)
        
        print("-" * 50)
        print(f"✅ SUCCESS: Filtering complete.")
        print(f"📊 Filtered Records (WANGOV): {len(filtered_df)}")
        print(f"💾 Saved to: {output_file}")
        print("-" * 50)
        
    except FileNotFoundError:
        print(f"❌ Error: The file {input_file} was not found.")
    except Exception as e:
        print(f"❌ An error occurred: {e}")

if __name__ == "__main__":
    filter_transactions()
