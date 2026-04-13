import pandas as pd
from pathlib import Path

def update_csv_file(input_path: str, output_path: str) -> None:
    """
    Reads a CSV file, adds specific columns with default values, 
    and saves it to a new file without modifying existing data.
    """
    # Create Path objects for easier file manipulation
    input_file = Path(input_path)
    output_file = Path(output_path)
    
    # Make sure the input file actually exists before trying to read it
    if not input_file.exists():
        print(f"Error: The file {input_file} does not exist.")
        return
        
    try:
        print(f"Reading data from: {input_file}")
        # Read the existing CSV file into a pandas DataFrame.
        # A DataFrame is essentially a table of data, like an Excel sheet.
        # This reads all rows and columns exactly as they are.
        df = pd.read_csv(input_file)
        
        # Determine how many rows are currently in the data
        original_row_count = len(df)
        print(f"Successfully loaded {original_row_count} rows.")
        
        # Add the new columns with their required default values.
        # Pandas will automatically fill every existing row with these exact values.
        # Existing columns and rows are completely unaffected by this.
        print("Adding 'nameof_client', 'client_id', and 'chiefdom' columns...")
        df['nameof_client'] = 'BO CITY COUNCIL'
        df['client_id'] = 'BCC'
        df['chiefdom'] = 'Bo City'
        
        # Save the modified DataFrame to the new location.
        # index=False prevents pandas from adding an extra column with row numbers
        # at the far left of the CSV file.
        print(f"Saving the updated data to: {output_file}")
        df.to_csv(output_file, index=False)
        
        print("Process completed successfully! The new file has been created.")
        
    except pd.errors.EmptyDataError:
        print("Error: The input CSV file is empty.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Define the exact file paths as requested
    INPUT_CSV = '/Users/user/Desktop/Korlie_Data/bodc_properties.csv'
    OUTPUT_CSV = '/Users/user/Desktop/Korlie_Data/bodc_properties_updated.csv'
    
    # Run the update function
    update_csv_file(INPUT_CSV, OUTPUT_CSV)
