import pandas as pd

INPUT_FILE = '/Users/user/Desktop/Korlie_Data/Vechile_renewal/vehicle_payments_nra_filtered.csv'

df = pd.read_csv(INPUT_FILE)

print(f"Total data points: {len(df)}")
