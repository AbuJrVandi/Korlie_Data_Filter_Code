import pandas as pd

INPUT_FILE = '/Users/user/Desktop/Korlie_Data/Vechile_renewal/clean_paid_vehicle_renewal_payments.csv'

df = pd.read_csv(INPUT_FILE)
unique_amounts = df['amount'].nunique()

print(f"Total unique amounts: {unique_amounts}")

