import pandas as pd

# Load the CSV file
df = pd.read_csv("/Users/user/Desktop/Korlie_Data/GovPay/GOVPAY-SLRSA.csv")
#print(df.head())

# Save to Excel
#df.to_excel("1GOVPAY-SLRSA.xlsx", index=False)

#print(f"CSV data has been converted to Excel: 1GOVPAY-SLRSA.xlsx")

# Convert 'Date' column to datetime
df['Date'] = pd.to_datetime(df['Date'])