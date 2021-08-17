import csv
import pandas as pd

column_names = ["DateTime", "Location", "Customer_Name",
                "Order", "Payment_Method", "Total_Price", "PII"]
df = pd.read_csv("2021-02-23-isle-of-wight.csv", names=column_names)


for value in df["PII"]:
    if value == "None":
        value += ","

df[['Payment_type', 'Account_number']
   ] = df.PII.str.split(",", expand=True,)  # Split payment into type and account number

for value in df["PII"]:
    value = value[0:-1]

df[['Date', 'Timestamp']  # Split DateTime into Date and Timestamp
   ] = df.DateTime.str.split(" ", expand=True,)

locations = df["Location"]  # Stores column of location
orders = df["Order"]  # stores column of orders
total_prices = df["Total_Price"]  # store total prices
to_drop = ["Customer_Name", "PII", "DateTime",
           "Account_number"]  # Data to sanitize


# inplace whether the returned data is copied(False) or updated(True)
delete_data = df.drop(columns=to_drop, inplace=False)
print(delete_data)
