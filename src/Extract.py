import pandas as pd


def extract_csv_via_pandas(file_name, column_names: list):
    df = pd.read_csv(file_name, names=column_names)
    return df


column = ["DateTime", "Location", "Customer",
          "Order", "Payment_method", "Final_price", "PII"]
extract_csv_via_pandas('2021-02-23-isle-of-wight.csv', column)
