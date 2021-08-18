import pandas as pd


def extract_csv(file_name):
    df = pd.read_csv(file_name)
    print(df)
    return df

extract_csv('2021-02-23-isle-of-wight.csv')
