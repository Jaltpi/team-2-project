import csv
import pandas as pd


def extract_csv(file_name):
    df = pd.read_csv(file_name)
    print(df)
    return df


# def read_csv_file(file_name, csv_to_read):
#     with open(file_name, 'r') as csv_file:
#         csv_to_read = csv.DictReader(csv_file)
#         csv_list = []
#         for row in csv_to_read:
#             csv_list.append(row)
#         return csv_list

extract_csv('2021-02-23-isle-of-wight.csv')
