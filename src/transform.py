import pandas as pd
import numpy as np
from config import config
from Load import create_connection, SQL_commands_executor, SQL_INSERT_STATEMENT_FROM_DATAFRAME
import psycopg2
from sqlalchemy import create_engine


def isfloat(value):
    """This function checks the value to see if it can be casted as a float. It returns True if it is a float,
    and returns False if otherwise."""
    try:
        float(value)
        return True
    except ValueError:
        return False


def get_unique_items(List: list):
    """This function takes in a list, and gets the unique items from the inputted list, then returns
    the unique items in the form of a list."""
    temporary_list = []
    for item in List:
        if item not in temporary_list:
            temporary_list.append(item)

    return temporary_list


column_names = ["DateTime", "Location", "Customer Name",
                "Order", "Payment_Method", "Total_Price", "PII"]

df = pd.read_csv("2021-02-23-isle-of-wight.csv",
                 names=column_names)


df[['Date', 'Time']  # Split DateTime into Date and Timestamp
   ] = df.DateTime.str.split(" ", expand=True,)

df[['Payment_type', 'Account_number']

   ] = df.PII.str.split(",", expand=True,)  # Split payment info into card names and card numbers

to_drop = ["Customer_Name", "PII"]  # Data to sanitize

card_used = df["Payment_type"]  # Debit/Credit card names
original_date = df["Date"]
original_time = df["Time"]
original_location = df["Location"]
original_total_price = df["Total_Price"]  # Final Price
original_payment_method = df["Payment_Method"]  # options: Cash or Card
original_orders = df["Order"]

new_orders = []
for order in original_orders:
    indexes = order.split(",")  # Splits original order

    # Loops through list to find empty spaces and fill them with string "Regular"
    for place, item in enumerate(indexes):
        if item == "":
            indexes[place] = "None"

    x = ",".join(indexes)  # rejoin modified orders
    new_orders.append(x)

# Order ID keeps track of every ID each customer makes per basket item
Customer_ID = []
size = []
product_name = []
product_price = []

# Splits new orders and separates info into Size, Product Name, and Product price
for i in range(len(new_orders)):
    words = new_orders[i].split(",")
    for word in words:
        if word == "Regular" or word == "Large" or word == "None":
            size.append(word)
            # Keeps track of each new basket item in the order
            Customer_ID.append(i+1)

        elif isfloat(word) == True:
            product_price.append(float(word))

        else:
            product_name.append(word)

# Get Unique ID out of entire list of order ID's
unique_order_ID = []
for item in Customer_ID:
    if item not in unique_order_ID:
        unique_order_ID.append(item)


# Creating a list of tuples containing Orders' Id, size of product, product name, product price that was ordered
zipped_values_orders = zip(Customer_ID, size, product_name, product_price)
zipped_list_orders = list(zipped_values_orders)

# Gets Unique orders out of list containing all orders
unique_orders = []
for item in zipped_list_orders:
    if item not in unique_orders:
        unique_orders.append(item)


# Creating a list of tuples containing all product's size, name, price that was ordered
unzipped_order_ID, unzipped_size, unzipped_name, unzipped_price = zip(
    *unique_orders)
zipped_values_products = zip(size, product_name, product_price)
zipped_list_products = list(zipped_values_products)


# Get Unique Credit/Debit Cards
unique_cards = []
for place, item in enumerate(card_used):
    if item not in unique_cards:
        unique_cards.append(item)

# Get ID for Unique cards (Visa, express, etc)
unique_cards_ID = []
for i in range(1, len(unique_cards)+1):
    unique_cards_ID.append(i)

# Gets unique Payment method Cash/card
unique_payment_method = []
for item in original_payment_method:
    if item not in unique_payment_method:
        unique_payment_method.append(item)


################################ Making Order df ######################################################################################################

# Function
def order_table():
    order_table_df = pd.DataFrame()
    order_table_df["Date"] = original_date
    order_table_df["Time"] = original_time
    order_table_df["Location"] = original_location
    # order_table_df["Order"] = unique_order_ID
    order_table_df["Total_price"] = original_total_price
    order_table_df["payment_type"] = df["Payment_Method"]

    return(order_table_df)

################################# Making Products df ####################################################################################################

# Function


def product_table():
    product_df = pd.DataFrame()
    product_df["product_name"] = unzipped_name
    product_df["product_size"] = unzipped_size
    product_df["product_price"] = unzipped_price
    cleaned_products_df = product_df.drop_duplicates(
        subset=["product_name", "product_size", "product_price"], ignore_index=True)
    cleaned_products_df["product_ID"] = range(
        1, len(cleaned_products_df["product_size"]) + 1)
    # Removes all duplicates
    # Agreed to ignore adding indexes, SQL will AutoIncrement the entry
    return(cleaned_products_df)

# making order_product table with quantity

################################# Making Order Products df ####################################################################################################

# Function


def order_product_table():

    temp_df = pd.DataFrame()
    temp_df["Customer_ID"] = Customer_ID
    temp_df["product_size"] = size
    temp_df["product_name"] = product_name
    temp_df["product_price"] = product_price

    # PRODUCT IDS ARE FLOATS AND WEIRD WARNING MESSAGE WHEN ASSIGNING PRODUCT IDS

    product_df = product_table()
    temp_df = temp_df.merge(product_df, on=["product_size",
                                            "product_name", "product_price"], how="left")
    temp_df = temp_df.groupby(
        ["Customer_ID", "product_ID"], axis=0).size().reset_index(name='quantity')
    order_product_df = temp_df.drop_duplicates()
    return order_product_df

#############################################################################################################################################


# Environment Variables
db = config()
host = db["host"]
user = db["user"]
password = db["password"]
database = db["database"]

# Use SQLAlchemy to create an engine (connection for Database)
engine = create_connection(system="postgresql", user_name=user, password=password, host=host,
                           port="5432", database_name=database)


cleaned_product_df = product_table()
order_table_df = order_table()
order_product_df = order_product_table()


products_insert_commands = SQL_INSERT_STATEMENT_FROM_DATAFRAME(
    cleaned_product_df, "products")
orders_insert_commands = SQL_INSERT_STATEMENT_FROM_DATAFRAME(
    order_table_df, "orders")
order_product_insert_commands = SQL_INSERT_STATEMENT_FROM_DATAFRAME(
    order_product_df, "order_product")


SQL_commands_executor(products_insert_commands)
SQL_commands_executor(orders_insert_commands)
SQL_commands_executor(order_product_insert_commands)
