import pandas as pd
# import numpy as np
# from config import config
# from Load import create_connection, SQL_commands_executor, SQL_INSERT_STATEMENT_FROM_DATAFRAME
# import psycopg2
# from sqlalchemy import create_engine


# def isfloat(value):
#     """This function checks the value to see if it can be casted as a float. It returns True if it is a float,
#     and returns False if otherwise."""
#     try:
#         float(value)
#         return True
#     except ValueError:
#         return False


# def get_unique_items(List: list):
#     """This function takes in a list, and gets the unique items from the inputted list, then returns
#     the unique items in the form of a list."""
#     temporary_list = []
#     for item in List:
#         if item not in temporary_list:
#             temporary_list.append(item)

#     return temporary_list


# column_names = ["DateTime", "Location", "Customer Name",
#                 "Order", "Payment_Method", "Total_Price", "PII"]

# df = pd.read_csv("2021-02-23-isle-of-wight.csv",
#                  names=column_names)


# df[['Date', 'Time']  # Split DateTime into Date and Timestamp
#    ] = df.DateTime.str.split(" ", expand=True,)

# df[['Payment_type', 'Account_number']

#    ] = df.PII.str.split(",", expand=True,)  # Split payment info into card names and card numbers

# to_drop = ["Customer_Name", "PII"]  # Data to sanitize

# card_used = df["Payment_type"]  # Debit/Credit card names
# original_date = df["Date"]
# original_time = df["Time"]
# original_location = df["Location"]
# original_total_price = df["Total_Price"]  # Final Price
# original_payment_method = df["Payment_Method"]  # options: Cash or Card
# original_orders = df["Order"]

# new_orders = []
# for order in original_orders:
#     indexes = order.split(",")  # Splits original order

#     # Loops through list to find empty spaces and fill them with string "Regular"
#     for place, item in enumerate(indexes):
#         if item == "":
#             indexes[place] = "None"

#     x = ",".join(indexes)  # rejoin modified orders
#     new_orders.append(x)

# # Order ID keeps track of every ID each customer makes per basket item
# Customer_ID = []
# size = []
# product_name = []
# product_price = []

# # Splits new orders and separates info into Size, Product Name, and Product price
# for i in range(len(new_orders)):
#     words = new_orders[i].split(",")
#     for word in words:
#         if word == "Regular" or word == "Large" or word == "None":
#             size.append(word)
#             # Keeps track of each new basket item in the order
#             Customer_ID.append(i+1)

#         elif isfloat(word) == True:
#             product_price.append(float(word))

#         else:
#             product_name.append(word)

# # Get Unique ID out of entire list of order ID's
# unique_order_ID = []
# for item in Customer_ID:
#     if item not in unique_order_ID:
#         unique_order_ID.append(item)


# # Creating a list of tuples containing Orders' Id, size of product, product name, product price that was ordered
# zipped_values_orders = zip(Customer_ID, size, product_name, product_price)
# zipped_list_orders = list(zipped_values_orders)

# # Gets Unique orders out of list containing all orders
# unique_orders = []
# for item in zipped_list_orders:
#     if item not in unique_orders:
#         unique_orders.append(item)


# # Creating a list of tuples containing all product's size, name, price that was ordered
# unzipped_order_ID, unzipped_size, unzipped_name, unzipped_price = zip(
#     *unique_orders)
# zipped_values_products = zip(size, product_name, product_price)
# zipped_list_products = list(zipped_values_products)


# # Get Unique Credit/Debit Cards
# unique_cards = []
# for place, item in enumerate(card_used):
#     if item not in unique_cards:
#         unique_cards.append(item)

# # Get ID for Unique cards (Visa, express, etc)
# unique_cards_ID = []
# for i in range(1, len(unique_cards)+1):
#     unique_cards_ID.append(i)

# # Gets unique Payment method Cash/card
# unique_payment_method = []
# for item in original_payment_method:
#     if item not in unique_payment_method:
#         unique_payment_method.append(item)


# ################################ Making Order df ######################################################################################################

# # Function
# def order_table():
#     order_table_df = pd.DataFrame()
#     order_table_df["Date"] = original_date
#     order_table_df["Time"] = original_time
#     order_table_df["Location"] = original_location
#     # order_table_df["Order"] = unique_order_ID
#     order_table_df["Total_price"] = original_total_price
#     order_table_df["payment_type"] = df["Payment_Method"]

#     return(order_table_df)

# ################################# Making Products df ####################################################################################################

# # Function


# def product_table():
#     product_df = pd.DataFrame()
#     product_df["product_name"] = unzipped_name
#     product_df["product_size"] = unzipped_size
#     product_df["product_price"] = unzipped_price
#     cleaned_products_df = product_df.drop_duplicates(
#         subset=["product_name", "product_size", "product_price"], ignore_index=True)
#     cleaned_products_df["product_ID"] = range(
#         1, len(cleaned_products_df["product_size"]) + 1)
#     # Removes all duplicates
#     # Agreed to ignore adding indexes, SQL will AutoIncrement the entry
#     return(cleaned_products_df)

# # making order_product table with quantity

# ################################# Making Order Products df ####################################################################################################

# # Function


# def order_product_table():

#     temp_df = pd.DataFrame()
#     temp_df["Customer_ID"] = Customer_ID
#     temp_df["product_size"] = size
#     temp_df["product_name"] = product_name
#     temp_df["product_price"] = product_price

#     # PRODUCT IDS ARE FLOATS AND WEIRD WARNING MESSAGE WHEN ASSIGNING PRODUCT IDS

#     product_df = product_table()
#     temp_df = temp_df.merge(product_df, on=["product_size",
#                                             "product_name", "product_price"], how="left")
#     temp_df = temp_df.groupby(
#         ["Customer_ID", "product_ID"], axis=0).size().reset_index(name='quantity')
#     order_product_df = temp_df.drop_duplicates()
#     return order_product_df

# #############################################################################################################################################


# # Environment Variables
# db = config()
# host = db["host"]
# user = db["user"]
# password = db["password"]
# database = db["database"]

# # Use SQLAlchemy to create an engine (connection for Database)
# engine = create_connection(system="postgresql", user_name=user, password=password, host=host,
#                            port="5432", database_name=database)


# cleaned_product_df = product_table()
# order_table_df = order_table()
# order_product_df = order_product_table()


# products_insert_commands = SQL_INSERT_STATEMENT_FROM_DATAFRAME(
#     cleaned_product_df, "products")
# orders_insert_commands = SQL_INSERT_STATEMENT_FROM_DATAFRAME(
#     order_table_df, "orders")
# order_product_insert_commands = SQL_INSERT_STATEMENT_FROM_DATAFRAME(
#     order_product_df, "order_product")


# SQL_commands_executor(products_insert_commands)
# SQL_commands_executor(orders_insert_commands)
# SQL_commands_executor(order_product_insert_commands)

def get_location_ID(raw_locations, location_keys: list, location_values: list):
    """This function takes in a Series from pandas, a list of keys from a dictionary,
    and a list of values. It loops through the series getting the ID number associated with the item
    returning a list of ID numbers"""
    location_ID = []

    for item in raw_locations:
        position = location_values.index(item) # Get index of item
        id_number = location_keys[position] # Get key value of item
        location_ID.append(id_number)
    return location_ID

def zipped_items_into_list(zipped_items: tuple):
    """This function loops through a an iterator of zipped values and returns the items in the form of a list."""
    temp = [item for item in zipped_items]
    return temp

def transform_data(data: list):
    """"This function takes in a list of orders, and separates the elements into three categories
    (size, name, price). Once seperated, the function returns a zipped list."""
    
    product_size = []
    product_price = []
    product_name = []

    for info in data:
        words = info.split(",") # Split list of all orders by commas

        for word in words:
            orders = word.lstrip().split(",") # Splits list of individual orders and remove white spaces
            
            for order in orders:
                item = order.split(" ") # Split individual order by spaces
                size = item[0] # Get product size
                product = " ".join(item[1:-2]) # Join product name + flavour
                price = item[-1] # Get product price
                product_size.append(size)
                product_name.append(product)
                product_price.append(float(price))
    
    return zip(product_size, product_name, product_price)

def new_products_to_load(db_product_tuples: list, cleaned_products: list):
    """This Function takes in two list of tuples, and returns a list of tuples containing the different items."""
    current_db_products = []
    new_products = []
    for item in db_product_tuples:
        product_list = list(item)
        removed_id_from_product = tuple(product_list[1:]) 
        current_db_products.append(removed_id_from_product)
    
    for item in cleaned_products:
        if item not in current_db_products:
            new_products.append(item)
    return list(set(new_products))

def get_order_tuples(raw_df : object, cleaned_location_id_df: object) -> list:
    """This function takes in a df, splits date time into separate columns, and and returns a list of tuples
    containing date, time, location, total price, and payment type."""
    
    raw_df[['date', 'time']] = raw_df.datetime.str.split(" ", expand=True,) # Split datetime to date and time
    
    zipped_order_tuples = zip(raw_df["date"].to_list(), raw_df["time"].to_list(), cleaned_location_id_df["location"].to_list(), \
        raw_df["total_price"].to_list(), raw_df["payment_type"].to_list()) # Creates a list of tuples
    
    return zipped_order_tuples

def create_basket_tuples(product_with_ids : list, order_with_ids : list, raw_products : list) -> list:
    """This function takes in 2 list containing tuples and a Pandas dataframe. It unpacks the tuples, creates a new dataframe,
    and returns zipped list from the merged dataframe"""
    product_ids, product_sizes, product_names, product_prices = zip(*product_with_ids)
    order_ids, order_location, order_price, order_payment_type = zip(*order_with_ids)
    raw_product_tuples = transform_data(raw_products)
    raw_product_sizes, raw_product_names, raw_product_prices = zip(*raw_product_tuples)
    temp_df = pd.DataFrame()
    temp_df["Customer_ID"] = order_ids
    temp_df["product_size"] = raw_product_sizes
    temp_df["product_name"] = raw_product_names
    temp_df["product_price"] = raw_product_prices
    product_df = pd.DataFrame()
    product_df["product_ids"] = product_ids
    product_df["product_sizes"] = product_sizes
    product_df["product_names"] = product_names
    product_df["product_prices"] = product_prices
    temp_df = temp_df.merge(product_df, on=["product_sizes",
                                            "product_names", "product_prices"], how="left")
    temp_df = temp_df.groupby(
        ["Customer_ID", "product_ids"], axis=0).size().reset_index(name='quantity')
    temp_df = temp_df.drop_duplicates()
    basket_Customer_ID = list(temp_df["Customer_ID"])
    basket_product_ID = list(temp_df["product_ids"])
    basket_quantity= list(temp_df["quantity"])
    return zip(basket_Customer_ID, basket_product_ID, basket_quantity)

def splitDataFrameList(df,target_column,separator):
    ''' df = dataframe to split,
    target_column = the column containing the values to split
    separator = the symbol used to perform the split
    returns: a dataframe with each entry for the target column separated, with each element moved into a new row. 
    The values in the other columns are duplicated across the newly divided rows.
    '''
    def splitListToRows(row,row_accumulator,target_column,separator):
        split_row = row[target_column].split(separator)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)
    new_rows = []
    df.apply(splitListToRows,axis=1,args = (new_rows,target_column,separator))
    new_df = pd.DataFrame(new_rows)
    return new_df

def create_customer_id(raw_customers: object):
    """This function Takes in a series of customer names, adds them to a list and counts how many new names
    were added (ID of name). It returns a zipped list of tuples for ID and names"""
    names = []
    id = []
    
    count = 0
    for customer in raw_customers:
        if customer not in names:
            count += 1
        elif customer in names:
            continue
        names.append(customer)
        id.append(count)
        
    return zip(id, names)

def get_customer_id(raw_customers: object, customer_id_keys: list, customer_id_values: list):
    """This function takes in a Series of customer, a list of Keys from a dictionary and a 
    list of values from the dictionary. It loops through the names replacing of the series finding the
    appropriate ID number for the name and appends that to a list. The function then returns the list of
    customer_id's"""
    
    customer_id = []
    
    for item in raw_customers:
        position = customer_id_values.index(item) # Get index of item
        id_number = customer_id_keys[position] # Get key value of item
        customer_id.append(id_number)
    return customer_id

def zip_from_df_orders(orders_df: object):
    "This function takes in the orders dataframe and returns a zipped list of tuples containing the data"
    zipped_orders = zip(orders_df["date"].to_list(),orders_df["time"].to_list(),orders_df["location_id"].to_list(),\
        orders_df["customer_id"].to_list(),orders_df["price"].to_list(),orders_df["payment"].to_list())

    return zipped_orders

def zip_from_basket_df(cleaned_basket_df: object):
    """This function take in the cleaned basket data frame and returns a zipped list of tuples containing the data"""
    zipped_basket = zip(cleaned_basket_df["order_id"].to_list(),cleaned_basket_df["customer_id"].to_list(),\
        cleaned_basket_df["product_id"].to_list())
    return zipped_basket
