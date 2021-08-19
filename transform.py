import pandas as pd

# column_names = ["DateTime", "Location", "Customer_Name",
#                 "Order", "Payment_Method", "Total_Price", "PII"]
# df = pd.read_csv("2021-02-23-isle-of-wight.csv", names=column_names)


# for value in df["PII"]:
#     if value == "None":
#         value += ","

# df[['Payment_type', 'Account_number']
#    ] = df.PII.str.split(",", expand=True,)  # Split payment into type and account number

# for value in df["PII"]:
#     value = value[0:-1]

# df[['Date', 'Timestamp']  # Split DateTime into Date and Timestamp
#    ] = df.DateTime.str.split(" ", expand=True,)

# locations = df["Location"]  # Stores column of location
# orders = df["Order"]  # stores column of orders
# total_prices = df["Total_Price"]  # store total prices
# to_drop = ["Customer_Name", "DateTime",
#            "Account_number"]  # Data to sanitize

# # inplace whether the returned data is copied(False) or updated(True)
# delete_data = df.drop(columns=to_drop, inplace=False)


#########################
# Quick test to insert values into empty csv spaces

# orders = df["Order"]

# new_orders = []

# indexes = []
# for order in orders:
#     # print(order)
#     indexes.append(order.split(","))


# # print(indexes)

# for place, item in enumerate(indexes):
#     if item == "":
#         indexes[place] = "Regular"
# new_orders.append(indexes)
# # print(new_orders)


# print(f"new {indexes}")

# print(len(indexes))

# print(new_orders)
# product_dict = {"Product_ID" : range(1, len(new_orders[0]), "Product_name" : new_order)}
######################################################################
def isfloat(value):
    """This function checks the value to see if it can be casted as a float. It returns True if it is a float,
    and returns False if otherwise."""
    try:
        float(value)
        return True
    except ValueError:
        return False


column_names = ["Time", "Location", "Customer Name",
                "Order", "Payment_Method", "Total_Price", "PII"]

df = pd.read_csv("2021-02-23-isle-of-wight.csv", names=column_names)


df[['Date', 'Timestamp']  # Split DateTime into Date and Timestamp
   ] = df.Time.str.split(" ", expand=True,)

original_date = df["Date"]
original_time = df["Timestamp"]
original_location = df["Location"]
original_total_price = df["Total_Price"]
original_payment_method = df["Payment_Method"]

# order_table_df = pd.DataFrame()
# order_table_df["Date"] = original_date
# order_table_df["Time"] = original_time
# order_table_df["Location"] = original_location
# order_table_df["Order"] = unique_order_ID
# order_table_df["Total_price"] = original_total_price
# order_table_df["Payment_method"] = original_payment_method

# print(order_table_df)


original_orders = df["Order"]
new_orders = []
size = []
product_name = []
product_price = []

for order in original_orders:
    indexes = order.split(",")  # Splits original order

    # Loops through list to find empty spaces and fill them with string "Regular"
    for place, item in enumerate(indexes):
        if item == "":
            indexes[place] = "Regular"

    x = ",".join(indexes)  # rejoin modified orders
    new_orders.append(x)

order_ID = []
unique_order_ID = []

for i in range(len(new_orders)):
    words = new_orders[i].split(",")
    for word in words:
        if word == "Regular" or word == "Large":
            size.append(word)
            # Keeps track of each new basket item in the order
            order_ID.append(i+1)

        elif isfloat(word) == True:
            product_price.append(float(word))

        else:
            product_name.append(word)

for item in order_ID:
    if item not in unique_order_ID:
        unique_order_ID.append(item)

zipped_values_orders = zip(order_ID, size, product_name, product_price)
zipped_list_orders = list(zipped_values_orders)

unique_orders = []

# for products table
for item in zipped_list_orders:
    if item not in unique_orders:
        unique_orders.append(item)

# making product df
unzipped_order_ID, unzipped_size, unzipped_name, unzipped_price = zip(
    *unique_orders)
product_df = pd.DataFrame()
product_df["product_size"] = unzipped_size
product_df["product_name"] = unzipped_name
product_df["product_price"] = unzipped_price

# dropped_product_df = product_df.drop_duplicates(
#     subset=["product_name", "product_size", "product_price"])
# dropped_product_df["product_ID"] = range(
#     1, (len(dropped_product_df["product_name"]) + 1))  # Error at 147

# for order_product table
# all_product_ID_in_order = []

# for i in range(len(zipped_list_orders)):
#     for j in range(len(product_df["product_name"])):
#         temp_list = [product_df["product_size"][j],
#                      product_df["product_name"][j], product_df["product_price"][j]]
#         if temp_list == zipped_list_orders[i]:
#             all_product_ID_in_order.append(product_df["product_ID"][j])

# # making order_product_df
# order_product_df = pd.DataFrame()
# order_product_df["order_ID"] = order_ID
# order_product_df["product_ID"] = all_product_ID_in_order

# Making Order Table
order_table_df = pd.DataFrame()
order_table_df["Date"] = original_date
order_table_df["Time"] = original_time
order_table_df["Location"] = original_location
order_table_df["Order"] = unique_order_ID
order_table_df["Total_price"] = original_total_price
order_table_df["Payment_method"] = original_payment_method

print(order_table_df)


# print(order_product_df.head(10))

# making orders df
# order id has 1552 items
# date has 540
# time has 540
# location 540
# Total spend 540


# making payments df
# payment id
# payment type

# print(order_product_df)
# print(product_df)

# order_ID, size, product_name, product_price = list(zip(*zipped_list_orders))

# zipped_values_products = zip(size, product_name, product_price)
# zipped_list_products = list(zipped_values_products)

# product_info = []
# for item in zipped_list_products:
#     if item not in product_info:
#         product_info.append(item)


# print(f"This is the length of the order ID list: {len(order_ID)}")
# print(f"This is the length of the size list: {len(size)}")
# print(f"This is the length of the product name list: {len(product_name)}")
# print(f"This is the length of the product price list: {len(product_price)}")
# print(
#     f"This is the length of the unique orders (This includes order_id, size, name, price): {len(unique_orders)}")
# print(
#     f"This is the length of the product_info (This includes size, name, price): {len(product_info)}")
# print(product_info)
# print(unique_orders)

# for item in unique_orders:
#     print(item)

# original order ["", "Tea", "2.50", ",", "Coffee", "3.0", "Large", "Smoothie", "1.50"]
# new order ["Regular", "Tea", "2.50", "Regular", "Coffee", "3.0", "Large", "Smoothie", "1.50"]
