import pandas as pd
from Extract import extract_csv_via_pandas


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

df = pd.read_csv("2021-02-23-isle-of-wight.csv", names=column_names)


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
            indexes[place] = "Regular"

    x = ",".join(indexes)  # rejoin modified orders
    new_orders.append(x)

# Order ID keeps track of every ID each customer makes per basket item
order_ID = []
size = []
product_name = []
product_price = []

# Splits new orders and separates info into Size, Product Name, and Product price
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

# Get Unique ID out of entire list of order ID's
unique_order_ID = []
for item in order_ID:
    if item not in unique_order_ID:
        unique_order_ID.append(item)


# Creating a list of tuples containing Orders' Id, size of product, product name, product price that was ordered
zipped_values_orders = zip(order_ID, size, product_name, product_price)
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

# Get Unique Products (Size, Product, Price)
unique_product_info = []
for item in zipped_list_products:
    if item not in unique_product_info:
        unique_product_info.append(item)

# Get Unique Credit/Debit Cards
unique_cards = []
for place, item in enumerate(card_used):
    if item == "None":
        item = "Cash"
    if item not in unique_cards:
        unique_cards.append(item)

# Get ID for Unique cards
unique_cards_ID = []
for i in range(1, len(unique_cards)+1):
    unique_cards_ID.append(i)

# Gets unique Payment method
unique_payment_method = []
for item in original_payment_method:
    if item not in unique_payment_method:
        unique_payment_method.append(item)

##################### USE PostgreSQL to make order-products table #####################################

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


# print(order_product_df.head(10))


################################ Making Order df ######################################################

order_table_df = pd.DataFrame()
order_table_df["Date"] = original_date
order_table_df["Time"] = original_time
order_table_df["Location"] = original_location
# order_table_df["Order"] = unique_order_ID
order_table_df["Total_price"] = original_total_price

print(order_table_df)
################################# Making Products df ###############################################

product_df = pd.DataFrame()
product_df["Product_size"] = unzipped_size
product_df["Product_name"] = unzipped_name
product_df["Product_price"] = unzipped_price
cleaned_products_df = product_df.drop_duplicates(
    subset=["Product_name", "Product_size", "Product_price"], ignore_index=True)  # Removes all duplicates
# cleaned_products_df["product_ID"] = range(1, (len(unique_product_info)+1)) #Agreed to ignore adding indexes, SQL will AutoIncrement the entry

print(cleaned_products_df)
####################################### Making payments df ############################################

# Team Decision in Progress:
# Make a column containing specific cards used for payment or\
# leave payment as either Cash or Card

unique_payment_method = []
for item in original_payment_method:
    if item not in unique_payment_method:
        unique_payment_method.append(item)


payments_df = pd.DataFrame()
#payments_df["Payment Type"] = original_payment_method
# payments_df["Card_ID"] = unique_cards_ID #Agreed to ignore adding indexes, SQL will AutoIncrement the entry
payments_df["Payment Type"] = unique_payment_method

print(payments_df)


############################################## MAKING OF CARD TYPES df ################################################################

unique_card_used = []
for item in card_used:
    if item not in unique_card_used:
        unique_card_used.append(item)

card_type_df = pd.DataFrame()
card_type_df["Types_of_Card"] = unique_card_used

print(card_type_df)
#####################################################################################################################################
