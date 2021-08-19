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


# for row in df["Orders"]:


# print(unique_products)


# inplace whether the returned data is copied(False) or updated(True)
delete_data = df.drop(columns=to_drop, inplace=False)
# print(delete_data.head())


#########################
# Quick test to insert values into empty csv spaces

orders = df["Order"]

print(orders)
new_orders = []

indexes = []
for order in orders:
    # print(order)
    indexes.append(order.split(","))


print(indexes)

for place, item in enumerate(indexes):
    if item == "":
        indexes[place] = "Regular"
new_orders.append(indexes)
print(new_orders)


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
                "Order", "Payment_Method", "Total Price", "PII"]

df = pd.read_csv("2021-02-23-isle-of-wight.csv", names=column_names)

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
for i in range(len(new_orders)):
    words = new_orders[i].split(",")
    for word in words:
        if word == "Regular" or word == "Large":
            size.append(word)
            order_ID.append(i+1)

        elif isfloat(word) == True:
            product_price.append(word)

        else:
            product_name.append(word)


zipped_values_orders = zip(order_ID, size, product_name, product_price)
zipped_list_orders = list(zipped_values_orders)

unique_orders = []

for item in zipped_list_orders:
    if item not in unique_orders:
        unique_orders.append(item)

product_ID = []
for item in zipped_list_orders:
    if item in unique_orders

order_ID, size, product_name, product_price = list(zip(*zipped_list_orders))

zipped_values_products = zip(size, product_name, product_price)
zipped_list_products = list(zipped_values_products)

product_info = []
for item in zipped_list_products:
    if item not in product_info:
        product_info.append(item)


print(f"This is the length of the order ID list: {len(order_ID)}")
print(f"This is the length of the size list: {len(size)}")
print(f"This is the length of the product name list: {len(product_name)}")
print(f"This is the length of the product price list: {len(product_price)}")
print(
    f"This is the length of the unique orders (This includes order_id, size, name, price): {len(unique_orders)}")
print(
    f"This is the length of the product_info (This includes size, name, price): {len(product_info)}")
# print(product_info)
# print(unique_orders)

# for item in unique_orders:
#     print(item)

# original order ["", "Tea", "2.50", ",", "Coffee", "3.0", "Large", "Smoothie", "1.50"]
# new order ["Regular", "Tea", "2.50", "Regular", "Coffee", "3.0", "Large", "Smoothie", "1.50"]
