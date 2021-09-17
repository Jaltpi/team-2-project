import pandas as pd

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
