from src.Extract import query_db_for_location_tuples, query_db_for_product_tuples, query_latest_entries
from src.transform import get_location_ID, transform_data, zipped_items_into_list
from src.transform import new_items_to_load, new_locations_to_load
from src.transform import splitDataFrameList, create_customer_id,get_customer_id, zip_from_df_orders, zip_from_basket_df
from src.Load import load_data_redshift
import pandas as pd
import io
import boto3
import psycopg2


def etl(df: object):
    
    new_elongated_raw_df = splitDataFrameList(df,"Order", ",")

    df[["date", "time"]] = df.Datetime.str.split(" ", expand=True,) #Split date and time

    long_raw_customers = new_elongated_raw_df["Customer"] # Combine this with transformed products for Basket df (clean with get/create customer id)

    # Use to make below to make Orders df
    raw_dates = df["date"]
    raw_times = df["time"]
    raw_locations = df["Location"] # Clean This with get location
    raw_customers = df["Customer"] # Clean This with get/create customer id
    raw_total_price = df["Price"]
    raw_payment = df["Payment"]

    # Use transform function on this
    raw_products = df["Order"]
    

    # CLEAN LOCATIONS:
    csv_locations = [i for i in raw_locations]
    database_location_id_tuple = query_db_for_location_tuples()
    new_locations_for_db = new_locations_to_load(database_location_id_tuple, csv_locations)
    cleaned_locations_for_db = zipped_items_into_list(new_locations_for_db)
    load_data_redshift("locations", "location_name", "%s", cleaned_locations_for_db)
    database_location_id_tuple = query_db_for_location_tuples()
    dict_database_id = dict(database_location_id_tuple)
    database_id_keys = list(dict_database_id.keys()) # Create a list of keys
    database_id_values = list(dict_database_id.values()) # Create a list of values
    location_id = get_location_ID(raw_locations, database_id_keys, database_id_values)

    # CLEAN PRODUCTS:
    database_products_id_tuple = query_db_for_product_tuples()
    current_zipped_products_from_csv = transform_data(raw_products)
    all_current_cleaned_products_from_csv = zipped_items_into_list(current_zipped_products_from_csv)
    new_products_for_db = new_items_to_load(database_products_id_tuple, all_current_cleaned_products_from_csv) # compare db products and csv products
    load_data_redshift("products", "product_size,product_name,product_price", "%s,%s,%s", new_products_for_db)

    #Use below to make database_products_df
    database_products_id_tuple = query_db_for_product_tuples()
    db_cleaned_product_id = [item[0] for item in database_products_id_tuple]
    db_cleaned_product_size = [item[1] for item in database_products_id_tuple]
    db_cleaned_product_name = [item[2] for item in database_products_id_tuple]
    db_cleaned_product_price = [item[3] for item in database_products_id_tuple]

    # CLEAN CUSTOMERS:
    zipped_long_customer_id = create_customer_id(long_raw_customers)
    long_customer_id_tuple = zipped_items_into_list(zipped_long_customer_id)
    dict_customer_id = dict(long_customer_id_tuple)
    customer_id_keys = list(dict_customer_id.keys()) # Creates a list of keys
    customer_id_values = list(dict_customer_id.values()) # Creates a list of values
    long_customer_id = get_customer_id(long_raw_customers, customer_id_keys, customer_id_values)
    customer_id = get_customer_id(raw_customers, customer_id_keys, customer_id_values)

    # CLEANED ORDERS:
    orders_df = pd.DataFrame()
    orders_df["date"] = raw_dates
    orders_df["time"] = raw_times
    orders_df["location_id"] = location_id
    orders_df["customer_id"] = customer_id
    orders_df["price"] = raw_total_price
    orders_df["payment"] = raw_payment
    zipped_orders = zip_from_df_orders(orders_df)
    cleaned_orders = zipped_items_into_list(zipped_orders)
    load_data_redshift("orders","date,time,location_id,customer_id,price,payment", "%s,%s,%s,%s,%s,%s", cleaned_orders)

    # CLEANED BASKET:
    temp_basket_df = pd.DataFrame()
    raw_size, raw_name, raw_price = zip(*all_current_cleaned_products_from_csv)
    temp_basket_df["customer_id"] = long_customer_id
    temp_basket_df["product_size"] = raw_size
    temp_basket_df["product_name"] = raw_name
    temp_basket_df["product_price"] = raw_price

    products_df = pd.DataFrame()
    products_df["product_id"] = db_cleaned_product_id
    products_df["product_size"] = db_cleaned_product_size
    products_df["product_name"] = db_cleaned_product_name
    products_df["product_price"] = db_cleaned_product_price

    temp_basket_df = temp_basket_df.merge(products_df, on= ["product_size","product_name","product_price"], how="left")

    basket_df = pd.DataFrame()
    basket_df["customer_id"] = temp_basket_df["customer_id"]
    basket_df["product_id"] = temp_basket_df["product_id"]

    # Get recent Orders
    recent_orders_from_db = query_latest_entries("*", len(orders_df),"orders","order_id")
    recent_orders_df = pd.DataFrame(recent_orders_from_db, columns=["order_id","date","time","location_id", "customer_id", "price", "payment"])

    basket_df = basket_df.merge(recent_orders_df, on = ["customer_id"], how="left")
    cleaned_basket_df = pd.DataFrame()
    cleaned_basket_df["order_id"] = basket_df["order_id"]
    cleaned_basket_df["customer_id"] = basket_df["customer_id"]
    cleaned_basket_df["product_id"] = basket_df["product_id"]
    zipped_basket = zip_from_basket_df(cleaned_basket_df)
    cleaned_basket = zipped_items_into_list(zipped_basket)
    load_data_redshift("basket", "order_id,customer_id,product_id", "%s,%s,%s", cleaned_basket)