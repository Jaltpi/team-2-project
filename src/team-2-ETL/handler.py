import json
from logging import exception
import pandas as pd
import io
import boto3
from src.Extract import query_db_for_location_tuples, query_db_for_product_tuples, query_latest_entries
from src.transform import get_location_ID, get_order_tuples, transform_data, zipped_items_into_list
from src.transform import new_products_to_load, create_basket_tuples
from src.transform import splitDataFrameList,get_customer_id,create_customer_id,zip_from_basket_df,zip_from_df_orders
from src.Load import load_data_redshift

# def extract_file() -> object:
#     column = ["datetime", "location", "name", "products",
#                 'total_price', "payment_type", "card_number"]
        
#     # Get key and bucket information
#     key = event['Records'][0]['s3']['object']['key']
#     bucket = event['Records'][0]['s3']['bucket']['name']
            
            
#     s3 = boto3.client('s3')
#     obj = s3.get_object(Bucket= bucket, Key= key)
#     raw_df = pd.read_csv(io.BytesIO(obj['Body'].read()), names = column)
#     return raw_df

def ETLPipeline(event, context):
    def extract_file() -> object:
        
        column = ["Datetime", "Location", "Customer", "Order",
                'Price', "Payment", "PII"]
        
        # Get key and bucket information
        key = event['Records'][0]['s3']['object']['key']
        bucket = event['Records'][0]['s3']['bucket']['name']
                
                
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket= bucket, Key= key)
        raw_df = pd.read_csv(io.BytesIO(obj['Body'].read()), names = column)
        
        return raw_df
    
    try:
        # # - Extract Data from S3 Bucket

        # raw_df = extract_file()

        # # - Query database for list of Branches 
        # # - Convert List to Dictionary
        # # - Create a List for Values of Dictionary and Keys of Dictionary
        # # - Set raw data location equal to ID of Branch in Database
        # # - Create New dataframe of Location ID's

        # locations_from_db = query_db_for_location_tuples()
        # dict_database_locations = dict(locations_from_db)

        # raw_locations = raw_df["location"]
        # location_keys = list(dict_database_locations.keys())
        # location_values = list(dict_database_locations.values())

        # location_id = get_location_ID(raw_locations, location_keys, location_values)

        # cleaned_location_id_df = pd.DataFrame()
        # cleaned_location_id_df["location"] = location_id

        # # - Query Database for all current products
        # # - Transform Raw Products
        # # - Compare database current products with transformed products 
        # # - Load New Products into Database

        # products_from_db = query_db_for_product_tuples()
        # raw_products = raw_df["products"]
        # zipped_products = transform_data(raw_products)
        # cleaned_products = zipped_items_into_list(zipped_products)
        # products_to_load = new_products_to_load(products_from_db, cleaned_products)
        # load_data_redshift("products", "product_size, product_name, product_price", "%s,%s,%s",products_to_load)

        # # - Clean Orders data
        # # - Load Orders into Database

        # zipped_order_tuples = get_order_tuples(raw_df, cleaned_location_id_df)
        # cleaned_orders = zipped_items_into_list(zipped_order_tuples)
        # load_data_redshift("orders", "date, time, location_id, total_price, payment_type","%s,%s,%s,%s,%s", cleaned_orders)

        # # - query database for updated products
        # # - query database for latest entries in orders
        # # - Create basket items
        # # - Load Basket items into Database

        # products_with_ids = query_db_for_product_tuples()
        # orders_with_ids = query_latest_entries("Customer_ID, location_id, total_price, payment_type", len(raw_df), "Orders", "Customer_ID")
        # zipped_basket = create_basket_tuples(products_with_ids, orders_with_ids, raw_products)
        # cleaned_basket = zipped_items_into_list(zipped_basket)
        # load_data_redshift("Basket", "customer_id, product_id, quantity", "%s,%s,%s",cleaned_basket)
        #column = ["Datetime", "Location", "Customer", "Order", "Price", "Payment", "PII"]

# file = pd.read_csv('longridge_25-08-2021_09-00-00.csv', names = column)
# file = pd.read_csv('chesterfield_25-08-2021_09-00-00.csv', names = column)
#df = pd.read_csv("chesterfield_25-08-2021_09-00-00.csv", names=column) # Chesterfield
#df = pd.read_csv("london_soho_25-08-2021_09-00-00.csv", names=column) # London Soho
#df = pd.read_csv("london_camden_25-08-2021_09-00-00.csv", names=column) # London Camden
#df = pd.read_csv("longridge_25-08-2021_09-00-00.csv", names=column) # Longride
#df = pd.read_csv("uppingham_25-08-2021_09-00-00.csv", names=column) # Uppingham
        df = extract_file()
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
        database_location_id_tuple = query_db_for_location_tuples()
        #database_location_id_tuple = [(1,"Chesterfield"),(2, "London Soho"),(3, "London Camden"),(4, "Longridge"),(5, "Uppingham"),(6,"Isle of Wight")]
        dict_database_id = dict(database_location_id_tuple)
        database_id_keys = list(dict_database_id.keys()) # Create a list of keys
        database_id_values = list(dict_database_id.values()) # Create a list of values
        location_id = get_location_ID(raw_locations, database_id_keys, database_id_values)

        # CLEAN PRODUCTS:
        database_products_id_tuple = query_db_for_product_tuples()
        #database_products_id_tuple = []
        current_zipped_products_from_csv = transform_data(raw_products)
        all_current_cleaned_products_from_csv = zipped_items_into_list(current_zipped_products_from_csv)
        new_products_for_db = new_products_to_load(database_products_id_tuple, all_current_cleaned_products_from_csv) # compare db products and csv products
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
        cleaned_basket_df["order_id"] = basket_df["orders_id"]
        cleaned_basket_df["customer_id"] = basket_df["customer_id"]
        cleaned_basket_df["product_id"] = basket_df["product_id"]
        zipped_basket = zip_from_basket_df(cleaned_basket_df)
        cleaned_basket = zipped_items_into_list(zipped_basket)
        load_data_redshift("basket", "order_id,customer_id,product_id", "%s,%s,%s", cleaned_basket)
        body = {
            
            "message": "Go Serverless v2.0! Your function executed successfully!",
            "input": event,
        }

        return {"statusCode": 200, "body": json.dumps(body)}
    
    except Exception as e:
        print(f"Error: {e}. StatusCode: 500")
