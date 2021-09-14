import json
from logging import exception
import pandas as pd
import io
import boto3
from src.Extract import query_db_for_location_tuples, query_db_for_product_tuples, query_latest_entries
from src.transform import get_location_ID, get_order_tuples, transform_data, zipped_items_into_list
from src.transform import new_products_to_load, create_basket_tuples
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
        
        column = ["datetime", "location", "name", "products",
                'total_price', "payment_type", "card_number"]
        
        # Get key and bucket information
        key = event['Records'][0]['s3']['object']['key']
        bucket = event['Records'][0]['s3']['bucket']['name']
                
                
        s3 = boto3.client('s3')
        obj = s3.get_object(Bucket= bucket, Key= key)
        raw_df = pd.read_csv(io.BytesIO(obj['Body'].read()), names = column)
        
        return raw_df
    
    try:
        # - Extract Data from S3 Bucket

        raw_df = extract_file()

        # - Query database for list of Branches 
        # - Convert List to Dictionary
        # - Create a List for Values of Dictionary and Keys of Dictionary
        # - Set raw data location equal to ID of Branch in Database
        # - Create New dataframe of Location ID's

        locations_from_db = query_db_for_location_tuples()
        dict_database_locations = dict(locations_from_db)

        raw_locations = raw_df["location"]
        location_keys = list(dict_database_locations.keys())
        location_values = list(dict_database_locations.values())

        location_id = get_location_ID(raw_locations, location_keys, location_values)

        cleaned_location_id_df = pd.DataFrame()
        cleaned_location_id_df["location"] = location_id

        # - Query Database for all current products
        # - Transform Raw Products
        # - Compare database current products with transformed products 
        # - Load New Products into Database

        products_from_db = query_db_for_product_tuples()
        raw_products = raw_df["products"]
        zipped_products = transform_data(raw_products)
        cleaned_products = zipped_items_into_list(zipped_products)
        products_to_load = new_products_to_load(products_from_db, cleaned_products)
        load_data_redshift("products", "product_size, product_name, product_price", products_to_load)

        # - Clean Orders data
        # - Load Orders into Database

        zipped_order_tuples = get_order_tuples(raw_df, cleaned_location_id_df)
        cleaned_orders = zipped_items_into_list(zipped_order_tuples)
        load_data_redshift("orders", "date, time, location_id, total_price, payment_type", cleaned_orders)

        # - query database for updated products
        # - query database for latest entries in orders
        # - Create basket items
        # - Load Basket items into Database

        products_with_ids = query_db_for_product_tuples()
        orders_with_ids = query_latest_entries("Customer_ID, location_id, total_price, payment_type", len(raw_df), "Orders", "Customer_ID")
        zipped_basket = create_basket_tuples(products_with_ids, orders_with_ids, raw_df)
        cleaned_basket = zipped_items_into_list(zipped_basket)
        load_data_redshift("Basket", "customer_id, product_id, quantity", cleaned_basket)
        
        body = {
            
            "message": "Go Serverless v2.0! Your function executed successfully!",
            "input": event,
        }

        return {"statusCode": 200, "body": json.dumps(body)}
    
    except Exception as e:
        print(f"Error: {e}. StatusCode: 500")
