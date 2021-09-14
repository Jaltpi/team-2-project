import json
import pandas as pd
import io
import boto3
import csv
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
    body = {
        "message": "Go Serverless v2.0! Your function executed successfully!",
        "input": event,
    }

    return {"statusCode": 200, "body": json.dumps(body)}
