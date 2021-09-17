import json
from logging import exception
import pandas as pd
import io
import boto3
from src.app import etl

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
        
        df = extract_file() # Extract Data
        etl(df) # Run Pipeline
        
        body = {
            
            "message": "Go Team 2! Your function executed successfully!",
            "input": event,
        }

        return {"statusCode": 200, "body": json.dumps(body)}
    
    except Exception as e:
        print(f"Error: {e}. StatusCode: 500")
