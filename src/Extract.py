import pandas as pd
import boto3
import io
import psycopg2

# def extract_csv_via_pandas(file_name, column_names: list):
#     df = pd.read_csv(file_name, names=column_names)
#     return df


# column = ["DateTime", "Location", "Customer",
#           "Order", "Payment_method", "Final_price", "PII"]
# extract_csv_via_pandas('2021-02-23-isle-of-wight.csv', column)

def extract_file() -> object:
    """This function reads a CSV File from an S3 Bucket and returns a dataframe containing the raw data"""
    column = ["datetime", "location", "name", "products",
                'total_price', "payment_type", "card_number"]
        
    # Get key and bucket information
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
            
            
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket= bucket, Key= key)
    raw_df = pd.read_csv(io.BytesIO(obj['Body'].read()), names = column)
    return raw_df

def query_db_for_location_tuples() -> list:
    """This function queries the redshift database and returns a list of tuples if the connection was successful."""
    try:
        client = boto3.client('redshift', region_name='eu-west-1')
        REDSHIFT_USER = "awsuser"
        REDSHIFT_CLUSTER = "redshiftcluster-fbtitpjkbelw"
        REDSHIFT_HOST = "redshiftcluster-fbtitpjkbelw.cnvqpqjunvdy.eu-west-1.redshift.amazonaws.com"
        REDSHIFT_DATABASE = "team2db" # DATABASE NAME GOES HERE
        
        # Obtain Credentials
        creds = client.get_cluster_credentials(
        DbUser=REDSHIFT_USER,
        DbName=REDSHIFT_DATABASE,
        ClusterIdentifier=REDSHIFT_CLUSTER,
        DurationSeconds=3600)
        
        # Create connection
        conn = psycopg2.connect(
        user=creds['DbUser'],
        password=creds['DbPassword'],
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,
        port=5439
        )
       
        # Create a cursor
        cursor = conn.cursor()
       
        # Query Database for all items in locations table
        cursor.execute("SELECT * FROM locations")
        
        items = cursor.fetchall()
        
        conn.close()
        return items
    
    except Exception as e:
            print(f"ERROR: {e}. StatusCode: 500")
            
def query_db_for_product_tuples() -> list:
    """This function queries the redshift database and returns a list of tuples if the connection was successful."""
    try:
        client = boto3.client('redshift', region_name='eu-west-1')
        REDSHIFT_USER = "awsuser"
        REDSHIFT_CLUSTER = "redshiftcluster-fbtitpjkbelw"
        REDSHIFT_HOST = "redshiftcluster-fbtitpjkbelw.cnvqpqjunvdy.eu-west-1.redshift.amazonaws.com"
        REDSHIFT_DATABASE = "team2db" # DATABASE NAME GOES HERE
        
        # Obtain Credentials
        creds = client.get_cluster_credentials(
        DbUser=REDSHIFT_USER,
        DbName=REDSHIFT_DATABASE,
        ClusterIdentifier=REDSHIFT_CLUSTER,
        DurationSeconds=3600)
        
        # Create connection
        conn = psycopg2.connect(
        user=creds['DbUser'],
        password=creds['DbPassword'],
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,
        port=5439
        )
       
        # Create a cursor
        cursor = conn.cursor()
       
        # Query Database for all items in products table
        cursor.execute("SELECT * FROM products")
        items = cursor.fetchall()
        
        conn.close()
        return items
    except Exception as e:
            print(f"ERROR: {e}. StatusCode: 500")
            

def query_latest_entries(item: str, amount: int, table: str, primary_key: str) -> list:
    """This function queries the database for the most recent input based on the user selection"""
    try:
        client = boto3.client('redshift', region_name='eu-west-1')
        REDSHIFT_USER = "awsuser"
        REDSHIFT_CLUSTER = "redshiftcluster-fbtitpjkbelw"
        REDSHIFT_HOST = "redshiftcluster-fbtitpjkbelw.cnvqpqjunvdy.eu-west-1.redshift.amazonaws.com"
        REDSHIFT_DATABASE = "team2db" # DATABASE NAME GOES HERE
        
        # Obtain Credentials
        creds = client.get_cluster_credentials(
        DbUser=REDSHIFT_USER,
        DbName=REDSHIFT_DATABASE,
        ClusterIdentifier=REDSHIFT_CLUSTER,
        DurationSeconds=3600)
        
        # Create connection
        conn = psycopg2.connect(
        user=creds['DbUser'],
        password=creds['DbPassword'],
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,
        port=5439
        )
        
        # Create a cursor
        cursor = conn.cursor()
        
        # Query Database for all recent entries from a specific column in a selected table
        cursor.execute(f"""SELECT {item}
        FROM (SELECT TOP {amount} * FROM {table}
        ORDER BY {primary_key} DESC)
        ORDER BY {primary_key} ASC;""")
        items = cursor.fetchall()
        
        conn.commit
        
        conn.close()
        return items
    except Exception as e:
            print(f"ERROR: {e}. StatusCode: 500")