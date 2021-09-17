import psycopg2
import boto3

def load_data_redshift(table: str, columns: str, data: str, Items: list):
    """This function connects to the the redshift database and inserts a list of tuples if the connection was successful.
    User must provide the table name (string), and which columns (string without parenthesis enclosing the column names)
    to insert the data into"""
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
        
        # Insert Data into table
        for item in Items:
            cursor.execute(f"""INSERT INTO {table} ({columns}) VALUES ({data}) """, item)
            
        #cursor.executemany(f"""INSERT INTO {table} ({columns}) VALUES ({data})""", (Items))
        print("Command executed successfully.")
        conn.commit()
        conn.close()
        return "Success"
    except Exception as e:
            print(f"ERROR: {e}. StatusCode: 500")
            