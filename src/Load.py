import psycopg2
import boto3
# from sqlalchemy import create_engine
# from config import config


# TODO = ["Create database", "Create tables in database", "Upload data into tables"]


# Use Pandas -> SQL
# DataFrame.to_sql(name, con, schema=None, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None, method=None)

# name is database
# con is connection
# schema is postgresql
# if_exists set to append or replace
# dtype change price datatype to float, everything else should remain a string

# Create Connection
# Template to create engine below
# engine = create_engine("postgresql://username:password@localhost:port/database_name")
# engine = create_engine("postgresql://root:password@localhost:5432/database")


# Pandas DataFrame Orders -> SQL
# order_table_df.to_sql("Orders", engine, if_exists = "replace", dtype = {Total_Spent: float64})

# Pandas DataFrame Products -> SQL
# cleaned_product_df.to_sql("Products", engine, if_exists ="replace", dtype = {"Product_price": float64})

# Pandas DataFrame Payment -> SQl
# payments_df.to_sql("Payments", engine, if_exists ="replace")

# def pandas_to_sql(dataframe: object, table_name: str, connection, schema=None, if_exists="replace", index=False, index_label=None, chunksize=None, dtype=None, method=None):
#     """"This function allows the user to take a dataframe and moves its data into a SQL database"""
#     try:
#         dataframe.to_sql(table_name, connection, schema,
#                          if_exists, index, index_label, chunksize, dtype)
#     except Exception as e:
#         print(f"Error: {e}.")
#         return "Fail"
#     else:
#         return "Success"


# def create_connection(system: str, user_name: str, password: str, host: str, port: str, database_name: str):
#     """"This function allows the user the create an engine using SQLAlchemy. If successful, it returns the connection."""
#     try:
#         print("Establishing connection too database...")
#         engine = create_engine(
#             f"{system}://{user_name}:{password}@{host}:{port}/{database_name}")

#     except Exception as e:
#         print(f"Error: {e}. Failure to connect to database.")
#         return "Fail"

#     else:
#         print("Successful connection to database established.")
#         return engine


# def SQL_INSERT_STATEMENT_FROM_DATAFRAME(SOURCE, TARGET):
#     sql_texts = []
#     for index, row in SOURCE.iterrows():
#         sql_texts.append('INSERT INTO '+TARGET+' (' +
#                          str(', '.join(SOURCE.columns)) + ') VALUES ' + str(tuple(row.values)))
#     return sql_texts


# def SQL_commands_executor(commands: list):
#     conn = None
#     try:
#         # read the connection parameters
#         params = config()
#         # connect to the PostgreSQL server
#         conn = psycopg2.connect(**params)
#         cur = conn.cursor()
#         # create command one by one
#         for command in commands:
#             cur.execute(command)
#         # close communication with the PostgreSQL database server
#         cur.close()
#         # commit the changes
#         conn.commit()
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#     finally:
#         if conn is not None:
#             conn.close()
def load_data_redshift(table: str, columns: str, Items: list):
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
        cursor.executemany(f"""INSERT INTO {table} VALUES ({columns})""", Items)
        print("Command executed successfully.")
        conn.commit()
        conn.close()
        return "Success"
    except Exception as e:
            print(f"ERROR: {e}. StatusCode: 500")
            