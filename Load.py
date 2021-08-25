import psycopg2
from sqlalchemy import create_engine

#TODO = ["Create database", "Create tables in database", "Upload data into tables"]


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

def pandas_to_sql(dataframe: object, table_name: str, connection, schema = None, if_exists = "replace", index = True, index_label = None, chunksize = None, dtype = None, method = None):
    """"This function allows the user to take a dataframe and moves its data into a SQL database"""
    try:
        dataframe.to_sql(table_name, connection, schema, if_exists, index, index_label, chunksize, dtype)
    except Exception as e:
        print(f"Error: {e}.")
        return "Fail"
    else:
        return "Success"
        
def create_connection(system: str,user_name: str, password: str, host: str, port: str, database_name: str):
    """"This function allows the user the create an engine using SQLAlchemy. If successful, it returns the connection."""
    try:
        print("Establishing connection too database...")
        engine = create_engine(f"{system}://{user_name}:{password}@{host}:{port}/{database_name}")
    
    except Exception as e:
        print(f"Error: {e}. Failure to connect to database.")
        return "Fail"
    
    else:
        print("Successful connection to database established.")
        return engine