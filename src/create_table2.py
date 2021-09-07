from config import config
import psycopg2
import boto3

client = boto3.client('redshift')


def create_database(user, password, host, port):
    # establishing the connection
    conn = psycopg2.connect(user=user, password=password, host=host, port=port)
    conn.autocommit = True

    # Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    # Preparing query to create a database
    sql = '''CREATE database infinite_data_system'''

    # Creating a database
    cursor.execute(sql)
    print("Database created successfully......")

    # Closing the connection
    conn.close()


def create_tables():

    commands = (
        """CREATE TABLE IF NOT EXISTS Orders(
            Customer_ID SERIAL PRIMARY KEY,
            Date VARCHAR(255),
            Time VARCHAR(255),
            Location VARCHAR(255),
            Total_price REAL,
            Payment_Type VARCHAR(255)
        )
        """,
        """ CREATE TABLE IF NOT EXISTS Products(
                Product_ID SERIAL PRIMARY KEY,
                Product_Name VARCHAR(255),
                Product_Size VARCHAR(255),
                Product_Price REAL
                )
        """,
        """CREATE TABLE IF NOT EXISTS Order_Product(
                Order_ID SERIAL PRIMARY KEY,
                Customer_ID INT,
                Product_ID INT,
                Quantity INT,
                FOREIGN KEY(Customer_ID)
                REFERENCES Orders(Customer_ID),
                FOREIGN KEY(Product_ID)
                REFERENCES Products(Product_ID)
        )
        """)
    conn = None

    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # commit the changes
        conn.commit()
        # close communication with the PostgreSQL database server
        cur.close()
        print('Table created successfully......')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


db = config()
host = db["host"]
user = db["user"]
password = db["password"]


create_database(host=host, user=user, password=password, port="5432")
create_tables()


# if __name__ == '__main__':
#     create_tables()
