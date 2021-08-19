import psycopg2
import os
from dotenv import load_dotenv

# recommended to use psycopg2 module to connect to PostgreSQL

# Load environment variables from .env file


def get_env_variables(os_library: object) -> dict:
    load_dotenv()
    env_variables = {"host": os_library.environ.get("mysql_host"), "user": os_library.environ.get("mysql_user"),
                     "password": os_library.environ.get("mysql_pass"), "database": os_library.environ.get("mysql_db")}
    return env_variables
# Establish a database connection


def connect_to_db(env_variables: dict, postgres_library: object) -> object:
    connection = postgres_library.connect(
        env_variables["host"],
        env_variables["user"],
        env_variables["password"],
        env_variables["database"]
    )
    return connection


def get_cursor(connection: object) -> object:
    cursor = connection.cursor()
    return cursor

# Orders Table


def create_orders_table(env_variables: dict, postgres_library: object):
    connection = connect_to_db(env_variables, postgres_library)
    cursor = connection.execute("""CREATE TABLE IF NOT EXISTS Orders(
                                    Order_ID INT NOT NULL AUTO_INCREMENT, 
                                    Order_Date DATE, 
                                    Order_Time TIME,
                                    Order_Location VARCHAR (255), 
                                    Total_Spendature INT NOT NULL, 
                                    Payment_ID INT,
                                    FOREIGN KEY (Payment_ID) REFERENCES Payment_Method(Payment_ID),
                                    PRIMARY KEY (Order_ID))
                                """)
    connection.commit()
    cursor.close()
    connection.close()


def create_products_table(env_variables: dict, postgres_library: object):
    connection = connect_to_db(env_variables, postgres_library)
    cursor = connection.execute("""CREATE TABLE IF NOT EXISTS Products(
                                    Product_ID INT NOT NULL AUTO_INCREMENT,
                                    Product_name VARCHAR(255),
                                    Product_price REAL,
                                    PRIMARY KEY(Product_ID))
                                """)
    connection.commit()
    cursor.close()
    connection.close()


def create_orderproduct_table(env_variables: dict, postgres_library: object):
    connection = connect_to_db(env_variables, postgres_library)
    cursor = connection.execute('''CREATE TABLE IF NOT EXISTS Order_Product(
                                    FOREIGN KEY(Order_ID) REFERENCES Orders(Order_ID),
                                    FOREIGN KEY(Product_ID) REFERENCES Products(Product_ID))
                                ''')
    connection.commit()
    cursor.close()
    connection.close()


def create_payment_method_table(env_variables: dict, postgres_library: object):
    connection = connect_to_db(env_variables, postgres_library)
    cursor = connection.execute('''CREATE TABLE IF NOT EXISTS Payments(
                                    Payment_ID INT NOT NULL AUTO_INCREMENT,
                                    Payment_Type VARCHAR(255))
                                ''')
    connection.commit()
    cursor.close()
    connection.close()

# if __name__ == "__main__":
#     main()
