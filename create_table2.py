import psycopg2
from config import config


def create_tables():

    commands = (
        """CREATE TABLE Payment(
                Payment_ID INT PRIMARY KEY NOT NULL,
                Payment_Type VARCHAR(255)
        )
        """,
        """CREATE TABLE IF NOT EXISTS Orders (
            Order_ID SERIAL PRIMARY KEY,
            Order_Date DATE,
            Order_Time Time,
            Order_Location Varchar (255),
            Total_Spendature REAL NOT NULL,
            Payment_ID INT,
            FOREIGN KEY (Payment_ID) REFERENCES Payment (Payment_ID)
        )
        """,
        """ CREATE TABLE IF NOT EXISTS Products (
                Product_ID SERIAL PRIMARY KEY,
                Product_Name VARCHAR(255) NOT NULL,
                Product_Size VARCHAR(255) NOT NULL,
                Product_Price REAL
                )
        """,
        """CREATE TABLE IF NOT EXISTS Order_Product(
                Order_ID INT,
                Product_ID INT,
                Quantity INT NOT NULL,                
                FOREIGN KEY (Order_id)
                REFERENCES Orders (Order_ID),
                FOREIGN KEY (Product_ID)
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
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()
