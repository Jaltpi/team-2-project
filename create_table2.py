import psycopg2
from config import config


def create_tables():

    commands = (
        """CREATE TABLE IF NOT EXISTS Payment(
                Payment_ID SERIAL PRIMARY KEY NOT NULL,
                Payment_Type VARCHAR(255)
        )
        """,
        """CREATE TABLE IF NOT EXISTS Orders(
            Order_ID SERIAL PRIMARY KEY,
            Date VARCHAR(255),
            Time VARCHAR(255),
            Location VARCHAR(255),
            Total_price REAL,
            Payment_ID INT,
            FOREIGN KEY(Payment_ID) REFERENCES Payment(Payment_ID)
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
                Order_Id INT,
                Product_ID INT,
                Quantity INT,
                FOREIGN KEY(Order_ID)
                REFERENCES Orders(Order_ID),
                FOREIGN KEY(Product_ID)
                REFERENCES Products(Product_ID)

        )
        """,
        """CREATE TABLE IF NOT EXISTS Card_Type(
            Card_ID SERIAL PRIMARY KEY,
            Card_Type VARCHAR(255)
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
        print('success')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


if __name__ == '__main__':
    create_tables()
