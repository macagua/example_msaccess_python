"""Program to create the database tables"""

import os
import pyodbc


# Define full path for database file
DB_DRIVER = "{Microsoft Access Driver (*.mdb, *.accdb)}"
DB_PATH = os.path.dirname(
    os.path.abspath(__file__)
) + os.sep + "data" + os.sep
DB_FILE = 'database.accdb'
DB = DB_PATH + DB_FILE

# Make DNS string
CONNECTION_STRING = (
    r'DRIVER={0};'
    r'DBQ={1};'
    'ExtendedAnsiSQL=1;'
).format(DB_DRIVER, DB)

# Set up connections between pyodbc and microsoft access
connection = pyodbc.connect(CONNECTION_STRING)
print("\nConnected to Microsoft Access database!\n")
cursor = connection.cursor()

# If The 'states' Table exists!
if cursor.tables(table="states").fetchone():
    print("The 'states' Table exists!\n")
else:
    print("The 'states' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    statement = """
        CREATE TABLE states (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,name VARCHAR(25) NOT NULL
                ,iso_3166_2 VARCHAR(4) NOT NULL
        );
    """
    cursor.execute(statement)
    connection.commit()
    print("The 'states' Table Created!\n")

# If The 'cities' Table exists!
if cursor.tables(table="cities").fetchone():
    print("The 'cities' Table exists!\n")
else:
    print("The 'cities' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    statement = """
        CREATE TABLE cities (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,state_id INTEGER NOT NULL
                ,name VARCHAR(200) NOT NULL
                ,capital SMALLINT NOT NULL
                ,CONSTRAINT [fk_state_id] FOREIGN KEY (state_id) REFERENCES states(id)
        );
    """
    cursor.execute(statement)
    connection.commit()
    print("The 'cities' Table Created!\n")

# If The 'categories' Table exists!
if cursor.tables(table="categories").fetchone():
    print("The 'categories' Table exists!\n")
else:
    print("The 'categories' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    statement = """
        CREATE TABLE categories (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,name VARCHAR(25) NOT NULL
                ,status BIT NOT NULL
        );
    """
    cursor.execute(statement)
    connection.commit()
    print("The 'categories' Table Created!\n")

# If The 'products' Table exists!
if cursor.tables(table="products").fetchone():
    print("The 'products' Table exists!\n")
else:
    print("The 'products' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    statement = """
        CREATE TABLE products (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,name VARCHAR(50) NOT NULL
                ,description VARCHAR(250) NOT NULL
                ,category_id INTEGER NOT NULL
                ,price SINGLE NOT NULL
                ,status BIT NOT NULL
                ,CONSTRAINT [fk_category_id] FOREIGN KEY (category_id) REFERENCES categories(id)
        );
    """
    cursor.execute(statement)
    connection.commit()
    print("The 'products' Table Created!\n")

# If The 'customers' Table exists!
if cursor.tables(table="customers").fetchone():
    print("The 'customers' Table exists!\n")
else:
    print("The 'customers' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    statement = """
        CREATE TABLE customers (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,name VARCHAR(25) NOT NULL
                ,lastname VARCHAR(25) NOT NULL
                ,zip_code INTEGER NOT NULL
                ,phone VARCHAR(11) NOT NULL
                ,CONSTRAINT [fk_city_id] FOREIGN KEY (zip_code) REFERENCES cities(id)
        );
    """
    cursor.execute(statement)
    connection.commit()
    print("The 'customers' Table Created!\n")

# If The 'orders' Table exists!
if cursor.tables(table="orders").fetchone():
    print("The 'orders' Table exists!\n")
else:
    print("The 'orders' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    statement = """
        CREATE TABLE orders (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,customer_id INTEGER NOT NULL
                ,made_at DATETIME NOT NULL
                ,product_id INTEGER NOT NULL
                ,status BIT NOT NULL
                ,CONSTRAINT [fk_customer_id] FOREIGN KEY (customer_id) REFERENCES customers(id)
                ,CONSTRAINT [fk_product_id] FOREIGN KEY (product_id) REFERENCES products(id)
        );
    """
    cursor.execute(statement)
    connection.commit()
    print("The 'orders' Table Created!\n")


if connection:
    connection.close()
    print(f"The Microsoft Access connection to database '{DB_FILE}' was closed!")
