""" Program to delete tables from a database """

import os
import pyodbc


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
).format(DB_DRIVER, DB)

connection = pyodbc.connect(CONNECTION_STRING, autocommit=True)
print("\nConnected to Microsoft Access database!\n")
cursor = connection.cursor()

total_count = 0

# If The 'orders' Table exists!
if cursor.tables(table="orders").fetchone():
    print("The 'orders' Table exists!\n")
    # Run DROP TABLE SQL statement
    # total_count = 0
    cursor.execute("DROP TABLE orders")
    total_count = total_count + 1
    connection.commit()
    print("The 'orders' Table was deleted!\n")

# If The 'customers' Table exists!
if cursor.tables(table="customers").fetchone():
    print("The 'customerss' Table exists!\n")
    # Run DROP TABLE SQL statement
    # total_count = 0
    cursor.execute("DROP TABLE customers")
    total_count = total_count + 1
    connection.commit()
    print("The 'customers' Table was deleted!\n")

# If The 'products' Table exists!
if cursor.tables(table="products").fetchone():
    print("The 'products' Table exists!\n")
    # Run DROP TABLE SQL statement
    # total_count = 0
    cursor.execute("DROP TABLE products")
    total_count = total_count + 1
    connection.commit()
    print("The 'products Table was deleted!\n")

# If The 'categories' Table exists!
if cursor.tables(table="categories").fetchone():
    print("The 'categories' Table exists!\n")
    # Run DROP TABLE SQL statement
    # total_count = 0
    cursor.execute("DROP TABLE categories")
    total_count = total_count + 1
    connection.commit()
    print("The 'categories' Table was deleted!\n")

# If The 'cities' Table exists!
if cursor.tables(table="cities").fetchone():
    print("The 'cities' Table exists!\n")
    # Run DROP TABLE SQL statement
    # total_count = 0
    cursor.execute("DROP TABLE cities")
    total_count = total_count + 1
    connection.commit()
    print("The 'cities' Table was deleted!\n")

# If The 'states' Table exists!
if cursor.tables(table="states").fetchone():
    print("The 'states' Table exists!\n")
    # Run DROP TABLE SQL statement
    cursor.execute("DROP TABLE states")
    total_count = total_count + 1
    connection.commit()
    print("The 'states' Table was deleted!\n")

print("{0} Table(s) Deleted!\n".format(total_count))

connection.close()
print("Desconnected to Microsoft Access database!")
