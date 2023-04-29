""" Program to create the database tables """

import os
import pyodbc
import urllib
from sqlalchemy import create_engine, MetaData, \
    Column, Boolean, Integer, Numeric, SmallInteger, \
    String, Date, Table, ForeignKey 


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

# Set up connections between sqlalchemy and access+pyodbc
# Instantiate metadate object
engine = create_engine(f"access+pyodbc:///?odbc_connect={urllib.parse.quote(CONNECTION_STRING)}")
metadata = MetaData()

# DDL for states, cities, categories, products, customers and orders 
states_tbl = Table(
    "states",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(25), nullable=False),
    Column("iso_3166_2", String(4), nullable=False),
)

cities_tbl = Table(
    "cities",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("state_id", ForeignKey("states.id"), nullable=False),
    Column("name", String(200), nullable=False),
    Column("capital", SmallInteger, nullable=False)
)

categories_tbl = Table(
    "categories",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(25), nullable=True),
    Column("status", Boolean, nullable=True)
)

products_tbl = Table(
    "products",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(50), nullable=True),
    Column("description", String(250), nullable=True),
    Column("category_id", ForeignKey("categories.id"), nullable=False),
    Column("price", Numeric(10,2), nullable=False),
    Column("status", Boolean, nullable=True)
)

customers_tbl = Table(
    "customers",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String(25), nullable=True),
    Column("lastname", String(25), nullable=True),
    Column("zip_code", ForeignKey("cities.id"), nullable=False),
    Column("phone", String(20), nullable=True)
)

orders_tbl = Table(
    "orders",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("customer_id", ForeignKey("customers.id"), nullable=False),
    Column("made_at", Date, nullable=False),
    Column("product_id", ForeignKey("products.id"), nullable=False),
    Column("status", Boolean, nullable=True)
)

# Get all of the tables
tables = metadata.tables.keys()

# Start transaction to commit DDL to database
with engine.begin() as conn:
    metadata.create_all(conn)
    print()

    for table in tables:
        print(f"The '{table}' table successfully created!")

print(f"'{len(tables)}' table(s) successfully created!")
