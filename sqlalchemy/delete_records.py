""" Program to deletion the record(s) of the table """

import logging
import os
import pyodbc
import urllib
from sqlalchemy import create_engine, bindparam, exc, func, text, MetaData


# logging INFO object
logging.basicConfig(level=logging.INFO)

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

STATES_ONE_ROW = [
    {"id": 3},
]
STATES_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

CITIES_ONE_ROW = [
    {"id": 3},
]
CITIES_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

CATEGORIES_ONE_ROW = [
    {"id": 3},
]
CATEGORIES_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

PRODUCTS_ONE_ROW = [
    {"id": 3},
]
PRODUCTS_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

CUSTOMERS_ONE_ROW = [
    {"id": 3},
]
CUSTOMERS_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

ORDERS_ONE_ROW = [
    {"id": 3},
]
ORDERS_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

# Set up connections between sqlalchemy and access+pyodbc
engine = create_engine(f"access+pyodbc:///?odbc_connect={urllib.parse.quote(CONNECTION_STRING)}")
connection = engine.connect()

# Instantiate metadate object
metadata = MetaData()
print("\n")
logging.info(f"Connected to Microsoft Access database '{DB_FILE}'!\n")

# Reflect metadata/schema from existing database to bring in existing tables
with engine.connect() as conn:
    metadata.reflect(conn)

states = metadata.tables["states"]
cities = metadata.tables["cities"]
categories = metadata.tables["categories"]
products = metadata.tables["products"]
customers = metadata.tables["customers"]
orders = metadata.tables["orders"]


def delete_records(rows=[], size="all", table=""):
    """Function to perform table record deletion

    Args:
        rows (list, optional): The rows to delete. Defaults to [].
        size (str, optional): How many record to delete. Defaults to "all".
        table (str, optional): The table name to manipulate. Defaults to "".
    """

    try:
        tables = metadata.tables.keys()
        id_param = [bindparam('id', value=item) for item in rows]
        with engine.begin() as conn:
            # Delete a simple record
            if size == "one":
                if table in tables:
                    result = 0
                    if table == "orders":
                        where_clause = orders.columns.id.in_(id_param)
                        statement = orders.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "customers":
                        where_clause = customers.c.id.in_(id_param)
                        statement = customers.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "products":
                        where_clause = products.c.id.in_(id_param)
                        statement = products.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "categories":
                        where_clause = categories.c.id.in_(id_param)
                        statement = categories.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "cities":
                        where_clause = cities.c.id.in_(id_param)
                        statement = cities.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "states":
                        where_clause = states.c.id.in_(id_param)
                        statement = states.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    logging.info(f"'{result.rowcount}' row(s) deleted successfully from '{table}' table!\n")

            # Delete a many records
            if size == "many" and len(rows) > 0:
                if table in tables:
                    result = 0
                    if table == "orders":
                        where_clause = orders.c.id.in_(id_param)
                        statement = orders.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "customers":
                        where_clause = customers.c.id.in_(id_param)
                        statement = customers.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "products":
                        where_clause = products.c.id.in_(id_param)
                        statement = products.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "categories":
                        where_clause = categories.c.id.in_(id_param)
                        statement = categories.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "cities":
                        where_clause = cities.c.id.in_(id_param)
                        statement = cities.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "states":
                        where_clause = states.c.id.in_(id_param)
                        statement = states.delete().where(where_clause)
                        print(statement)
                        print()
                        result = connection.execute(statement, rows)
                    logging.info(f"'Many' row(s) deleted successfully from '{table}' table!\n")

            # Delete all records
            if size == "all":
                if table in tables:
                    result = 0
                    if table == "orders":
                        statement = orders.delete()
                        print(statement)
                        result = connection.execute(statement)
                    if table == "customers":
                        statement = customers.delete()
                        print(statement)
                        result = connection.execute(statement)
                    if table == "products":
                        statement = products.delete()
                        print(statement)
                        result = connection.execute(statement)
                    if table == "categories":
                        statement = categories.delete()
                        print(statement)
                        result = connection.execute(statement)
                    if table == "cities":
                        statement = cities.delete()
                        print(statement)
                        result = connection.execute(statement)
                    if table == "states":
                        statement = states.delete()
                        print(statement)
                        result = connection.execute(statement)
                    logging.info(f"All '{result.rowcount}' record(s) deleted successfully from '{table}' table!")

    except exc.SQLAlchemyError as error:
        print("Delete record(s) in table failed!", error)
    except exc.IntegrityError as error:
        print("Delete record(s) not possible for id!", error)
    # finally:
    #     if connection:
    #         connection.close()
    #         logging.info(f"The connection to the Microsoft Access database '{DB_FILE}' was closed!\n")

if __name__ == "__main__":
    delete_records(rows=ORDERS_ONE_ROW, size="one", table="orders")
    delete_records(rows=ORDERS_MULTIPLE_ROWS, size="many", table="orders")
    delete_records(size="all", table="orders")
    delete_records(rows=CUSTOMERS_ONE_ROW, size="one", table="customers")
    delete_records(rows=CUSTOMERS_MULTIPLE_ROWS, size="many", table="customers")
    delete_records(size="all", table="customers")
    delete_records(rows=PRODUCTS_ONE_ROW, size="one", table="products")
    delete_records(rows=PRODUCTS_MULTIPLE_ROWS, size="many", table="products")
    delete_records(size="all", table="products")
    delete_records(rows=CATEGORIES_ONE_ROW, size="one", table="categories")
    delete_records(rows=CATEGORIES_MULTIPLE_ROWS, size="many", table="categories")
    delete_records(size="all", table="categories")
    delete_records(rows=CITIES_ONE_ROW, size="one", table="cities")
    delete_records(rows=CITIES_MULTIPLE_ROWS, size="many", table="cities")
    delete_records(size="all", table="cities")
    delete_records(rows=STATES_ONE_ROW, size="one", table="states")
    delete_records(rows=STATES_MULTIPLE_ROWS, size="many", table="states")
    delete_records(size="all", table="states")
