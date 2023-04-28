""" Program to deletion the record(s) of the table """

import logging
import os
import pyodbc

# logging INFO object
logging.basicConfig(level=logging.INFO)

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

STATES_SQL_SCRIPTS = """
    DELETE FROM states
    WHERE id = ?;
"""
STATES_ONE_ROW = [3,]
STATES_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]

CITIES_SQL_SCRIPTS = """
    DELETE FROM cities
    WHERE id = ?;
"""
CITIES_ONE_ROW = [3,]
CITIES_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]

CATEGORIES_SQL_SCRIPTS = """
    DELETE FROM categories
    WHERE id = ?;
"""
CATEGORIES_ONE_ROW = [3,]
CATEGORIES_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]

PRODUCTS_SQL_SCRIPTS = """
    DELETE FROM products
    WHERE id = ?;
"""
PRODUCTS_ONE_ROW = [3,]
PRODUCTS_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]

CUSTOMERS_SQL_SCRIPTS = """
    DELETE FROM customers
    WHERE id = ?;
"""
CUSTOMERS_ONE_ROW = [3,]
CUSTOMERS_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]

ORDERS_SQL_SCRIPTS = """
    DELETE FROM orders
    WHERE id = ?
"""
ORDERS_MULTIPLE_ROWS_SQL_SCRIPTS = """
    DELETE FROM orders
    WHERE id IN (?, ?, ?)
"""
ORDERS_ONE_ROW = [3,]
ORDERS_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]


def delete_records(sql=[], rows=[], size="all", table=""):
    """Function to perform table record deletion

    Args:
        sql (list, optional): The SQL DELETE statement. Defaults to [].
        rows (list, optional): The rows to delete. Defaults to [].
        size (str, optional): How many record to delete. Defaults to "all".
        table (str, optional): The table name to manipulate. Defaults to "".
    """

    try:
        connection = pyodbc.connect(CONNECTION_STRING)
        cursor = connection.cursor()
        print("\n")
        logging.info(f"Connected to Microsoft Access database '{DB_FILE}'!\n")

        if size == "one":
            # Delete a simple record
            count = cursor.execute(sql, rows).rowcount
            connection.commit()
            logging.info(f"Row(s) deleted: {count}\n")

        if size == "many":
            # Delete many records
            cursor.executemany(sql, rows)
            connection.commit()
            logging.info("Three Rows Deleted!\n")

        if size == "all":
            # Delete all records
            cursor.execute(f"DELETE FROM {table}")
            connection.commit()
            logging.info("All record(s) deleted successfully!\n")

        cursor.close()

    except pyodbc.Error as error:
        sqlstate = error.args[1]
        sqlstate = sqlstate.split(".")
        print("Delete record(s) in table failed!", sqlstate)
    finally:
        if connection:
            connection.close()
            logging.info(f"The connection to the Microsoft Access database '{DB_FILE}' was closed!\n")

if __name__ == "__main__":
    delete_records(sql=ORDERS_SQL_SCRIPTS, rows=ORDERS_ONE_ROW, size="one")
    delete_records(sql=ORDERS_SQL_SCRIPTS, rows=ORDERS_MULTIPLE_ROWS, size="many")
    delete_records(sql=ORDERS_SQL_SCRIPTS, size="all", table="orders")
    delete_records(sql=CUSTOMERS_SQL_SCRIPTS, rows=CUSTOMERS_ONE_ROW, size="one")
    delete_records(sql=CUSTOMERS_SQL_SCRIPTS, rows=CUSTOMERS_MULTIPLE_ROWS, size="many")
    delete_records(sql=CUSTOMERS_SQL_SCRIPTS, size="all", table="customers")
    delete_records(sql=PRODUCTS_SQL_SCRIPTS, rows=PRODUCTS_ONE_ROW, size="one")
    delete_records(sql=PRODUCTS_SQL_SCRIPTS, rows=PRODUCTS_MULTIPLE_ROWS, size="many")
    delete_records(sql=PRODUCTS_SQL_SCRIPTS, size="all", table="products")
    delete_records(sql=CATEGORIES_SQL_SCRIPTS, rows=CATEGORIES_ONE_ROW, size="one")
    delete_records(sql=CATEGORIES_SQL_SCRIPTS, rows=CATEGORIES_MULTIPLE_ROWS, size="many")
    delete_records(sql=CATEGORIES_SQL_SCRIPTS, size="all", table="categories")
    delete_records(sql=CITIES_SQL_SCRIPTS, rows=CITIES_ONE_ROW, size="one")
    delete_records(sql=CITIES_SQL_SCRIPTS, rows=CITIES_MULTIPLE_ROWS, size="many")
    delete_records(sql=CITIES_SQL_SCRIPTS, size="all", table="cities")
    delete_records(sql=STATES_SQL_SCRIPTS, rows=STATES_ONE_ROW, size="one")
    delete_records(sql=STATES_SQL_SCRIPTS, rows=STATES_MULTIPLE_ROWS, size="many")
    delete_records(sql=STATES_SQL_SCRIPTS, size="all", table="states")
