"""Program to query the record(s) of the table"""

import logging
import os
import pyodbc

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

STATES_SQL_SCRIPTS = """
    SELECT *
    FROM states
    ORDER BY name;
"""

CITIES_SQL_SCRIPTS = """
    SELECT
        cit.id, cit.name, cit.capital,
        sta.name
    FROM
        states AS edo,
        cities AS ciu
    WHERE cit.state_id = sta.id
    ORDER BY cit.name;
"""

CATEGORIES_SQL_SCRIPTS = """
    SELECT *
    FROM categories
    ORDER BY name;
"""

PRODUCTS_SQL_SCRIPTS = """
    SELECT
        pro.id, pro.name, pro.description,
        cat.name, pro.price, pro.status
    FROM
        categories AS cat,
        products AS pro
    WHERE pro.category_id = cat.id
    ORDER BY pro.name;
"""

CUSTOMERS_SQL_SCRIPTS = """
    SELECT
        cus.id, cus.name & ' ' & cus.lastname AS name_full,
        cit.name, cus.phone
    FROM
        cities AS cit,
        customers AS cus
    WHERE cus.zip_code = cit.id
    ORDER BY cus.id;
"""

ORDERS_SQL_SCRIPTS = """
    SELECT
        ord.id, cus.name & ' ' & cus.lastname AS name_full,
        ord.made_at, pro.name, ord.status
    FROM
        customers AS cus,
        products AS pro,
        orders AS ord
    WHERE ord.customer_id = cus.id
    AND ord.product_id = pro.id
    ORDER BY ord.made_at;
"""

def select_records(sql=""):
    """Function to query the record(s) of the table

    Args:
        sql (str, optional): The SQL SELECT statement. Defaults to "".
    """

    try:
        # Set up connections between pyodbc and microsoft access
        connection = pyodbc.connect(CONNECTION_STRING)
        cursor = connection.cursor()
        print("\n")
        logging.info(f"Connected to Microsoft Access database '{DB_FILE}'!\n")

        # Run SELECT SQL statement
        cursor.execute(sql)

        # Get one row
        one_row = cursor.fetchone()
        print(one_row)
        print("One Row Seleted!\n")

        # Get all rows
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        print("All Rows Seleted!\n")

        logging.info(f"Query(s) {len(rows) + 1} record(s) in table successfully!\n")

        cursor.close()

    except pyodbc.Error as error:
        sqlstate = error.args[1]
        sqlstate = sqlstate.split(".")
        print("Query for record(s) in table failed!", sqlstate)
    finally:
        if connection:
            connection.close()
            print("\n")
            logging.info(f"The connection to the Microsoft Access database '{DB_FILE}' was closed!\n")

if __name__ == "__main__":
    select_records(sql=STATES_SQL_SCRIPTS)
    select_records(sql=CITIES_SQL_SCRIPTS)
    select_records(sql=CATEGORIES_SQL_SCRIPTS)
    select_records(sql=PRODUCTS_SQL_SCRIPTS)
    select_records(sql=CUSTOMERS_SQL_SCRIPTS)
    select_records(sql=ORDERS_SQL_SCRIPTS)
