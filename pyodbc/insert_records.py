""" Program to perform the insertion of the record(s) of the table """

import csv
import json
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
STATES_FILE = 'states.csv'
CITIES_FILE = 'cities.json'
DB = DB_PATH + DB_FILE
STATES = DB_PATH + STATES_FILE
CITIES = DB_PATH + CITIES_FILE

# Make DNS string
CONNECTION_STRING = (
    r'DRIVER={0};'
    r'DBQ={1};'
).format(DB_DRIVER, DB)

STATES_SQL_SCRIPTS = """
    INSERT INTO states (id, name, iso_3166_2)
    VALUES (?, ?, ?);
"""

CITIES_SQL_SCRIPTS = """
    INSERT INTO cities (id, state_id, name, capital)
    VALUES (?, ?, ?, ?);
"""

CATEGORIES_SQL_SCRIPTS = """
    INSERT INTO categories (id, name, status)
    VALUES (?, ?, ?);
"""
CATEGORIES_MULTIPLE_ROWS = [
    (1, 'Tecnología', True),
    (2, 'Ropa', False),
    (3, 'Estética', True),
    (4, 'Herramientas', True),
    (5, 'Entretenimientos', True)
]

PRODUCTS_SQL_SCRIPTS = """
    INSERT INTO products(id, name, description, category_id, price, status)
    VALUES (?, ?, ?, ?, ?, ?);
"""
PRODUCTS_MULTIPLE_ROWS = [
    (1, "Pantalón Jean LEVI'S 511", "Producto 2 detallado", 2, 23.78, True),
    (2, 'Teléfono iPhone 13 Pro Max', 'Producto 2 detallado', 1, 1045.56, False),
    (3, 'Consola Play Station 5', 'Producto 3 detallado', 5, 234.8, True),
    (4, 'Caja de herramientas Stanley 99 piezas', 'Producto 4 detallado', 4, 899.99, True),
    (5, 'Zapatos Clarks', 'Producto 5 detallado', 2, 1234.62, True)
]

CUSTOMERS_SQL_SCRIPTS = """
    INSERT INTO customers (id, name, lastname, zip_code, phone)
    VALUES (?, ?, ?, ?, ?);
"""
CUSTOMERS_MULTIPLE_ROWS = [
    (1, 'Leo', 'Garcia', 245, '04144567239'),
    (2, 'Carol', 'Guevarra', 6, '04249804536'),
    (3, 'Rafa', 'Garcia', 461, '04121894605'),
    (4, 'Eduardo', 'Perez', 487, '04163387633'),
    (5, 'Manuel', 'Matos', 245, '04264893321')
]

ORDERS_SQL_SCRIPTS = """
    INSERT INTO orders (id, customer_id, made_at, product_id, status)
    VALUES (?, ?, ?, ?, ?);
"""
ORDERS_MULTIPLE_ROWS = [
    (1, 1, '12/02/2022 10:23:45 AM', 2, True),
    (2, 3, '01/06/2023 03:55:51 PM', 1, True),
    (3, 4, '02/18/2023 12:48:33 AM', 2, True),
    (4, 5, '03/22/2023 07:22:03 PM', 1, True),
    (5, 2, '03/12/2023 08:26:54 PM', 3, True)
]


def insert_records(sql="", rows=[], file_name=None):
    """Function to perform the insertion of several records from the table

    Args:
        sql (str, optional): The SQL INSERT statement. Defaults to "".
        rows (list, optional): The rows to inserts. Defaults to [].
        file_name (_type_, optional): The file name full path. Defaults to None.
    """

    try:
        connection = pyodbc.connect(CONNECTION_STRING)
        cursor = connection.cursor()
        print("\n")
        logging.info(f"Connected to Microsoft Access database {DB_FILE}!\n")

        if 'states' not in sql or 'cities' not in sql:
            if 'rows' in vars() and len(rows) == 1:
                count = cursor.execute(sql, rows).rowcount
                connection.commit()
                logging.info(f"{count} record(s) were successfully inserted into the table!\n")

            if 'rows' in vars() and len(rows) > 1:
                cursor.executemany(sql, rows)
                connection.commit()
                logging.info(f"{len(rows)} record(s) were successfully inserted into the table!\n")

        records_total = 0
        if 'states' in sql:
            with open(file_name, 'r', encoding='utf-8') as csv_file:
                rows = csv.reader(csv_file, delimiter=',')
                next(rows)
                # records_total = 0
                for row in rows:
                    cursor.execute(sql, row)
                    records_total = records_total + 1
                connection.commit()
                logging.info(f"{records_total} record(s) were successfully inserted into the table!\n")

        if 'cities' in sql:
            with open(file_name, 'r', encoding='utf-8') as json_file:
                rows = json.load(json_file)
                # records_total = 0
                for row in rows:
                    cursor.execute(sql, (row['id'], row['state_id'], row['name'], row['capital']))
                    records_total = records_total + 1
                connection.commit()
                logging.info(f"{records_total} record(s) were successfully inserted into the table!\n")

        cursor.close()

    except pyodbc.Error as error:
        sqlstate = error.args[1]
        sqlstate = sqlstate.split(".")
        print("Insertion of record(s) into table failed!", sqlstate)
    finally:
        if connection:
            connection.close()
            logging.info(f"The Microsoft Access connection to database '{DB_FILE}' was closed!")


if __name__ == "__main__":
    insert_records(sql=STATES_SQL_SCRIPTS, file_name=STATES)
    insert_records(sql=CITIES_SQL_SCRIPTS, file_name=CITIES)
    insert_records(sql=CATEGORIES_SQL_SCRIPTS, rows=CATEGORIES_MULTIPLE_ROWS)
    insert_records(sql=PRODUCTS_SQL_SCRIPTS, rows=PRODUCTS_MULTIPLE_ROWS)
    insert_records(sql=CUSTOMERS_SQL_SCRIPTS, rows=CUSTOMERS_MULTIPLE_ROWS)
    insert_records(sql=ORDERS_SQL_SCRIPTS, rows=ORDERS_MULTIPLE_ROWS)
