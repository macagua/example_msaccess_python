""" Program to update the record(s) of the table """

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
    UPDATE states
    SET name = ?
    WHERE id = ?;
"""
STATES_ONE_ROW = ["MÉRIDA", 13]
STATES_MULTIPLE_ROWS = [
    ("ANZOÁTEQUI", 2),
    ("ZULIA", 23),
    ("LARA", 12)
]

CITIES_SQL_SCRIPTS = """
    UPDATE cities
    SET name = ?
    WHERE id = ?;
"""
CITIES_ONE_ROW = ["MÉRIDA", 13]
CITIES_MULTIPLE_ROWS = [
    ("BARCELONA", 2),
    ("BACHAQUERO", 461),
    ("MARACAIBO", 487)
]

CATEGORIES_SQL_SCRIPTS = """
    UPDATE categories
    SET name = ?
    WHERE id = ?;
"""
CATEGORIES_ONE_ROW = ["TECNOLOGÍA", 1]
CATEGORIES_MULTIPLE_ROWS = [
    ("ESTÉTICA", 3),
    ("HERRAMIENTAS", 4),
    ("ENTRETENIMIENTOS", 5)
]

PRODUCTS_SQL_SCRIPTS = """
    UPDATE products
    SET
        description = ?,
        price = ?
    WHERE id = ?
"""
PRODUCTS_ONE_ROW = ["Pantalón Jean LEVI'S 511 Slim Fit, Talla 34x32 y 34x34, Color disponible Pumped Up, 99% Algodón y 1% Elastane, Hecho en Bangladesh. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", '59.33', 1]
PRODUCTS_MULTIPLE_ROWS = [
    ("Consola Play Station 5; Edición Gob Of War; Capacidad 825 GB; Memoria RAM de 16 GB; Tipo de consola de sobremesa, Wi-FI incluido; cantidad de controles incluidos 1. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", 829.00, 3),
    ("Caja de herramientas Stanley 99 piezas; Caja de herramientas Stanley, Cantidad de piezas incluidas 99 como dados, extensiones, destornillador y llaves. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", 180.30, 4),
    ("Zapatos Clarks; Talla 39, 40 y 41; Colores disponible  Negro, Marrón, Azul; 99% Cuero 1% Tela, Hecho en Londres. Producto 100% original, Producto importado de Inglaterra. Envíos a todo el país.", 120.00, 5)
]

CUSTOMERS_SQL_SCRIPTS = """
    UPDATE customers
    SET
        name = ?,
        lastname = ?
    WHERE id = ?;
"""
CUSTOMERS_ONE_ROW = ["Leonardo", "Caballero", 1]
CUSTOMERS_MULTIPLE_ROWS = [
    ("Ana", "Poleo", 3),
    ("Rafael", "Lugo", 4),
    ("Maximiliano", "Vilchez", 5)
]

ORDERS_SQL_SCRIPTS = """
    UPDATE orders
    SET
        made_at = ?,
        status = ?
    WHERE id = ?;
"""
ORDERS_ONE_ROW = ["12/02/2022 11:23:34 PM", False, 1]
ORDERS_MULTIPLE_ROWS = [
    ("02/18/2023 10:22:33 AM", False, 3),
    ("04/22/2023 09:22:03 AM", False, 4),
    ("03/12/2023 12:26:54 AM", False, 5)
]

def update_records(sql="", rows=[], size=""):
    """Function to perform the update of several records from the table

    Args:
        sql (str, optional): The SQL UPDATE statement. Defaults to "".
        rows (list, optional): The rows to update. Defaults to [].
        size (str, optional): How many record to update. Defaults to "".
    """

    try:
        connection = pyodbc.connect(CONNECTION_STRING)
        cursor = connection.cursor()
        print("\n")
        logging.info(f"Connected to Microsoft Access database '{DB_FILE}'!\n")

        if size == "one":
            count = cursor.execute(sql, rows).rowcount
            connection.commit()

            logging.info(
                "1 record was successfully updated in the table!\n"
            )

        if size == "many":
            cursor.executemany(sql, rows)
            connection.commit()
            logging.info(f"{len(rows)} record(s) were successfully updated into the table!\n")

        cursor.close()

    except pyodbc.Error as error:
        sqlstate = error.args[1]
        sqlstate = sqlstate.split(".")
        print("Update of record(s) in table failed!", sqlstate)
    finally:
        if connection:
            connection.close()
            print("\n")
            logging.info(f"The connection to the Microsoft Access database '{DB_FILE}' was closed!\n")


if __name__ == "__main__":
    update_records(sql=STATES_SQL_SCRIPTS, rows=STATES_ONE_ROW, size="one")
    update_records(sql=STATES_SQL_SCRIPTS, rows=STATES_MULTIPLE_ROWS, size="many")
    update_records(sql=CITIES_SQL_SCRIPTS, rows=CITIES_ONE_ROW, size="one")
    update_records(sql=CITIES_SQL_SCRIPTS, rows=CITIES_MULTIPLE_ROWS, size="many")
    update_records(sql=CATEGORIES_SQL_SCRIPTS, rows=CATEGORIES_ONE_ROW, size="one")
    update_records(sql=CATEGORIES_SQL_SCRIPTS, rows=CATEGORIES_MULTIPLE_ROWS, size="many")
    update_records(sql=PRODUCTS_SQL_SCRIPTS, rows=PRODUCTS_ONE_ROW, size="one")
    update_records(sql=PRODUCTS_SQL_SCRIPTS, rows=PRODUCTS_MULTIPLE_ROWS, size="many")
    update_records(sql=CUSTOMERS_SQL_SCRIPTS, rows=CUSTOMERS_ONE_ROW, size="one")
    update_records(sql=CUSTOMERS_SQL_SCRIPTS, rows=CUSTOMERS_MULTIPLE_ROWS, size="many")
    update_records(sql=ORDERS_SQL_SCRIPTS, rows=ORDERS_ONE_ROW, size="one")
    update_records(sql=ORDERS_SQL_SCRIPTS, rows=ORDERS_MULTIPLE_ROWS, size="many")
