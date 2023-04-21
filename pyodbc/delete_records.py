""" Program to deletion the record(s) of the table """

import logging
import os
import pyodbc

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

ESTADOS_SQL_SCRIPTS = """
    DELETE FROM estados
    WHERE id = ?;
"""
ESTADOS_ONE_ROW = [3,]
ESTADOS_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]

CIUDADES_SQL_SCRIPTS = """
    DELETE FROM ciudades
    WHERE id = ?;
"""
CIUDADES_ONE_ROW = [3,]
CIUDADES_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]

CATEGORIAS_SQL_SCRIPTS = """
    DELETE FROM categorias
    WHERE id = ?;
"""
CATEGORIAS_ONE_ROW = [3,]
CATEGORIAS_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]

PRODUCTOS_SQL_SCRIPTS = """
    DELETE FROM productos
    WHERE id = ?;
"""
PRODUCTOS_ONE_ROW = [3,]
PRODUCTOS_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]

CLIENTES_SQL_SCRIPTS = """
    DELETE FROM clientes
    WHERE id = ?;
"""
CLIENTES_ONE_ROW = [3,]
CLIENTES_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]

PEDIDOS_SQL_SCRIPTS = """
    DELETE FROM pedidos
    WHERE id = ?
"""
PEDIDOS_MULTIPLE_ROWS_SQL_SCRIPTS = """
    DELETE FROM pedidos
    WHERE id IN (?, ?, ?)
"""
PEDIDOS_ONE_ROW = [3,]
PEDIDOS_MULTIPLE_ROWS = [
    (1,), (2,), (4,)
]


def delete_row(sql, rows=[], size="all", table=""):
    """
    Function to perform table record deletion
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
        print("Delete record(s) in table failed!", error)
    finally:
        if connection:
            connection.close()
            logging.info(
                "The connection to the Microsoft Access database '{}' was closed!\n".format(
                    DB_FILE
                )
            )

if __name__ == "__main__":
    delete_row(PEDIDOS_SQL_SCRIPTS, PEDIDOS_ONE_ROW, size="one")
    delete_row(PEDIDOS_SQL_SCRIPTS, PEDIDOS_MULTIPLE_ROWS, size="many")
    delete_row(PEDIDOS_SQL_SCRIPTS, size="all", table="pedidos")
    delete_row(CLIENTES_SQL_SCRIPTS, CLIENTES_ONE_ROW, size="one")
    delete_row(CLIENTES_SQL_SCRIPTS, CLIENTES_MULTIPLE_ROWS, size="many")
    delete_row(CLIENTES_SQL_SCRIPTS, size="all", table="clientes")
    delete_row(PRODUCTOS_SQL_SCRIPTS, PRODUCTOS_ONE_ROW, size="one")
    delete_row(PRODUCTOS_SQL_SCRIPTS, PRODUCTOS_MULTIPLE_ROWS, size="many")
    delete_row(PRODUCTOS_SQL_SCRIPTS, size="all", table="productos")
    delete_row(CATEGORIAS_SQL_SCRIPTS, CATEGORIAS_ONE_ROW, size="one")
    delete_row(CATEGORIAS_SQL_SCRIPTS, CATEGORIAS_MULTIPLE_ROWS, size="many")
    delete_row(CATEGORIAS_SQL_SCRIPTS, size="all", table="categorias")
    delete_row(CIUDADES_SQL_SCRIPTS, CIUDADES_ONE_ROW, size="one")
    delete_row(CIUDADES_SQL_SCRIPTS, CIUDADES_MULTIPLE_ROWS, size="many")
    delete_row(CIUDADES_SQL_SCRIPTS, size="all", table="ciudades")
    delete_row(ESTADOS_SQL_SCRIPTS, ESTADOS_ONE_ROW, size="one")
    delete_row(ESTADOS_SQL_SCRIPTS, ESTADOS_MULTIPLE_ROWS, size="many")
    delete_row(ESTADOS_SQL_SCRIPTS, size="all", table="estados")
