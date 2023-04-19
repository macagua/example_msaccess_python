""" Program to update the record(s) of the table """

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
    UPDATE estados
    SET nombre = ?
    WHERE id = ?;
"""
ESTADOS_ONE_ROW = ["MÉRIDA", 13]
ESTADOS_MULTIPLE_ROWS = [
    ("ANZOÁTEQUI", 2),
    ("ZULIA", 23),
    ("LARA", 12)
]

CIUDADES_SQL_SCRIPTS = """
    UPDATE ciudades
    SET nombre = ?
    WHERE id = ?;
"""
CIUDADES_ONE_ROW = ["MÉRIDA", 13]
CIUDADES_MULTIPLE_ROWS = [
    ("BARCELONA", 2),
    ("BACHAQUERO", 461),
    ("MARACAIBO", 487)
]

CATEGORIAS_SQL_SCRIPTS = """
    UPDATE categorias
    SET nombre = ?
    WHERE id = ?;
"""
CATEGORIAS_ONE_ROW = ["TECNOLOGÍA", 1]
CATEGORIAS_MULTIPLE_ROWS = [
    ("ESTÉTICA", 3),
    ("HERRAMIENTAS", 4),
    ("ENTRETENIMIENTOS", 5)
]

PRODUCTOS_SQL_SCRIPTS = """
    UPDATE productos
    SET
        descripcion = ?,
        precio = ?
    WHERE id = ?
"""
PRODUCTOS_ONE_ROW = ["Pantalón Jean LEVI'S 511 Slim Fit, Talla 34x32 y 34x34, Color disponible Pumped Up, 99% Algodón y 1% Elastane, Hecho en Bangladesh. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", '59.33', 1]
PRODUCTOS_MULTIPLE_ROWS = [
    ("Consola Play Station 5; Edición Gob Of War; Capacidad 825 GB; Memoria RAM de 16 GB; Tipo de consola de sobremesa, Wi-FI incluido; cantidad de controles incluidos 1. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", 829.00, 3),
    ("Caja de herramientas Stanley 99 piezas; Caja de herramientas Stanley, Cantidad de piezas incluidas 99 como dados, extensiones, destornillador y llaves. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", 180.30, 4),
    ("Zapatos Clarks; Talla 39, 40 y 41; Colores disponible  Negro, Marrón, Azul; 99% Cuero 1% Tela, Hecho en Londres. Producto 100% original, Producto importado de Inglaterra. Envíos a todo el país.", 120.00, 5)
]

CLIENTES_SQL_SCRIPTS = """
    UPDATE clientes
    SET
        nombre = ?,
        apellido = ?
    WHERE id = ?;
"""
CLIENTES_ONE_ROW = ["Leonardo", "Caballero", 1]
CLIENTES_MULTIPLE_ROWS = [
    ("Ana", "Poleo", 3),
    ("Rafael", "Lugo", 4),
    ("Maximiliano", "Vilchez", 5)
]

PEDIDOS_SQL_SCRIPTS = """
    UPDATE pedidos
    SET
        fecha = ?,
        status = ?
    WHERE id = ?;
"""
PEDIDOS_ONE_ROW = ["12/02/2022 11:23:34 PM", False, 1]
PEDIDOS_MULTIPLE_ROWS = [
    ("02/18/2023 10:22:33 AM", False, 3),
    ("04/22/2023 09:22:03 AM", False, 4),
    ("03/12/2023 12:26:54 AM", False, 5)
]

def update_row(sql, rows, size):
    """
    Function to perform the update of several records from the table
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
            logging.info(
                "{} record(s) were successfully updated into the table!\n".format(
                    len(rows)
                )
            )

        cursor.close()

    except pyodbc.Error as error:
        print("Update of record(s) in table failed!", error)
    finally:
        if connection:
            connection.close()
            print("\n")
            logging.info(f"The Microsoft Access connection to database '{DB_FILE}' was closed!\n")


if __name__ == "__main__":
    update_row(ESTADOS_SQL_SCRIPTS, ESTADOS_ONE_ROW, size="one")
    update_row(ESTADOS_SQL_SCRIPTS, ESTADOS_MULTIPLE_ROWS, size="many")
    update_row(CIUDADES_SQL_SCRIPTS, CIUDADES_ONE_ROW, size="one")
    update_row(CIUDADES_SQL_SCRIPTS, CIUDADES_MULTIPLE_ROWS, size="many")
    update_row(CATEGORIAS_SQL_SCRIPTS, CATEGORIAS_ONE_ROW, size="one")
    update_row(CATEGORIAS_SQL_SCRIPTS, CATEGORIAS_MULTIPLE_ROWS, size="many")
    update_row(PRODUCTOS_SQL_SCRIPTS, PRODUCTOS_ONE_ROW, size="one")
    update_row(PRODUCTOS_SQL_SCRIPTS, PRODUCTOS_MULTIPLE_ROWS, size="many")
    update_row(CLIENTES_SQL_SCRIPTS, CLIENTES_ONE_ROW, size="one")
    update_row(CLIENTES_SQL_SCRIPTS, CLIENTES_MULTIPLE_ROWS, size="many")
    update_row(PEDIDOS_SQL_SCRIPTS, PEDIDOS_ONE_ROW, size="one")
    update_row(PEDIDOS_SQL_SCRIPTS, PEDIDOS_MULTIPLE_ROWS, size="many")
