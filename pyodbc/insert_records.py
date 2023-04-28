""" Program to perform the insertion of the record(s) of the table """

import csv
import json
import logging
import os
import pyodbc

logging.basicConfig(level=logging.INFO)

DB_DRIVER = "{Microsoft Access Driver (*.mdb, *.accdb)}"
DB_PATH = os.path.dirname(
    os.path.abspath(__file__)
) + os.sep + "data" + os.sep
DB_FILE = 'database.accdb'
ESTADOS_FILE = 'estados.csv'
CIUDADES_FILE = 'ciudades.json'
DB = DB_PATH + DB_FILE
ESTADOS = DB_PATH + ESTADOS_FILE
CIUDADES = DB_PATH + CIUDADES_FILE

# Make DNS string
CONNECTION_STRING = (
    r'DRIVER={0};'
    r'DBQ={1};'
).format(DB_DRIVER, DB)

ESTADOS_SQL_SCRIPTS = """
    INSERT INTO estados (id, nombre, iso_3166_2)
    VALUES (?, ?, ?);
"""

CIUDADES_SQL_SCRIPTS = """
    INSERT INTO ciudades (id, estado_id, nombre, capital)
    VALUES (?, ?, ?, ?);
"""

CATEGORIAS_SQL_SCRIPTS = """
    INSERT INTO categorias (id, nombre, status)
    VALUES (?, ?, ?);
"""
CATEGORIAS_MULTIPLE_ROWS = [
    (1, 'Tecnología', True),
    (2, 'Ropa', False),
    (3, 'Estética', True),
    (4, 'Herramientas', True),
    (5, 'Entretenimientos', True)
]

PRODUCTOS_SQL_SCRIPTS = """
    INSERT INTO productos (id, nombre, descripcion, categoria_id, precio, status)
    VALUES (?, ?, ?, ?, ?, ?);
"""
PRODUCTOS_MULTIPLE_ROWS = [
    (1, "Pantalón Jean LEVI'S 511", "Producto 2 detallado", 2, 23.78, True),
    (2, 'Teléfono iPhone 13 Pro Max', 'Producto 2 detallado', 1, 1045.56, False),
    (3, 'Consola Play Station 5', 'Producto 3 detallado', 5, 234.8, True),
    (4, 'Caja de herramientas Stanley 99 piezas', 'Producto 4 detallado', 4, 899.99, True),
    (5, 'Zapatos Clarks', 'Producto 5 detallado', 2, 1234.62, True)
]

CLIENTES_SQL_SCRIPTS = """
    INSERT INTO clientes (id, nombre, apellido, codigo_postal, telefono)
    VALUES (?, ?, ?, ?, ?);
"""
CLIENTES_MULTIPLE_ROWS = [
    (1, 'Leo', 'Garcia', 245, '04144567239'),
    (2, 'Carol', 'Guevarra', 6, '04249804536'),
    (3, 'Rafa', 'Garcia', 461, '04121894605'),
    (4, 'Eduardo', 'Perez', 487, '04163387633'),
    (5, 'Manuel', 'Matos', 245, '04264893321')
]

PEDIDOS_SQL_SCRIPTS = """
    INSERT INTO pedidos (id, cliente_id, fecha, producto_id, status)
    VALUES (?, ?, ?, ?, ?);
"""
PEDIDOS_MULTIPLE_ROWS = [
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

        if 'estados' not in sql or 'ciudades' not in sql:
            if 'rows' in vars() and len(rows) == 1:
                count = cursor.execute(sql, rows).rowcount
                connection.commit()
                logging.info(f"{count} record(s) were successfully inserted into the table!\n")

            if 'rows' in vars() and len(rows) > 1:
                cursor.executemany(sql, rows)
                connection.commit()
                logging.info(f"{len(rows)} record(s) were successfully inserted into the table!\n")

        if 'estados' in sql:
            with open(file_name, 'r', encoding='utf-8') as csv_file:
                rows = csv.reader(csv_file, delimiter=',')
                next(rows)
                records_total = 0
                for row in rows:
                    cursor.execute(sql, row)
                    records_total = records_total + 1

        if 'ciudades' in sql:
            with open(file_name, 'r', encoding='utf-8') as json_file:
                rows = json.load(json_file)
                records_total = 0
                for row in rows:
                    cursor.execute(sql, (row['id'], row['estado_id'], row['nombre'], row['capital']))
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
    insert_records(sql=ESTADOS_SQL_SCRIPTS, file_name=ESTADOS)
    insert_records(sql=CIUDADES_SQL_SCRIPTS, file_name=CIUDADES)
    insert_records(sql=CATEGORIAS_SQL_SCRIPTS, rows=CATEGORIAS_MULTIPLE_ROWS)
    insert_records(sql=PRODUCTOS_SQL_SCRIPTS, rows=PRODUCTOS_MULTIPLE_ROWS)
    insert_records(sql=CLIENTES_SQL_SCRIPTS, rows=CLIENTES_MULTIPLE_ROWS)
    insert_records(sql=PEDIDOS_SQL_SCRIPTS, rows=PEDIDOS_MULTIPLE_ROWS)
