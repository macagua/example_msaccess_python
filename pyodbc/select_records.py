""" Program to query the record(s) of the table """

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
    SELECT *
    FROM estados
    ORDER BY nombre;
"""

CIUDADES_SQL_SCRIPTS = """
    SELECT
        ciu.id, ciu.nombre, ciu.capital,
        edo.nombre
    FROM
        estados AS edo,
        ciudades AS ciu
    WHERE ciu.estado_id = edo.id
    ORDER BY ciu.nombre;
"""

CATEGORIAS_SQL_SCRIPTS = """
    SELECT *
    FROM categorias
    ORDER BY nombre;
"""

PRODUCTOS_SQL_SCRIPTS = """
    SELECT
        pro.id, pro.nombre, pro.descripcion,
        cat.nombre, pro.precio, pro.status
    FROM
        categorias AS cat,
        productos AS pro
    WHERE pro.categoria_id = cat.id
    ORDER BY pro.nombre;
"""

CLIENTES_SQL_SCRIPTS = """
    SELECT
        cli.id, cli.nombre & ' ' & cli.apellido AS nombre_completo,
        ciu.nombre, cli.telefono
    FROM
        ciudades AS ciu,
        clientes AS cli
    WHERE cli.codigo_postal = ciu.id
    ORDER BY cli.id;
"""

PEDIDOS_SQL_SCRIPTS = """
    SELECT
        ped.id, cli.nombre & ' ' & cli.apellido AS nombre_completo,
        ped.fecha, pro.nombre, ped.status
    FROM
        clientes AS cli,
        productos AS pro,
        pedidos AS ped
    WHERE ped.cliente_id = cli.id
    AND ped.producto_id = pro.id
    ORDER BY ped.fecha;
"""

def select_row(sql=""):
    """Function to query the record(s) of the table

    Args:
        sql (str, optional): The SQL SELECT statement. Defaults to "".
    """

    try:
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
    select_row(sql=ESTADOS_SQL_SCRIPTS)
    select_row(sql=CIUDADES_SQL_SCRIPTS)
    select_row(sql=CATEGORIAS_SQL_SCRIPTS)
    select_row(sql=PRODUCTOS_SQL_SCRIPTS)
    select_row(sql=CLIENTES_SQL_SCRIPTS)
    select_row(sql=PEDIDOS_SQL_SCRIPTS)
