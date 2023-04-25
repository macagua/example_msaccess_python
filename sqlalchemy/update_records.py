""" Program to update the record(s) of the table """

import logging
import os
import pyodbc
import urllib
from sqlalchemy import create_engine, bindparam, exc, func, text, MetaData

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

ESTADOS_VALUES = [
    {"nombre": bindparam('nombre')},
]
ESTADOS_ONE_ROW = [
    {"nombre": "MÉRIDA", "id": 13},
]
ESTADOS_MULTIPLE_ROWS = [
    {"nombre": "ANZOÁTEQUI", "id": 2},
    {"nombre": "LARA", "id": 12},
    {"nombre": "ZULIA", "id": 23},
]

CIUDADES_VALUES = [
    {"nombre": bindparam('nombre')},
]
CIUDADES_ONE_ROW = [
    {"nombre": "MÉRIDA", "id": 13},
]
CIUDADES_MULTIPLE_ROWS = [
    {"nombre": "BARCELONA", "id": 2},
    {"nombre": "BACHAQUERO", "id": 461},
    {"nombre": "MARACAIBO", "id": 487},
]

CATEGORIAS_VALUES = [
    {"nombre": bindparam('nombre')},
]
CATEGORIAS_ONE_ROW = [
    {"nombre": "TECNOLOGÍA", "id": 1},
]
CATEGORIAS_MULTIPLE_ROWS = [
    {"nombre": "ESTÉTICA", "id": 3},
    {"nombre": "HERRAMIENTAS", "id": 4},
    {"nombre": "ENTRETENIMIENTOS", "id": 5},
]

PRODUCTOS_VALUES = [
    {"descripcion": bindparam('descripcion'), "precio": bindparam('precio')},
]
PRODUCTOS_ONE_ROW = [
    {"descripcion": "Pantalón Jean LEVI'S 511 Slim Fit, Talla 34x32 y 34x34, Color disponible Pumped Up, 99% Algodón y 1% Elastane, Hecho en Bangladesh. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", "precio": 59.33, "id": 1},
]
PRODUCTOS_MULTIPLE_ROWS = [
    {"descripcion": "Consola Play Station 5; Edición Gob Of War; Capacidad 825 GB; Memoria RAM de 16 GB; Tipo de consola de sobremesa, Wi-FI incluido; cantidad de controles incluidos 1. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", "precio": 829.00, "id": 3},
    {"descripcion": "Caja de herramientas Stanley 99 piezas; Caja de herramientas Stanley, Cantidad de piezas incluidas 99 como dados, extensiones, destornillador y llaves. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", "precio": 180.30, "id": 4},
    {"descripcion": "Zapatos Clarks; Talla 39, 40 y 41; Colores disponible  Negro, Marrón, Azul; 99% Cuero 1% Tela, Hecho en Londres. Producto 100% original, Producto importado de Inglaterra. Envíos a todo el país.", "precio": 120.00, "id": 5}
]

CLIENTES_VALUES = [
    {"nombre": bindparam('nombre'), "apellido": bindparam('apellido')},
]
CLIENTES_ONE_ROW = [
    {"nombre": "Leonardo", "apellido": "Caballero", "id": 1},
]
CLIENTES_MULTIPLE_ROWS = [
    {"nombre": "Ana", "apellido": "Poleo", "id": 3},
    {"nombre": "Rafael", "apellido": "Lugo", "id": 4},
    {"nombre": "Maximiliano", "apellido": "Vilchez", "id": 5},
]

PEDIDOS_VALUES = [
    {"fecha": bindparam('fecha'), "status": bindparam('status')},
]
PEDIDOS_ONE_ROW = [
    {"fecha": "12/02/2022 11:23:34 PM", "status": False, "id": 1},
]
PEDIDOS_MULTIPLE_ROWS = [
    {"fecha": "02/18/2023 10:22:33 AM", "status": False, "id": 3},
    {"fecha": "04/22/2023 09:22:03 AM", "status": False, "id": 4},
    {"fecha": "03/12/2023 12:26:54 AM", "status": False, "id": 5},
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

estados = metadata.tables["estados"]
ciudades = metadata.tables["ciudades"]
categorias = metadata.tables["categorias"]
productos = metadata.tables["productos"]
clientes = metadata.tables["clientes"]
pedidos = metadata.tables["pedidos"]


def update_records(values=[], rows=[], size="all", table=""):
    """
    Function to update table records
    """

    try:
        tables = metadata.tables.keys()

        with engine.begin() as conn:
            # Update a simple record
            if size == "one":
                if table in tables:
                    result = 0
                    if table == "pedidos":
                        where_clause = pedidos.columns.id == bindparam('id')
                        statement = pedidos.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement, rows)
                    if table == "clientes":
                        where_clause = clientes.c.id == bindparam('id')
                        statement = clientes.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    if table == "productos":
                        where_clause = productos.c.id == bindparam('id')
                        statement = productos.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    if table == "categorias":
                        where_clause = categorias.c.id == bindparam('id')
                        statement = categorias.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    if table == "ciudades":
                        where_clause = ciudades.c.id == bindparam('id')
                        statement = ciudades.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    if table == "estados":
                        where_clause = estados.c.id == bindparam('id')
                        statement = estados.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    logging.info(f"'{result.rowcount}' row(s) updated successfully from '{table}' table!")

            # Update a many records
            if size == "many" and len(rows) > 0:
                if table in tables:
                    result = 0
                    if table == "pedidos":
                        where_clause = pedidos.c.id == bindparam('id')
                        statement = pedidos.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    if table == "clientes":
                        where_clause = clientes.c.id == bindparam('id')
                        statement = clientes.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    if table == "productos":
                        where_clause = productos.c.id == bindparam('id')
                        statement = productos.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    if table == "categorias":
                        where_clause = categorias.c.id == bindparam('id')
                        statement = categorias.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    if table == "ciudades":
                        where_clause = ciudades.c.id == bindparam('id')
                        statement = ciudades.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    if table == "estados":
                        where_clause = estados.c.id == bindparam('id')
                        statement = estados.update().where(where_clause).values(values)
                        print(statement, rows)
                        result = connection.execute(statement)
                    logging.info(f"'Many' row(s) updated successfully from '{table}' table!")

    except exc.SQLAlchemyError as error:
        print("Update record(s) in table failed!", error)
    except exc.IntegrityError as error:
        print("Update record(s) not possible for id!", error)
    finally:
        if connection:
            connection.close()
            logging.info(
                "The connection to the Microsoft Access database '{}' was closed!\n".format(
                    DB_FILE
                )
            )

if __name__ == "__main__":
    update_records(PEDIDOS_VALUES, PEDIDOS_VALUES, PEDIDOS_ONE_ROW, size="one", table="pedidos")
    update_records(PEDIDOS_VALUES, PEDIDOS_VALUES, PEDIDOS_MULTIPLE_ROWS, size="many", table="pedidos")
    update_records(CLIENTES_VALUES, CLIENTES_ONE_ROW, size="one", table="clientes")
    update_records(CLIENTES_VALUES, CLIENTES_MULTIPLE_ROWS, size="many", table="clientes")
    update_records(PRODUCTOS_VALUES, PRODUCTOS_ONE_ROW, size="one", table="productos")
    update_records(PRODUCTOS_VALUES, PRODUCTOS_MULTIPLE_ROWS, size="many", table="productos")
    update_records(CATEGORIAS_VALUES, CATEGORIAS_ONE_ROW, size="one", table="categorias")
    update_records(CATEGORIAS_VALUES, CATEGORIAS_MULTIPLE_ROWS, size="many", table="categorias")
    update_records(CIUDADES_VALUES, CIUDADES_ONE_ROW, size="one", table="ciudades")
    update_records(CIUDADES_VALUES, CIUDADES_MULTIPLE_ROWS, size="many", table="ciudades")
    update_records(ESTADOS_VALUES, ESTADOS_ONE_ROW, size="one", table="estados")
    update_records(ESTADOS_VALUES, ESTADOS_MULTIPLE_ROWS, size="many", table="estados")
