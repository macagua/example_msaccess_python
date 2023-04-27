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

ESTADOS_VALUES = {
    "nombre": bindparam('nombre'),
}
ESTADOS_ONE_ROW = [
    {"nombre": "MÉRIDA", "_id": 13},
]
ESTADOS_MULTIPLE_ROWS = [
    {"nombre": "ANZOÁTEQUI", "_id": 2},
    {"nombre": "LARA", "_id": 12},
    {"nombre": "ZULIA", "_id": 23},
]

CIUDADES_VALUES = {
    "nombre": bindparam('nombre'),
}
CIUDADES_ONE_ROW = [
    {"nombre": "MÉRIDA", "_id": 13},
]
CIUDADES_MULTIPLE_ROWS = [
    {"nombre": "BARCELONA", "_id": 2},
    {"nombre": "BACHAQUERO", "_id": 461},
    {"nombre": "MARACAIBO", "_id": 487},
]

CATEGORIAS_VALUES = {
    "nombre": bindparam('nombre'),
}
CATEGORIAS_ONE_ROW = [
    {"nombre": "TECNOLOGÍA", "_id": 1},
]
CATEGORIAS_MULTIPLE_ROWS = [
    {"nombre": "ESTÉTICA", "_id": 3},
    {"nombre": "HERRAMIENTAS", "_id": 4},
    {"nombre": "ENTRETENIMIENTOS", "_id": 5},
]

PRODUCTOS_VALUES = {
    "descripcion": bindparam('descripcion'),
    "precio": bindparam('precio'),
}
PRODUCTOS_ONE_ROW = [
    {"descripcion": "Pantalón Jean LEVI'S 511 Slim Fit, Talla 34x32 y 34x34, Color disponible Pumped Up, 99% Algodón y 1% Elastane, Hecho en Bangladesh. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", "precio": 59.33, "_id": 1},
]
PRODUCTOS_MULTIPLE_ROWS = [
    {"descripcion": "Consola Play Station 5; Edición Gob Of War; Capacidad 825 GB; Memoria RAM de 16 GB; Tipo de consola de sobremesa, Wi-FI incluido; cantidad de controles incluidos 1. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", "precio": 829.00, "_id": 3},
    {"descripcion": "Caja de herramientas Stanley 99 piezas; Caja de herramientas Stanley, Cantidad de piezas incluidas 99 como dados, extensiones, destornillador y llaves. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", "precio": 180.30, "_id": 4},
    {"descripcion": "Zapatos Clarks; Talla 39, 40 y 41; Colores disponible  Negro, Marrón, Azul; 99% Cuero 1% Tela, Hecho en Londres. Producto 100% original, Producto importado de Inglaterra. Envíos a todo el país.", "precio": 120.00, "_id": 5}
]

CLIENTES_VALUES = {
    "nombre": bindparam('nombre'),
    "apellido": bindparam('apellido'),
}
CLIENTES_ONE_ROW = [
    {"nombre": "Leonardo", "apellido": "Caballero", "_id": 1},
]
CLIENTES_MULTIPLE_ROWS = [
    {"nombre": "Ana", "apellido": "Poleo", "_id": 3},
    {"nombre": "Rafael", "apellido": "Lugo", "_id": 4},
    {"nombre": "Maximiliano", "apellido": "Vilchez", "_id": 5},
]

PEDIDOS_VALUES = {
    "fecha": bindparam('fecha'),
    "status": bindparam('status'),
}
PEDIDOS_ONE_ROW = [
    {"fecha": "12/02/2022 11:23:34 PM", "status": False, "_id": 1},
]
PEDIDOS_MULTIPLE_ROWS = [
    {"fecha": "02/18/2023 10:22:33 AM", "status": False, "_id": 3},
    {"fecha": "04/22/2023 09:22:03 AM", "status": False, "_id": 4},
    {"fecha": "03/12/2023 12:26:54 AM", "status": False, "_id": 5},
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


def update_records(values={}, rows=[], size="all", table=""):
    """Function to update table records

    Args:
        values (dict, optional): The bind param values. Defaults to {}.
        rows (list, optional): The rows to update. Defaults to [].
        size (str, optional): How many record to update. Defaults to "all".
        table (str, optional): The table name to manipulate. Defaults to "".
    """

    try:
        tables = metadata.tables.keys()

        with engine.begin() as conn:
            # Update a simple record
            if size == "one":
                if table in tables:
                    result = 0
                    if table == "pedidos":
                        where_clause = pedidos.c.id == bindparam('_id')
                        statement = pedidos.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "clientes":
                        where_clause = clientes.c.id == bindparam('_id')
                        statement = clientes.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "productos":
                        where_clause = productos.c.id == bindparam('_id')
                        statement = productos.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "categorias":
                        where_clause = categorias.c.id == bindparam('_id')
                        statement = categorias.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "ciudades":
                        where_clause = ciudades.c.id == bindparam('_id')
                        statement = ciudades.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "estados":
                        where_clause = estados.c.id == bindparam('_id')
                        statement = estados.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    logging.info(f"'{result.rowcount}' row(s) updated successfully from '{table}' table!\n")

            # Update a many records
            if size == "many" and len(rows) > 0:
                if table in tables:
                    result = 0
                    if table == "pedidos":
                        where_clause = pedidos.c.id == bindparam('_id')
                        statement = pedidos.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "clientes":
                        where_clause = clientes.c.id == bindparam('_id')
                        statement = clientes.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "productos":
                        where_clause = productos.c.id == bindparam('_id')
                        statement = productos.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "categorias":
                        where_clause = categorias.c.id == bindparam('_id')
                        statement = categorias.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "ciudades":
                        where_clause = ciudades.c.id == bindparam('_id')
                        statement = ciudades.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "estados":
                        where_clause = estados.c.id == bindparam('_id')
                        statement = estados.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    logging.info(f"'Many' row(s) updated successfully from '{table}' table!\n")

    except exc.SQLAlchemyError as error:
        print("Update record(s) in table failed!", error)
    except exc.IntegrityError as error:
        print("Update record(s) not possible for id!", error)
    # finally:
    #     if connection:
    #         connection.close()
    #         logging.info(f"The connection to the Microsoft Access database '{DB_FILE}' was closed!\n")

if __name__ == "__main__":
    update_records(values=PEDIDOS_VALUES, rows=PEDIDOS_ONE_ROW, size="one", table="pedidos")
    update_records(values=PEDIDOS_VALUES, rows=PEDIDOS_MULTIPLE_ROWS, size="many", table="pedidos")
    update_records(values=CLIENTES_VALUES, rows=CLIENTES_ONE_ROW, size="one", table="clientes")
    update_records(values=CLIENTES_VALUES, rows=CLIENTES_MULTIPLE_ROWS, size="many", table="clientes")
    update_records(values=PRODUCTOS_VALUES, rows=PRODUCTOS_ONE_ROW, size="one", table="productos")
    update_records(values=PRODUCTOS_VALUES, rows=PRODUCTOS_MULTIPLE_ROWS, size="many", table="productos")
    update_records(values=CATEGORIAS_VALUES, rows=CATEGORIAS_ONE_ROW, size="one", table="categorias")
    update_records(values=CATEGORIAS_VALUES, rows=CATEGORIAS_MULTIPLE_ROWS, size="many", table="categorias")
    update_records(values=CIUDADES_VALUES, rows=CIUDADES_ONE_ROW, size="one", table="ciudades")
    update_records(values=CIUDADES_VALUES, rows=CIUDADES_MULTIPLE_ROWS, size="many", table="ciudades")
    update_records(values=ESTADOS_VALUES, rows=ESTADOS_ONE_ROW, size="one", table="estados")
    update_records(values=ESTADOS_VALUES, rows=ESTADOS_MULTIPLE_ROWS, size="many", table="estados")
