""" Program to deletion the record(s) of the table """

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

ESTADOS_ONE_ROW = [
    {"id": 3},
]
ESTADOS_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

CIUDADES_ONE_ROW = [
    {"id": 3},
]
CIUDADES_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

CATEGORIAS_ONE_ROW = [
    {"id": 3},
]
CATEGORIAS_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

PRODUCTOS_ONE_ROW = [
    {"id": 3},
]
PRODUCTOS_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

CLIENTES_ONE_ROW = [
    {"id": 3},
]
CLIENTES_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
]

PEDIDOS_ONE_ROW = [
    {"id": 3},
]
PEDIDOS_MULTIPLE_ROWS = [
    {"id": 1},
    {"id": 2},
    {"id": 4},
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


def delete_records(rows=[], size="all", table=""):
    """
    Function to perform table record deletion
    """

    try:
        tables = metadata.tables.keys()
        with engine.begin() as conn:
            # Delete a simple record
            if size == "one":
                if table in tables:
                    result = 0
                    if table == "pedidos":
                        where_clause = pedidos.columns.id.in_([bindparam('id', value=item) for item in rows])
                        statement = pedidos.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    if table == "clientes":
                        where_clause = clientes.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = clientes.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    if table == "productos":
                        where_clause = productos.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = productos.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    if table == "categorias":
                        where_clause = categorias.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = categorias.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    if table == "ciudades":
                        where_clause = ciudades.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = ciudades.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    if table == "estados":
                        where_clause = estados.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = estados.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    logging.info(f"'{result.rowcount}' row(s) deleted successfully from '{table}' table!")

            # Delete a many records
            if size == "many" and len(rows) > 0:
                if table in tables:
                    result = 0
                    if table == "pedidos":
                        where_clause = pedidos.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = pedidos.delete().where(where_clause)
                        print(statement)
                        result = connection.execute(statement, rows)
                    if table == "clientes":
                        where_clause = clientes.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = clientes.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    if table == "productos":
                        where_clause = productos.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = productos.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    if table == "categorias":
                        where_clause = categorias.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = categorias.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    if table == "ciudades":
                        where_clause = ciudades.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = ciudades.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    if table == "estados":
                        where_clause = estados.c.id.in_([bindparam('id', value=item) for item in rows])
                        statement = estados.delete().where(where_clause)
                        result = connection.execute(statement, rows)
                    logging.info(f"'Many' row(s) deleted successfully from '{table}' table!")

            # Delete all records
            if size == "all":
                if table in tables:
                    result = 0
                    if table == "pedidos":
                        statement = pedidos.delete()
                        result = connection.execute(statement)
                    if table == "clientes":
                        statement = clientes.delete()
                        result = connection.execute(statement)
                    if table == "productos":
                        statement = productos.delete()
                        result = connection.execute(statement)
                    if table == "categorias":
                        statement = categorias.delete()
                        result = connection.execute(statement)
                    if table == "ciudades":
                        statement = ciudades.delete()
                        result = connection.execute(statement)
                    if table == "estados":
                        statement = estados.delete()
                        result = connection.execute(statement)
                    logging.info(f"All '{result.rowcount}' record(s) deleted successfully from '{table}' table!")

    except exc.SQLAlchemyError as error:
        print("Delete record(s) in table failed!", error)
    except exc.IntegrityError as error:
        print("Delete record(s) not possible for id!", error)
    finally:
        if conn:
            conn.close()
            logging.info(
                "The connection to the Microsoft Access database '{}' was closed!\n".format(
                    DB_FILE
                )
            )

if __name__ == "__main__":
    delete_records(PEDIDOS_ONE_ROW, size="one", table="pedidos")
    delete_records(PEDIDOS_MULTIPLE_ROWS, size="many", table="pedidos")
    delete_records(size="all", table="pedidos")
    delete_records(CLIENTES_ONE_ROW, size="one", table="clientes")
    delete_records(CLIENTES_MULTIPLE_ROWS, size="many", table="clientes")
    delete_records(size="all", table="clientes")
    delete_records(PRODUCTOS_ONE_ROW, size="one", table="productos")
    delete_records(PRODUCTOS_MULTIPLE_ROWS, size="many", table="productos")
    delete_records(size="all", table="productos")
    delete_records(CATEGORIAS_ONE_ROW, size="one", table="categorias")
    delete_records(CATEGORIAS_MULTIPLE_ROWS, size="many", table="categorias")
    delete_records(size="all", table="categorias")
    delete_records(CIUDADES_ONE_ROW, size="one", table="ciudades")
    delete_records(CIUDADES_MULTIPLE_ROWS, size="many", table="ciudades")
    delete_records(size="all", table="ciudades")
    delete_records(ESTADOS_ONE_ROW, size="one", table="estados")
    delete_records(ESTADOS_MULTIPLE_ROWS, size="many", table="estados")
    delete_records(size="all", table="estados")
