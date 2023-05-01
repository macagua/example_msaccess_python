"""Program to update the record(s) of the table"""

import logging
import os
import pyodbc
import urllib
from sqlalchemy import create_engine, bindparam, exc, func, text, MetaData

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

STATES_VALUES = {
    "name": bindparam('name'),
}
STATES_ONE_ROW = [
    {"name": "MÉRIDA", "_id": 13},
]
STATES_MULTIPLE_ROWS = [
    {"name": "ANZOÁTEQUI", "_id": 2},
    {"name": "LARA", "_id": 12},
    {"name": "ZULIA", "_id": 23},
]

CITIES_VALUES = {
    "name": bindparam('name'),
}
CITIES_ONE_ROW = [
    {"name": "MÉRIDA", "_id": 13},
]
CITIES_MULTIPLE_ROWS = [
    {"name": "BARCELONA", "_id": 2},
    {"name": "BACHAQUERO", "_id": 461},
    {"name": "MARACAIBO", "_id": 487},
]

CATEGORIES_VALUES = {
    "name": bindparam('name'),
}
# 'categories' list
CATEGORIES_ONE_ROW = [
    {"name": "TECNOLOGÍA", "_id": 1},
]
CATEGORIES_MULTIPLE_ROWS = [
    {"name": "ESTÉTICA", "_id": 3},
    {"name": "HERRAMIENTAS", "_id": 4},
    {"name": "ENTRETENIMIENTOS", "_id": 5},
]

# 'products' list
PRODUCTS_VALUES = {
    "description": bindparam('description'),
    "price": bindparam('price'),
}
PRODUCTS_ONE_ROW = [
    {"description": "Pantalón Jean LEVI'S 511 Slim Fit, Talla 34x32 y 34x34, Color disponible Pumped Up, 99% Algodón y 1% Elastane, Hecho en Bangladesh. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", "price": 59.33, "_id": 1},
]
PRODUCTS_MULTIPLE_ROWS = [
    {"description": "Consola Play Station 5; Edición Gob Of War; Capacidad 825 GB; Memoria RAM de 16 GB; Tipo de consola de sobremesa, Wi-FI incluido; cantidad de controles incluidos 1. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", "price": 829.00, "_id": 3},
    {"description": "Caja de herramientas Stanley 99 piezas; Caja de herramientas Stanley, Cantidad de piezas incluidas 99 como dados, extensiones, destornillador y llaves. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", "price": 180.30, "_id": 4},
    {"description": "Zapatos Clarks; Talla 39, 40 y 41; Colores disponible  Negro, Marrón, Azul; 99% Cuero 1% Tela, Hecho en Londres. Producto 100% original, Producto importado de Inglaterra. Envíos a todo el país.", "price": 120.00, "_id": 5}
]

CUSTOMERS_VALUES = {
    "name": bindparam('name'),
    "lastname": bindparam('lastname'),
}
# 'customers' list
CUSTOMERS_ONE_ROW = [
    {"name": "Leonardo", "lastname": "Caballero", "_id": 1},
]
CUSTOMERS_MULTIPLE_ROWS = [
    {"name": "Ana", "lastname": "Poleo", "_id": 3},
    {"name": "Rafael", "lastname": "Lugo", "_id": 4},
    {"name": "Maximiliano", "lastname": "Vilchez", "_id": 5},
]

ORDERS_VALUES = {
    "made_at": bindparam('made_at'),
    "status": bindparam('status'),
}
# 'orders' list
ORDERS_ONE_ROW = [
    {"made_at": "12/02/2022 11:23:34 PM", "status": False, "_id": 1},
]
ORDERS_MULTIPLE_ROWS = [
    {"made_at": "02/18/2023 10:22:33 AM", "status": False, "_id": 3},
    {"made_at": "04/22/2023 09:22:03 AM", "status": False, "_id": 4},
    {"made_at": "03/12/2023 12:26:54 AM", "status": False, "_id": 5},
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

states = metadata.tables["states"]
cities = metadata.tables["cities"]
categories = metadata.tables["categories"]
products = metadata.tables["products"]
customers = metadata.tables["customers"]
orders = metadata.tables["orders"]


def update_records(values={}, rows=[], size="all", table=""):
    """Function to perform the update of several records from the table

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
                    if table == "orders":
                        where_clause = orders.c.id == bindparam('_id')
                        statement = orders.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "customers":
                        where_clause = customers.c.id == bindparam('_id')
                        statement = customers.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "products":
                        where_clause = products.c.id == bindparam('_id')
                        statement = products.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "categories":
                        where_clause = categories.c.id == bindparam('_id')
                        statement = categories.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "cities":
                        where_clause = cities.c.id == bindparam('_id')
                        statement = cities.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "states":
                        where_clause = states.c.id == bindparam('_id')
                        statement = states.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    logging.info(f"'{result.rowcount}' row(s) updated successfully from '{table}' table!\n")

            # Update a many records
            if size == "many" and len(rows) > 0:
                if table in tables:
                    result = 0
                    if table == "orders":
                        where_clause = orders.c.id == bindparam('_id')
                        statement = orders.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "customers":
                        where_clause = customers.c.id == bindparam('_id')
                        statement = customers.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "products":
                        where_clause = products.c.id == bindparam('_id')
                        statement = products.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "categories":
                        where_clause = categories.c.id == bindparam('_id')
                        statement = categories.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "cities":
                        where_clause = cities.c.id == bindparam('_id')
                        statement = cities.update().where(where_clause).values(values)
                        print(statement, rows)
                        print()
                        result = connection.execute(statement, rows)
                    if table == "states":
                        where_clause = states.c.id == bindparam('_id')
                        statement = states.update().where(where_clause).values(values)
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
    update_records(values=ORDERS_VALUES, rows=ORDERS_ONE_ROW, size="one", table="orders")
    update_records(values=ORDERS_VALUES, rows=ORDERS_MULTIPLE_ROWS, size="many", table="orders")
    update_records(values=CUSTOMERS_VALUES, rows=CUSTOMERS_ONE_ROW, size="one", table="customers")
    update_records(values=CUSTOMERS_VALUES, rows=CUSTOMERS_MULTIPLE_ROWS, size="many", table="customers")
    update_records(values=PRODUCTS_VALUES, rows=PRODUCTS_ONE_ROW, size="one", table="products")
    update_records(values=PRODUCTS_VALUES, rows=PRODUCTS_MULTIPLE_ROWS, size="many", table="products")
    update_records(values=CATEGORIES_VALUES, rows=CATEGORIES_ONE_ROW, size="one", table="categories")
    update_records(values=CATEGORIES_VALUES, rows=CATEGORIES_MULTIPLE_ROWS, size="many", table="categories")
    update_records(values=CITIES_VALUES, rows=CITIES_ONE_ROW, size="one", table="cities")
    update_records(values=CITIES_VALUES, rows=CITIES_MULTIPLE_ROWS, size="many", table="cities")
    update_records(values=STATES_VALUES, rows=STATES_ONE_ROW, size="one", table="states")
    update_records(values=STATES_VALUES, rows=STATES_MULTIPLE_ROWS, size="many", table="states")
