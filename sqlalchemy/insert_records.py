"""Program to perform the insertion of the record(s) of the table"""

import csv
import json
import logging
import os
import pyodbc
import urllib
import random
import sys
from collections import OrderedDict
from sqlalchemy import create_engine, desc, MetaData,\
    insert, select


# logging INFO object
logging.basicConfig(level=logging.INFO)

# Define full path for database file
DB_DRIVER = "{Microsoft Access Driver (*.mdb, *.accdb)}"
DB_PATH = os.path.dirname(
    os.path.abspath(__file__)
) + os.sep + "data" + os.sep
DB_FILE = 'database.accdb'
STATES_FILE = 'states.json'
CITIES_FILE = 'cities.csv'
DB = DB_PATH + DB_FILE
STATES = DB_PATH + STATES_FILE
CITIES = DB_PATH + CITIES_FILE

# Make DNS string
CONNECTION_STRING = (
    r'DRIVER={0};'
    r'DBQ={1};'
    'ExtendedAnsiSQL=1;'
).format(DB_DRIVER, DB)

# Set up connections between sqlalchemy and access+pyodbc
# Instantiate metadate object
engine = create_engine(f"access+pyodbc:///?odbc_connect={urllib.parse.quote(CONNECTION_STRING)}")
metadata = MetaData()

# Reflect metadata/schema from existing database to bring in existing tables
with engine.connect() as conn:
    metadata.reflect(conn)

states = metadata.tables["states"]
cities = metadata.tables["cities"]
categories = metadata.tables["categories"]
products = metadata.tables["products"]
customers = metadata.tables["customers"]
orders = metadata.tables["orders"]

# 'categories' list
CATEGORIES_MULTIPLE_ROWS = [
    (1, 'Tecnología', True),
    (2, 'Ropa', False),
    (3, 'Estética', True),
    (4, 'Herramientas', True),
    (5, 'Entretenimientos', True)
]

# 'products' list
PRODUCTS_MULTIPLE_ROWS = [
    (1, "Pantalón Jean LEVI'S 511", "Pantalón Jean LEVI'S 511 Slim Fit, Talla 34x32 y 34x34, Color disponible Pumped Up, 99% Algodón y 1% Elastane, Hecho en Bangladesh. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.", 2, 59.33, True),
    (2, 'Teléfono iPhone 13 Pro Max', 'Producto 2 detallado', 1, 1045.56, False),
    (3, 'Consola Play Station 5', 'Consola Play Station 5; Edición Gob Of War; Capacidad 825 GB; Memoria RAM de 16 GB; Tipo de consola de sobremesa, Wi-FI incluido; cantidad de controles incluidos 1. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.', 5, 829.00, True),
    (4, 'Caja de herramientas Stanley 99 piezas', 'Caja de herramientas Stanley 99 piezas; Caja de herramientas Stanley, Cantidad de piezas incluidas 99 como dados, extensiones, destornillador y llaves. Producto 100% original, Producto importado de EE.UU. Envíos a todo el país.', 4, 180.30, True),
    (5, 'Zapatos Clarks', 'Zapatos Clarks; Talla 39, 40 y 41; Colores disponible  Negro, Marrón, Azul; 99% Cuero 1% Tela, Hecho en Londres. Producto 100% original, Producto importado de Inglaterra. Envíos a todo el país.', 2, 120.00, True)
]

# 'customers' list
CUSTOMERS_MULTIPLE_ROWS = [
    (1, 'Leonardo', 'Caballero', 245, '04144567239'),
    (2, 'Ana', 'Poleo', 6, '04249804536'),
    (3, 'Rafa', 'Lugo', 461, '04121894605'),
    (4, 'Eduardo', 'Perez', 487, '04163387633'),
    (5, 'Maximiliano', 'Vilchez', 245, '04264893321')
]

# 'orders' list
ORDERS_MULTIPLE_ROWS = [
    (1, 1, '12/02/2022 10:23:45 AM', 2, True),
    (2, 3, '01/06/2023 03:55:51 PM', 1, True),
    (3, 4, '02/18/2023 12:48:33 AM', 2, True),
    (4, 5, '03/22/2023 07:22:03 PM', 1, True),
    (5, 2, '03/12/2023 08:26:54 PM', 3, True)
]


class GenerateData:
    """Generate a specific number of records to a target table in
    the database"""

    def __init__(self, table=""):
        """Initialize command line arguments

        Args:
            table (str, optional): The table name to manipulate. Defaults to "".
        """
        self.table_name = table


    def __str__(self):
        """Informal Representation Class"""
        return f"Table: '{self.table_name}'."


    def __repr__(self):
        """Official Representation Class"""
        return f"Table: '{self.table_name}'."


    def create_data(self):
        """Using csv, and json libraries, generate data and execute DML"""

        if self.table_name not in metadata.tables.keys():
            return print(f"\nThe '{self.table_name}' table does not exist!")

        if self.table_name == "states":
            with engine.begin() as conn:

                with open(STATES, 'r', encoding='utf-8') as json_file:
                    rows = json.load(json_file)
                    records_total = 0
                    for row in rows:
                        statement = states.insert().values(
                            id=row['id'],
                            name=row['name'],
                            iso_3166_2=row['iso_3166_2'],
                        )
                        conn.execute(statement)
                        records_total = records_total + 1
                conn.commit()
                print()
                logging.info(f"'{records_total}' record(s) were successfully inserted into the '{self.table_name}' table!")
        elif self.table_name == "cities":
            with engine.begin() as conn:

                with open(CITIES, 'r', encoding='utf-8') as csv_file:
                    rows = csv.reader(csv_file, delimiter=',')
                    next(rows)
                    records_total = 0

                    for row in rows:
                        statement = cities.insert().values(
                            id=row[0],
                            state_id=row[1],
                            name=row[2],
                            capital=row[3],
                        )
                        conn.execute(statement)
                conn.commit()
                records_total = records_total + 1
                logging.info(f"'{records_total}' record(s) were successfully inserted into the '{self.table_name}' table!")
        elif self.table_name == "categories":
            with engine.begin() as conn:
                for fila in CATEGORIES_MULTIPLE_ROWS:
                    statement = categories.insert().values(
                        id=fila[0],
                        name=fila[1],
                        status=fila[2],
                    )
                    conn.execute(statement)
                conn.commit()
                logging.info(f"'{len(CATEGORIES_MULTIPLE_ROWS)}' record(s) were successfully inserted into the '{self.table_name}' table!")
        elif self.table_name == "products":
            with engine.begin() as conn:
                for fila in PRODUCTS_MULTIPLE_ROWS:
                    statement = products.insert().values(
                        id=fila[0],
                        name=fila[1],
                        description=fila[2],
                        category_id=fila[3],
                        price=fila[4],
                        status=fila[5],
                    )
                    conn.execute(statement)
                conn.commit()
                logging.info(f"'{len(PRODUCTS_MULTIPLE_ROWS)}' record(s) were successfully inserted into the '{self.table_name}' table!")
        elif self.table_name == "customers":
            with engine.begin() as conn:
                for fila in CUSTOMERS_MULTIPLE_ROWS:
                    statement = customers.insert().values(
                        id=fila[0],
                        name=fila[1],
                        lastname=fila[2],
                        zip_code=fila[3],
                        phone=fila[4],
                    )
                    conn.execute(statement)
                conn.commit()
                logging.info(f"'{len(CUSTOMERS_MULTIPLE_ROWS)}' record(s) were successfully inserted into the '{self.table_name}' table!")
        elif self.table_name == "orders":
            with engine.begin() as conn:
                for fila in ORDERS_MULTIPLE_ROWS:
                    statement = orders.insert().values(
                        id=fila[0],
                        customer_id=fila[1],
                        made_at=fila[2],
                        product_id=fila[3],
                        status=fila[4],
                    )
                    conn.execute(statement)
                conn.commit()
                # print()
                logging.info(f"'{len(ORDERS_MULTIPLE_ROWS)}' record(s) were successfully inserted into the '{self.table_name}' table!")
        else:
            pass

if __name__ == "__main__":
    generate_data0 = GenerateData(table="states")
    generate_data0.create_data()
    generate_data1 = GenerateData(table="cities")
    generate_data1.create_data()
    generate_data2 = GenerateData(table="categories")
    generate_data2.create_data()
    generate_data3 = GenerateData(table="products")
    generate_data3.create_data()
    generate_data4 = GenerateData(table="customers")
    generate_data4.create_data()
    generate_data5 = GenerateData(table="orders")
    generate_data5.create_data()
    generate_data5 = GenerateData(table="profile")
    generate_data5.create_data()
