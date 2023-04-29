"""Program to perform the insertion of the record(s) of the table

Source:
    https://python.plainenglish.io/generating-a-fake-database-with-python-8523bf6db9ec
    https://seraph13.medium.com/generar-datos-falsos-con-faker-4d4313ff8205
"""

import csv
import json
import os
import logging
import pyodbc
import urllib
import random
import sys
from collections import OrderedDict
from faker import Faker
from sqlalchemy import create_engine, desc, MetaData,\
    insert, select

# logging INFO object
logging.basicConfig(level=logging.INFO)


# locales faker object
locales = OrderedDict([
    ('es_AR', 1),
    ('es_CL', 2),
    ('es_MX', 3)
])
# Instantiate faker object
fake = Faker(locales)

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

    def __init__(self, table="", numbers=0):
        """Initialize command line arguments

        Args:
            table (str, optional): The table name to manipulate. Defaults to "".
            numbers (int, optional): The rows number to insert into the table. Defaults to 0.
        """
        self.table_name = table
        self.num_records = numbers


    def __str__(self):
        """Informal Representation Class"""
        return f"Table: '{self.table_name}' and Number Records: '{self.num_records}'."


    def __repr__(self):
        """Official Representation Class"""
        return f"Table: '{self.table_name}'."

    def capitalize_list(self, list=[]):
        """Capitalize the values of a list

        Args:
            list (list, optional): A list values for Capitalize. Defaults to [].

        Returns:
            list: A list values capitalized
        """
        return [l.capitalize() for l in list]


    def create_data(self):
        """Using faker library, generate data and execute DML"""

        if self.table_name not in metadata.tables.keys():
            return print(f"{self.table_name} does not exist")

        if self.table_name == "states" and self.num_records == 0:
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
                logging.info(f"'{records_total}' record(s) were successfully inserted into the table!")
        else:
            pass

        if self.table_name == "cities" and self.num_records == 0:
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
                        records_total = records_total + 1
                print()
                logging.info(f"'{records_total}' record(s) were successfully inserted into the table!")
        else:
            pass

        if self.table_name == "categories" and self.num_records == 0:
            with engine.begin() as conn:
                for fila in CATEGORIES_MULTIPLE_ROWS:
                    statement = categories.insert().values(
                        id=fila[0],
                        name=fila[1],
                        status=fila[2],
                    )
                    conn.execute(statement)
                print(f"\n'{len(CATEGORIES_MULTIPLE_ROWS)}' row(s) inserted into '{self.table_name}' table!")
        else:
            pass

        if self.table_name == "products" and self.num_records == 0:
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
                print(f"\n'{len(PRODUCTS_MULTIPLE_ROWS)}' row(s) inserted into '{self.table_name}' table!")
        elif self.table_name == "products" and self.num_records > 0:
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    statement = products.insert().values(
                        id=int(
                            conn.execute(
                                products.select().order_by(
                                    desc(products.c.id)
                                )
                            ).first()[0]
                        ) + 1,
                        name=" ".join(self.capitalize_list(list(fake.words()))),
                        description=fake.sentence(nb_words=10),
                        category_id=random.choice(
                            conn.execute(
                                categories.select()
                            ).fetchall()
                        )[0],
                        price=fake.random_int(1,100000) / 100.0,
                        status=random.choice([True, False]),
                    )
                    conn.execute(statement)
                print(f"\n'{self.num_records}' row(s) inserted into '{self.table_name}' table!")
        else:
            pass

        if self.table_name == "customers" and self.num_records == 0:
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
                print(f"\n'{len(CUSTOMERS_MULTIPLE_ROWS)}' row(s) inserted into '{self.table_name}' table!")
        elif self.table_name == "customers" and self.num_records > 0:
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    statement = customers.insert().values(
                        id=int(
                            conn.execute(
                                customers.select().order_by(
                                    desc(customers.c.id)
                                )
                            ).first()[0]
                        ) + 1,
                        name=fake.first_name(),
                        lastname=fake.last_name(),
                        zip_code=random.choice(
                            conn.execute(
                                cities.select()
                            ).fetchall()
                        )[0],
                        phone=fake.unique.phone_number(),
                    )
                    conn.execute(statement)
                print(f"\n'{self.num_records}' row(s) inserted into '{self.table_name}' table!")
        else:
            pass

        if self.table_name == "orders" and self.num_records == 0:
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
                print(f"\n'{len(ORDERS_MULTIPLE_ROWS)}' row(s) inserted into '{self.table_name}' table!")
        elif self.table_name == "orders" and self.num_records > 0:
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    statement = orders.insert().values(
                        id=int(
                            conn.execute(
                                orders.select().order_by(
                                    desc(orders.c.id)
                                )
                            ).first()[0]
                        ) + 1,
                        customer_id=random.choice(
                            conn.execute(
                                customers.select()
                            ).fetchall()
                        )[0],
                        made_at=fake.date_time().strftime("%Y/%m/%d %H:%M:%S %p"),
                        product_id=random.choice(
                            conn.execute(
                                products.select()
                            ).fetchall()
                        )[0],
                        status=fake.pybool(),
                    )
                    conn.execute(statement)
                print(f"\n'{self.num_records}' row(s) inserted into '{self.table_name}' table!")
        else:
            pass


if __name__ == "__main__":
    table_name = ""
    num_records = 0

    if len(sys.argv) == 2:
        if sys.argv[1]:
            table_name = sys.argv[1]
            num_records = 0

    if len(sys.argv) == 3:
        if sys.argv[1]:
            table_name = sys.argv[1]
        if int(sys.argv[2]) > 0:
            num_records = int(sys.argv[2])
        else:
            num_records = 0

    if table_name in ['states', 'cities', 'categories']:
        generate_data = GenerateData(table_name)
        generate_data.create_data()
    else:
        generate_data = GenerateData(table_name, num_records)
        generate_data.create_data()
