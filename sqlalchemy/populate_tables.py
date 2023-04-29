"""Program to perform the populate (insertion) of the record(s) of the tables using fake data generated

Source:
    https://python.plainenglish.io/generating-a-fake-database-with-python-8523bf6db9ec
    https://seraph13.medium.com/generar-datos-falsos-con-faker-4d4313ff8205
"""

import faker_commerce
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
    # ('es_AR', 1),
    # ('es_CL', 2),
    # ('es_MX', 3)
    ('es_MX', 1)
])
# Instantiate faker object
fake = Faker(locales)
fake.add_provider(faker_commerce.Provider)

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

# Set up connections between sqlalchemy and access+pyodbc
# Instantiate metadate object
engine = create_engine(f"access+pyodbc:///?odbc_connect={urllib.parse.quote(CONNECTION_STRING)}")
metadata = MetaData()

# Reflect metadata/schema from existing database to bring in existing tables
with engine.connect() as conn:
    metadata.reflect(conn)

#states = metadata.tables["states"]
cities = metadata.tables["cities"]
categories = metadata.tables["categories"]
products = metadata.tables["products"]
customers = metadata.tables["customers"]
orders = metadata.tables["orders"]


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

        if self.table_name == "categories" and self.num_records > 0:
            with engine.begin() as conn:
                for _ in range(self.num_records):
                    statement = categories.insert().values(
                        id=int(
                            conn.execute(
                                categories.select().order_by(
                                    desc(categories.c.id)
                                )
                            ).first()[0]
                        ) + 1,
                        name=fake.ecommerce_category(),
                        status=random.choice([True, False]),
                    )
                    conn.execute(statement)
                conn.commit()
                print()
                logging.info(f"'{self.num_records}' record(s) were successfully inserted into the '{self.table_name}' table!")
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
                        name=" ".join(
                            self.capitalize_list(
                                list(
                                    fake.words()
                                )
                            )
                        ),
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
                conn.commit()
                print()
                logging.info(f"'{self.num_records}' record(s) were successfully inserted into the '{self.table_name}' table!")
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
                conn.commit()
                print()
                logging.info(f"'{self.num_records}' record(s) were successfully inserted into the '{self.table_name}' table!")
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
                conn.commit()
                print()
                logging.info(f"'{self.num_records}' record(s) were successfully inserted into the '{self.table_name}' table!")
        else:
            pass

if __name__ == "__main__":
    table_name = ""
    num_records = 0

    if len(sys.argv) == 3:
        if sys.argv[1]:
            table_name = sys.argv[1]
        if int(sys.argv[2]) > 0:
            num_records = int(sys.argv[2])
        else:
            num_records = 0

    if table_name in ['categories', 'products', 'customers', 'orders']:
        generate_data = GenerateData(table_name, num_records)
        generate_data.create_data()
    else:
        print("The table don't supported for the fake data generation!")
