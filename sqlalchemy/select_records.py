"""Program to query the record(s) of the table"""

import os
import pyodbc
import urllib
from sqlalchemy import and_, case, cast, create_engine,\
    desc, func, select, text, Float, MetaData


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

states = metadata.tables["states"]
cities = metadata.tables["cities"]
categories = metadata.tables["categories"]
products = metadata.tables["products"]
customers = metadata.tables["customers"]
orders = metadata.tables["orders"]

'''
SELECT name, price, status
FROM products
WHERE status = True
AND price >= 150
'''
with engine.begin() as conn:
    statement = text("""
        SELECT name, price, status
        FROM products
        WHERE status = :status
        AND price >= :price
        """)
    args = {
        'status': True,
        'price': 150,
    }
    print(str(statement) + "\n\t" + str(args) + "\n")
    result = conn.execute(statement, args)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("Records from 'products' table!")
    print("----")

'''
SELECT * FROM states
'''
with engine.begin() as conn:
    statement = states.select()
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchone()
    print(rows)
    print("Only '1' record from 'states' table!\n")

    rows = result.fetchmany(5)
    for row in rows:
        print(row)
    print("Only '5' records' from 'states' table!\n")

    rows = result.fetchall()
    for row in rows:
        print(row)
    print("All records from 'states' table!")
    print("----")

'''
SELECT * FROM cities
WHERE state_id = 13
'''
with engine.begin() as conn:
    statement = cities.select().where(cities.columns.state_id == '13')
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")
    print("All records with 'state_id' iqual to '13' from 'cities' table!")

'''
SELECT name, iso_3166_2
FROM states
WHERE name IN (Mérida, Zulia)
'''
with engine.begin() as conn:
    statement = select(
        states.columns.name,
        states.columns.iso_3166_2
        ).where(
        states.c.name.in_([
            'Mérida',
            'Zulia'
        ])
    )
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")
    print("All records with 'name' be 'Mérida' and 'Zulia' from 'states' table!")

'''
SELECT * FROM cities
WHERE state_id = 23 AND NOT capital = 1
'''
with engine.begin() as conn:
    statement = cities.select().where(
        and_(
            cities.columns.state_id == 23,
            cities.columns.capital != 1
        )
    )
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")
    print("All records with 'state' iqual to 'Zulia' and be a 'capital' from 'cities' table!")

'''
SELECT * FROM categories
ORDER BY name DESC, status
'''
with engine.begin() as conn:
    statement = categories.select().order_by(
        desc(categories.columns.name),
        categories.columns.status
    )
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")

'''
SELECT
    COUNT(price) AS 'Quantity_prices',
    AVG(price) AS 'Average_prices',
    SUM(price) AS 'Total_prices',
    MIN(price) AS 'Minimum_price',
    MAX(price) AS 'Maximum_price',
    NOW() AS 'Date_time_current'
FROM products
'''
with engine.begin() as conn:
    statement = select(
        func.count(products.columns.price).label('Quantity_prices'),
        func.avg(products.columns.price).label('Average_prices'),
        func.sum(products.columns.price).label('Total_prices'),
        func.min(products.columns.price).label('Minimum_price'),
        func.max(products.columns.price).label('Maximum_price'),
        func.now().label('Date_time_current')
    )
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")

'''
SELECT SUM(price) as price, status
FROM products
'''
with engine.begin() as conn:
    statement = select(
        func.sum(products.columns.price).label('price'),
        products.columns.status
    ).group_by(
        products.columns.status
    )
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")
