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

estados = metadata.tables["estados"]
ciudades = metadata.tables["ciudades"]
categorias = metadata.tables["categorias"]
productos = metadata.tables["productos"]
clientes = metadata.tables["clientes"]
pedidos = metadata.tables["pedidos"]

'''
SELECT nombre, precio, status
FROM productos
WHERE status = True
AND precio >= 150
'''
with engine.begin() as conn:
    statement = text("""
        SELECT nombre, precio, status
        FROM productos
        WHERE status = :status
        AND precio >= :precio
        """)
    args = {
        'status': True,
        'precio': 150,
    }
    print(str(statement) + "\n\t" + str(args) + "\n")
    result = conn.execute(statement, args)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("Records from 'productos' table!")
    print("----")

'''
SELECT * FROM estados
'''
with engine.begin() as conn:
    statement = estados.select()
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchone()
    print(rows)
    print("Only '1' record from 'estados' table!\n")

    rows = result.fetchmany(5)
    for row in rows:
        print(row)
    print("Only '5' records' from 'estados' table!\n")

    rows = result.fetchall()
    for row in rows:
        print(row)
    print("All records from 'estados' table!")
    print("----")

'''
SELECT * FROM cuidades
WHERE estado_id = 13
'''
with engine.begin() as conn:
    statement = ciudades.select().where(ciudades.columns.estado_id == '13')
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")
    print("All records with 'estado_id' iqual to '13' from 'ciudades' table!")

'''
SELECT nombre, iso_3166_2
FROM estados
WHERE nombre IN (Mérida, Zulia)
'''
with engine.begin() as conn:
    statement = select(
        estados.columns.nombre,
        estados.columns.iso_3166_2
        ).where(
        estados.c.nombre.in_([
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
    print("All records with 'nombre' be 'Mérida' and 'Zulia' from 'estados' table!")

'''
SELECT * FROM ciudades
WHERE estado_id = 23 AND NOT capital = 1
'''
with engine.begin() as conn:
    statement = ciudades.select().where(
        and_(
            ciudades.columns.estado_id == 23,
            ciudades.columns.capital != 1
        )
    )
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")
    print("All records with 'estado' iqual to 'Zulia' and be a 'capital' from 'ciudades' table!")

'''
SELECT * FROM categorias
ORDER BY nombre DESC, status
'''
with engine.begin() as conn:
    statement = categorias.select().order_by(
        desc(categorias.columns.nombre),
        categorias.columns.status
    )
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")

'''
SELECT
    COUNT(precio) AS 'Cantidad_precios',
    AVG(precio) AS 'Average_precios',
    SUM(precio) AS 'Total_precios',
    MIN(precio) AS 'Minimo_precio',
    MAX(precio) AS 'Maximo_precio',
    NOW() AS 'Fecha_hora_actual'
FROM productos
'''
with engine.begin() as conn:
    statement = select(
        func.count(productos.columns.precio).label('Cantidad_precios'),
        func.avg(productos.columns.precio).label('Average_precios'),
        func.sum(productos.columns.precio).label('Total_precios'),
        func.min(productos.columns.precio).label('Minimo_precio'),
        func.max(productos.columns.precio).label('Maximo_precio'),
        func.now().label('Fecha_hora_actual')
    )
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")

'''
SELECT SUM(precio) as precio, status
FROM productos
'''
with engine.begin() as conn:
    statement = select(
        func.sum(productos.columns.precio).label('precio'),
        productos.columns.status
    ).group_by(
        productos.columns.status
    )
    print("\n" + str(statement) + "\n")
    result = conn.execute(statement)
    rows = result.fetchall()
    for row in rows:
        print(row)
    print("----")
