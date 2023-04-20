import os
import pyodbc
import urllib
from sqlalchemy import create_engine, MetaData, \
    Column, Boolean, Integer, Numeric, SmallInteger, \
    String, Date, Table, ForeignKey 


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

# DDL for estados, ciudades, categorias, productos, clientes and pedidos 
estados_tbl = Table(
    "estados",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("nombre", String(25), nullable=False),
    Column("iso_3166_2", String(4), nullable=False),
)

ciudades_tbl = Table(
    "ciudades",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("estado_id", ForeignKey("estados.id"), nullable=False),
    Column("nombre", String(200), nullable=False),
    Column("capital", SmallInteger, nullable=False)
)

categorias_tbl = Table(
    "categorias",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("nombre", String(25), nullable=True),
    Column("status", Boolean, nullable=True)
)

productos_tbl = Table(
    "productos",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("nombre", String(50), nullable=True),
    Column("descripcion", String(250), nullable=True),
    Column("categoria_id", ForeignKey("categorias.id"), nullable=False),
    Column("precio", Numeric(10,2), nullable=False),
    Column("status", Boolean, nullable=True)
)

clientes_tbl = Table(
    "clientes",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("nombre", String(25), nullable=True),
    Column("apellido", String(25), nullable=True),
    Column("codigo_postal", ForeignKey("ciudades.id"), nullable=False),
    Column("telefono", String(20), nullable=True)
)

pedidos_tbl = Table(
    "pedidos",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("cliente_id", ForeignKey("clientes.id"), nullable=False),
    Column("fecha", Date, nullable=False),
    Column("producto_id", ForeignKey("productos.id"), nullable=False),
    Column("status", Boolean, nullable=True)
)

# Get all of the tables
tables = metadata.tables.keys()

# Start transaction to commit DDL to database
with engine.begin() as conn:
    metadata.create_all(conn)
    print()

    for table in tables:
        print(f"the '{table}' table successfully created!")

print(f"'{len(tables)}' table(s) successfully created!")
