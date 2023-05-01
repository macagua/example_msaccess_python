"""Program to delete tables from a database"""

import os
import pyodbc
import urllib
from sqlalchemy import create_engine, inspect, MetaData


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

# Get all of the tables
tables = inspect(engine).get_table_names()

# Reflect metadata/schema from existing database to bring in existing tables
with engine.connect() as conn:
    metadata.reflect(conn)
    metadata.drop_all(engine)
    print()

    for table in tables:
        print(f"the '{table}' table successfully deleted!")

print(f"'{len(tables)}' table(s) successfully deleted!")
