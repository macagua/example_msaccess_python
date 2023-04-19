import msaccessdb
import os
import pyodbc
import urllib
from sqlalchemy import create_engine


# Define full path for database file
DB_DRIVER = "{Microsoft Access Driver (*.mdb, *.accdb)}"
DB_PATH = os.path.dirname(
    os.path.abspath(__file__)
) + os.sep + "data" + os.sep
DB_FILE = 'sistema_fake.accdb'
DB = DB_PATH + DB_FILE

# Create Microsoft Access file
msaccessdb.create(DB)
print(f"{DB_FILE} database successfully created")

# Make DNS string
CONNECTION_STRING = (
    r'DRIVER={0};'
    r'DBQ={1};'
    'ExtendedAnsiSQL=1;'
).format(DB_DRIVER, DB)

# Set up connections between sqlalchemy and access+pyodbc
# Instantiate metadate object
engine = create_engine(f"access+pyodbc:///?odbc_connect={urllib.parse.quote(CONNECTION_STRING)}")

try:
    with engine.connect() as connection:
        print(f"Connection to the {DB} database file created successfully.")
except Exception as ex:
    print("Connection could not be made due to the following error: ", ex)
    connection.rollback()
    raise
finally:
    engine.dispose()
