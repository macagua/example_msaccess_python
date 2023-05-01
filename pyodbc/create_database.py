"""Program to create the database"""

import logging
import msaccessdb
import os
import pyodbc


# logging INFO object
logging.basicConfig(level=logging.INFO)

# Define full path for database file
DB_DRIVER = "{Microsoft Access Driver (*.mdb, *.accdb)}"
DB_PATH = os.path.dirname(
    os.path.abspath(__file__)
) + os.sep + "data" + os.sep
DB_FILE = 'database.accdb'
DB = DB_PATH + DB_FILE

# Create Microsoft Access file
msaccessdb.create(DB)
print()
logging.info(f"Created the Microsoft Access file as '{DB_FILE}'!\n")

# Make DNS string
CONNECTION_STRING = (
    r'DRIVER={0};'
    r'DBQ={1};'
    'ExtendedAnsiSQL=1;'
).format(DB_DRIVER, DB)

# Set up connections between pyodbc and microsoft access
connection = pyodbc.connect(CONNECTION_STRING)
logging.info(f"Connected to Microsoft Access database file called '{DB_FILE}'!\n")

if connection:
    connection.close()
    logging.info(f"The Microsoft Access connection to database '{DB_FILE}' was closed!")
