""" Program to delete the database file """

import os


DB_PATH = os.path.dirname(
    os.path.abspath(__file__)
) + os.sep + "data" + os.sep
DB_FILE = 'database.accdb'
DB = DB_PATH + DB_FILE

# Delete Microsoft Access file
if os.path.exists(DB):
    print(f"\nExists the '{DB_FILE}' database file!")
    os.remove(DB)
    print(f"Deleted the '{DB_FILE}' database file!")
else:
    print(f"\nDon't exists the '{DB_FILE}' database file!\n")
