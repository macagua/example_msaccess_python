""" Program to delete the database file """

import os


DB_PATH = os.path.dirname(
    os.path.abspath(__file__)
) + os.sep + "data" + os.sep
DB_FILE = 'database.accdb'
DB = DB_PATH + DB_FILE

# Delete Microsoft Access file
if os.path.exists(DB):
    print("\nExists the Microsoft Access database file!\n")
    os.remove(DB)
    print("\nDelete Microsoft Access database file!\n")
else:
    print("\nDon't exists the Microsoft Access database file!\n")
