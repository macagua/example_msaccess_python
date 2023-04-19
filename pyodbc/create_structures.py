""" Program to create the database tables """

import os
import pyodbc


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
).format(DB_DRIVER, DB)

connection = pyodbc.connect(CONNECTION_STRING)
print("\nConnected to Microsoft Access database!\n")
cursor = connection.cursor()

# If The 'estados' Table exists!
if cursor.tables(table="estados").fetchone():
    print("The 'estados' Table exists!\n")
else:
    print("The 'estados' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    cursor.execute("""
        CREATE TABLE estados (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,nombre VARCHAR(25) NOT NULL
                ,iso_3166_2 VARCHAR(4) NOT NULL
        );
    """)
    connection.commit()
    print("The 'estados' Table Created!\n")

# If The 'ciudades' Table exists!
if cursor.tables(table="ciudades").fetchone():
    print("The 'ciudades' Table exists!\n")
else:
    print("The 'ciudades' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    cursor.execute("""
        CREATE TABLE ciudades (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,estado_id INTEGER NOT NULL
                ,nombre VARCHAR(200) NOT NULL
                ,capital SMALLINT NOT NULL
                ,CONSTRAINT [fk_estado_id] FOREIGN KEY (estado_id) REFERENCES estados(id)
        );
    """)
    connection.commit()
    print("The 'ciudades' Table Created!\n")

# If The 'categorias' Table exists!
if cursor.tables(table="categorias").fetchone():
    print("The 'categorias' Table exists!\n")
else:
    print("The 'categorias' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    cursor.execute("""
        CREATE TABLE categorias (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,nombre VARCHAR(25) NOT NULL
                ,status BIT NOT NULL
        );
    """)
    connection.commit()
    print("The 'categorias' Table Created!\n")

# If The 'productos' Table exists!
if cursor.tables(table="productos").fetchone():
    print("The 'productos' Table exists!\n")
else:
    print("The 'productos' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    cursor.execute("""
        CREATE TABLE productos (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,nombre VARCHAR(50) NOT NULL
                ,descripcion VARCHAR(250) NOT NULL
                ,categoria_id INTEGER NOT NULL
                ,precio SINGLE NOT NULL
                ,status BIT NOT NULL
                ,CONSTRAINT [fk_categoria_id] FOREIGN KEY (categoria_id) REFERENCES categorias(id)
        );
    """)
    connection.commit()
    print("The 'productos' Table Created!\n")

# If The 'clientes' Table exists!
if cursor.tables(table="clientes").fetchone():
    print("The 'clientes' Table exists!\n")
else:
    print("The 'clientes' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    cursor.execute("""
        CREATE TABLE clientes (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,nombre VARCHAR(25) NOT NULL
                ,apellido VARCHAR(25) NOT NULL
                ,codigo_postal INTEGER NOT NULL
                ,telefono VARCHAR(11) NOT NULL
                ,CONSTRAINT [fk_ciudad_id] FOREIGN KEY (codigo_postal) REFERENCES ciudades(id)
        );
    """)
    connection.commit()
    print("The 'clientes' Table Created!\n")

# If The 'pedidos' Table exists!
if cursor.tables(table="pedidos").fetchone():
    print("The 'pedidos' Table exists!\n")
else:
    print("The 'pedidos' Table don't exists!\n")
    # Run CREATE TABLE SQL statement
    cursor.execute("""
        CREATE TABLE pedidos (
                id AUTOINCREMENT PRIMARY KEY NOT NULL
                ,cliente_id INTEGER NOT NULL
                ,fecha DATETIME NOT NULL
                ,producto_id INTEGER NOT NULL
                ,status BIT NOT NULL
                ,CONSTRAINT [fk_cliente_id] FOREIGN KEY (cliente_id) REFERENCES clientes(id)
                ,CONSTRAINT [fk_producto_id] FOREIGN KEY (producto_id) REFERENCES productos(id)
        );
    """)
    connection.commit()
    print("The 'pedidos' Table Created!\n")


if connection:
    connection.close()
    print(f"The Microsoft Access connection to database '{DB_FILE}' was closed!")
