# SQLAlchemy Examples

Examples for [Microsoft Access](https://en.wikipedia.org/wiki/Microsoft_Access) databases with [sqlalchemy](https://www.sqlalchemy.org) library in Python.

## Install

```console
virtualenv venv
source venv/bin/activate
cd sqlalchemy
pip install -r requirements.txt
```

## Run Exmaples

### Create database file

```console
python create_database.py
```

### Create structures (tables)

```console
python create_structures.py
```

### Insert records

This script have two parameters to execute it:

- **table_name**, the name of table to insert records.

- **num_records**, records number to generate and insert into the table.

```console
python insert_records.py table_name num_records
```

#### Insert default records

```console
python insert_records.py estados 0
python insert_records.py estados 0
python insert_records.py ciudades 0
python insert_records.py categorias 0
python insert_records.py productos 0
python insert_records.py clientes 0
python insert_records.py pedidos 0
```

#### Insert Faker records

```console
python insert_records.py productos 10
python insert_records.py clientes 10
python insert_records.py pedidos 10
```

### Select records

```console
python select_records.py
```

### Update records

```console
python update_records.py
```

### Delete records

```console
python delete_records.py
```

### Delete structures (tables)

```console
python delete_structures.py
```

### Delete database file

```console
python delete_database.py
```
