# SQLAlchemy Examples

Examples for [Microsoft Access](https://en.wikipedia.org/wiki/Microsoft_Access) databases with [sqlalchemy](https://www.sqlalchemy.org) library in Python.

## Install

```console
virtualenv venv
source venv/bin/activate
cd sqlalchemy
pip install -r requirements/sqlalchemy.txt
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

```console
python insert_records.py
```

#### Insert fake records

For generate the fake records use the [Faker](https://pypi.org/project/Faker/) library.

This script have two parameters to execute it:

- **table_name**, the name of table to insert records.

- **num_records**, records number to generate and insert into the table.

```console
python populate_tables.py table_name num_records
```

The following it is possible use to generate 10 records for  the products, customers and orders tables.

```console
python populate_tables.py categories 10
python populate_tables.py products 10
python populate_tables.py customers 10
python populate_tables.py orders 10
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
