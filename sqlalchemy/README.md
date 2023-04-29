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
python insert_records.py states
python insert_records.py cities
python insert_records.py categories
python insert_records.py products
python insert_records.py customers
python insert_records.py orders
```

#### Insert fake records

For generate the fake records use the [Faker](https://pypi.org/project/Faker/) library.

```console
python insert_records.py products 10
python insert_records.py customers 10
python insert_records.py orders 10
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
