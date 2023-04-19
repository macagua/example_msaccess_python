# SQLAlchemy Examples

Examples for Microsoft Access databases with [sqlalchemy](https://www.sqlalchemy.org) library in Python.

## Install

```console
virtualenv venv
source venv/bin/activate
cd pyodbc
pip install -r requirements.txt
```

## Run Exmaples

- Create database file.

  ```console
  cd pyodbc
  python create_database.py
  cd ..
  ```

- Create structures (tables).

  ```console
  cd pyodbc
  python create_structures.py
  cd ..
  ```

- Delete database file.

  ```console
  cd pyodbc
  python delete_database.py
  cd ..
  ```
