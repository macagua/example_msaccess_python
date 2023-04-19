# pyODBC Examples

Examples for Microsoft Access databases with [pyODBC](https://pypi.org/project/pyodbc/) library in Python.

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

- Insert rows.

  ```console
  cd pyodbc
  python insert_rows.py
  cd ..
  ```

- Select rows.

  ```console
  cd pyodbc
  python select_rows.py
  cd ..
  ```

- Update rows.

  ```console
  cd pyodbc
  python update_rows.py
  cd ..
  ```

- Delete rows.

  ```console
  cd pyodbc
  python delete_rows.py
  cd ..
  ```

- Delete database file.

  ```console
  cd pyodbc
  python delete_database.py
  cd ..
  ```
