# pyODBC Examples

Examples for Microsoft Access databases with [pyODBC](https://pypi.org/project/pyodbc/) library in Python.

## Install

::

  virtualenv venv
  source venv/bin/activate
  cd pyodbc
  pip install -r requirements.txt


## Run Exmaples

- Create database file.

  ::

    cd pyodbc
    python create_database.py
    cd ..

- Create structures (tables).

  ::

    cd pyodbc
    python create_structures.py
    cd ..

- Insert rows.

  ::

    cd pyodbc
    python insert_rows.py
    cd ..

- Select rows.

  ::

    cd pyodbc
    python select_rows.py
    cd ..

- Update rows.

  ::

    cd pyodbc
    python update_rows.py
    cd ..

- Delete rows.

  ::

    cd pyodbc
    python delete_rows.py
    cd ..

- Delete database file.

  ::

    cd pyodbc
    python delete_database.py
    cd ..
