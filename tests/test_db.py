"""
db_test.py

A module to test the data loading from JSONL files into an in-memory DuckDB database
by checking if tables exist and querying data from them.

Usage:
    python db_test.py
"""

import duckdb
import os

# Connect to the in-memory DuckDB instance where data is assumed to be loaded.
# con = duckdb.connect(database=':memory:')
con = duckdb.connect(database='duck.db')

def test_tables_exist():
    """Check if the expected tables exist in the database."""
    tables = con.execute("SHOW TABLES").fetchall()
    expected_tables = ["agency", "title", "section"]  # Matching the endpoint_names from db_loader.py

    for table in expected_tables:
        if table not in [t[0] for t in tables]:
            print(f"Error: Table '{table}' not found in the database.")
            return False
    print("All expected tables are present.")
    return True

def test_query_data():
    """Query data from each table to confirm they have data."""
    tables = ["agency", "title", "section"]
    for table in tables:
        result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        if result[0] > 0:
            print(f"Table '{table}' has {result[0]} records.")
        else:
            print(f"Warning: Table '{table}' is empty.")

import json

def test_query_data_simple():
    column_names = ["title", "chapter"]
    result = con.execute("SELECT title, chapter FROM section LIMIT 20").fetchall()
    print(json.dumps(dict(zip(column_names, result))))

def test_query_data_get_first_row():
    """Return the first row of data for each table in JSON format with indent 2."""
    # tables = ["agency", "title", "section"]
    tables = ["agency", "agency_section_ref", "section"]
    all_data = {}
    
    for table in tables:
        columns = con.execute(f"DESCRIBE {table}").fetchall()
        column_names = [col[0] for col in columns]
        result = con.execute(f"SELECT * FROM {table} LIMIT 1").fetchone()
        all_data[table] = None if result is None else dict(zip(column_names, result))

    print(json.dumps(all_data, indent=2))


def test_query_data_flatten_list():
    """Return the first row of flattened list data for each table in JSON format with indent 2."""
    # tables = ["agency", "title", "section"]
    tables = ["agency"]
    all_data = {}
    
    for table in tables:
        columns = con.execute(f"DESCRIBE {table}").fetchall()
        column_names = [col[0] for col in columns]
        # column_names = ["slug", "cfr_ref_title", "cfr_ref_chapter"]
        result = con.execute(f"""
            SELECT
                a.slug,
                CAST(REPLACE((ref)->'unnest'->'title', '"', '') AS INTEGER) AS cfr_ref_title,
                CAST(REPLACE((ref)->'unnest'->'chapter', '"', '') AS VARCHAR) AS cfr_ref_chapter
            FROM {table} a
            CROSS JOIN UNNEST(a.cfr_references) AS ref
            LIMIT 1
        """).fetchone()
        all_data[table] = None if result is None else dict(zip(column_names, result))

    print(json.dumps(all_data, indent=2))


def main():
    if not test_tables_exist():
        print("Test failed: Not all tables exist.")
        return

    test_query_data()
    # test_query_data_simple()
    test_query_data_get_first_row()
    print("Tests completed.")

if __name__ == "__main__":
    main()