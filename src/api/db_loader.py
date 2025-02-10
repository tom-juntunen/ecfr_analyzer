"""
db_loader.py

A module to load each JSONL file in the "data" folder into an in‑memory DuckDB database.
The table name is determined by the route name (endpoint name) as inferred from the file name.

Usage:
    python db_loader.py
"""

import os
import glob
import json
import duckdb
import pandas as pd
import re

# Define known endpoint names – adjust as needed.
endpoint_names = ["agency", "title", "section"]

data_dir = os.path.abspath("data")
if not os.path.exists(data_dir):
    print("Data directory does not exist. Please run ecfr_client.py first.")
    exit(1)

# Connect to an in-memory DuckDB instance.
# con = duckdb.connect(database=':memory:')
con = duckdb.connect(database='duck.db')

# Function to determine the table name based on the file name.
def get_table_name(filename):
    # For each known endpoint name, check if the file name starts with it.
    for name in endpoint_names:
        if filename.startswith(name):
            return name
    # Fallback: use the filename (without extension) as table name.
    return os.path.splitext(filename)[0]

def sanitize_identifier(identifier):
    # Only allow letters, numbers, and underscores, and must start with a letter or underscore.
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", identifier):
        raise ValueError("Invalid identifier")
    return identifier

# Process all JSONL files in the data folder.
jsonl_files = glob.glob(os.path.join(data_dir, "*.jsonl"))

# Set a flag to track whether the section table has been created
section_table_created = False

for filepath in jsonl_files:
    filename = os.path.basename(filepath)
    table_name = sanitize_identifier(get_table_name(filename))
    records = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except Exception as e:
                print(f"Error parsing line in {filename}: {e}")

    if records:
        df = pd.DataFrame(records)
        if table_name == 'section':
            # Convert fields that might be dictionaries to JSON strings.
            for col in ['CITA', 'EDNOTE']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)
                else:
                    df[col] = ''
            # Ensure optional columns exist in the data frame
            opt_cols = ['subtitle', 'subchapter', 'subpart', '@VOLUME']
            for o_col in opt_cols:
                if o_col not in df.columns:
                    df[o_col] = ''

            if not section_table_created:
                # Create the table for the first file
                con.execute(f"""
                    CREATE OR REPLACE TABLE {table_name} AS 
                    SELECT 
                        md5(concat(title, '_surrogate_key_', chapter, part, HEAD, P)) AS id,
                        title,
                        CAST(SPLIT_PART(SPLIT_PART(title, '\u2014', 1), ' ', 2) AS INTEGER) AS cfr_ref_title,
                        subtitle,
                        chapter,
                        SPLIT_PART(SPLIT_PART(chapter, '\u2014', 1), ' ', 2) AS cfr_ref_chapter,
                        subchapter,
                        part,
                        subpart,
                        CAST("@N" AS VARCHAR) AS n,
                        CAST("@TYPE" AS VARCHAR) AS type,
                        CAST("@VOLUME" AS VARCHAR) AS volume,
                        CAST(HEAD AS VARCHAR) AS head,
                        CAST(P AS VARCHAR) AS p,
                        CAST(CITA AS VARCHAR) AS cita,
                        CAST(EDNOTE AS VARCHAR) AS ednote
                    FROM df
                """)
                section_table_created = True
            else:
                # Insert new data into the existing table for subsequent files
                con.execute(f"""
                    INSERT INTO {table_name}
                    SELECT 
                        md5(concat(title, '_surrogate_key_', chapter, part, HEAD, P)) AS id,
                        title,
                        CAST(SPLIT_PART(SPLIT_PART(title, '\u2014', 1), ' ', 2) AS INTEGER) AS cfr_ref_title,
                        subtitle,
                        chapter,
                        SPLIT_PART(SPLIT_PART(chapter, '\u2014', 1), ' ', 2) AS cfr_ref_chapter,
                        subchapter,
                        part,
                        subpart,
                        CAST("@N" AS VARCHAR) AS n,
                        CAST("@TYPE" AS VARCHAR) AS type,
                        CAST("@VOLUME" AS VARCHAR) AS volume,
                        CAST(HEAD AS VARCHAR) AS head,
                        CAST(P AS VARCHAR) AS p,
                        CAST(CITA AS VARCHAR) AS cita,
                        CAST(EDNOTE AS VARCHAR) AS ednote
                    FROM df
                """)
        elif table_name == 'agency':
            # Write both agency and agency_section_ref tables here
            con.execute("CREATE OR REPLACE TABLE agency AS SELECT * FROM df")
            con.execute("""
                CREATE OR REPLACE TABLE agency_section_ref 
                AS 
                SELECT
                    a.slug,
                    CAST(REPLACE((ref)->'unnest'->'title', '"', '') AS INTEGER) AS cfr_ref_title,
                    CAST(REPLACE((ref)->'unnest'->'chapter', '"', '') AS VARCHAR) AS cfr_ref_chapter
                FROM agency a
                CROSS JOIN UNNEST(a.cfr_references) AS ref
            """)
        else:
            # For other endpoints, load the whole DataFrame.
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        print(f"Loaded {len(df)} records into table '{table_name}' from {filename}")
    else:
        print(f"No records found in {filename}")

# list the tables loaded.
tables = con.execute("SHOW TABLES").fetchall()
print("\nTables in DuckDB in-memory database:")
for t in tables:
    print(t[0])
