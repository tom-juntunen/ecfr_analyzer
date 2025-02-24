"""
db_loader.py

A module to load each JSONL file in the "data" folder into a DuckDB database.
The table name is determined by the route name (endpoint name) as inferred from the file name.
Now includes a merge_jsonl_file() function that performs SCD Type 2 merging for the "section" table.
Usage:
    python db_loader.py
"""

import os
import glob
import json
import duckdb
import pandas as pd

from db_models import models

# Define known endpoint names – adjust as needed.
endpoint_names = ["agency", "title", "section"]

data_dir = os.path.abspath("data")
if not os.path.exists(data_dir):
    print("Data directory does not exist. Please run ecfr_client.py first.")
    exit(1)

# Connect to a DuckDB database (file-based).
con = duckdb.connect(database='ecfr_analyzer_local.db')

def get_table_name(filename):
    """
    Determines the table name based on the file name.
    """
    for name in endpoint_names:
        if filename.startswith(name):
            return name
    return os.path.splitext(filename)[0].split('_')[0]

def merge_jsonl_file(file_path, valid_from_date):
    """
    Reads a JSONL file from the data folder and merges its contents into the database.
    For the "section" table, applies SCD Type 2 logic:
      - If the table does not exist yet, creates it with SCD fields.
      - If it exists, expires current records that have changed and inserts new rows.
    For other endpoints, simply loads the data.
    
    Args:
        file_path (str): Path to the JSONL file.
        valid_from_date (str): Date string for the record snapshot date in YYYY-MM-DD format.
    """
    filename = os.path.basename(file_path)
    table_name = get_table_name(filename)
    if not table_name:
        print(f"Could not determine table name for file: {filename}")
        return

    # Read JSONL file into a DataFrame
    records = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except Exception as e:
                print(f"Error parsing line in {filename}: {e}")

    if not records:
        print(f"No records found in {filename}")
        return

    df = pd.DataFrame(records)

    if table_name == 'section':
        # Convert dictionary columns to JSON strings if necessary
        for col in ['CITA', 'EDNOTE']:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
            else:
                df[col] = ''
        
        # Ensure optional columns exist and handle renaming
        column_mapping = {
            'subtitle': 'subtitle',
            'subchapter': 'subchapter',
            'subpart': 'subpart',
            '@VOLUME': 'volume',
            'HEAD': 'head',
            'P': 'p'
        }
        for src_col, tgt_col in column_mapping.items():
            if src_col not in df.columns:
                df[tgt_col] = ''
            elif src_col != tgt_col:
                df[tgt_col] = df[src_col]
                df.drop(columns=[src_col], inplace=True)
        
        # Add valid_from if not present
        if 'valid_from' not in df.columns:
            df['valid_from'] = valid_from_date

        # Add p_delta_chars if not present (default to 0)
        if 'p_delta_chars' not in df.columns:
            df['p_delta_chars'] = 0

        # Get filter for delete tracking
        df.fillna({'title': ' '}, inplace=True)
        df.fillna({'chapter': ' '}, inplace=True)
        df['cfr_ref_title'] = df['title'].apply(lambda x: x.split(' ')[1] if '\u2014' not in x else x.split('\u2014')[0].split(' ')[1])
        df['cfr_ref_chapter'] = df['chapter'].apply(lambda x: x.split(' ')[1] if '\u2014' not in x else x.split('\u2014')[0].split(' ')[1])
        cfr_ref_title = df.iloc[0]['cfr_ref_title']

        # 1. Create the target table if it doesn't exist
        con.execute("""
            CREATE TABLE IF NOT EXISTS section (
                id VARCHAR,
                subpart_id VARCHAR,
                title VARCHAR,
                cfr_ref_title INTEGER,
                subtitle VARCHAR,
                chapter VARCHAR,
                cfr_ref_chapter VARCHAR,
                subchapter VARCHAR,
                part VARCHAR,
                subpart VARCHAR,
                n VARCHAR,
                head VARCHAR,
                p VARCHAR,
                p_delta_chars INTEGER,
                valid_from DATE,
                valid_to DATE,
                is_current BOOLEAN,
                is_deleted BOOLEAN,
                PRIMARY KEY (id, valid_from)
            );
        """)

        # 2. Insert new/updated records with p_delta_chars on the new record
        insert_query = """
        INSERT INTO section (
            id, subpart_id, title, cfr_ref_title, subtitle, chapter, cfr_ref_chapter,
            subchapter, part, subpart, n, head, p, p_delta_chars, valid_from, valid_to,
            is_current, is_deleted
        )
        WITH prev AS (
            SELECT id, p
            FROM section
            WHERE is_current = TRUE
        )
        SELECT 
            s.id,
            s.subpart_id,
            s.title,
            s.cfr_ref_title,
            s.subtitle,
            s.chapter,
            s.cfr_ref_chapter,
            s.subchapter,
            s.part,
            s.subpart,
            s."@N" AS n,
            s.HEAD AS head,
            s.P AS p,
            CASE 
                WHEN prev.id IS NOT NULL THEN LENGTH(s.P) - LENGTH(prev.p)
                ELSE 0 
            END AS p_delta_chars,
            s.valid_from,
            CAST('9999-12-31' AS DATE) AS valid_to,
            TRUE AS is_current,
            FALSE AS is_deleted
        FROM df s
        LEFT JOIN prev 
            ON s.id = prev.id
        WHERE prev.id IS NULL OR prev.p IS DISTINCT FROM s.P
        """
        con.execute(insert_query)

        # 3. Expire updated records in the target table
        update_query = """
        UPDATE section
        SET 
            valid_to = (SELECT s.valid_from FROM df s WHERE s.id = section.id),
            is_current = FALSE
        WHERE is_current = TRUE
        AND EXISTS (
            SELECT 1 FROM df s
            WHERE s.id = section.id 
            AND section.p IS DISTINCT FROM s.P
        );
        """
        con.execute(update_query)

        # 4. Expire (mark as deleted) records not in df
        delete_update_query = """
        UPDATE section
        SET 
            valid_to = ?,
            is_current = FALSE,
            is_deleted = TRUE
        WHERE is_current = TRUE
        AND NOT EXISTS (
            SELECT 1 FROM df s 
            WHERE s.id = section.id
        )
        AND cfr_ref_title = CAST(? AS INTEGER);
        """
        con.execute(delete_update_query, [valid_from_date, cfr_ref_title])

        print(f"Merged {len(df)} records into table 'section' using SCD2 logic from {filename}.")

    elif table_name == 'agency':
        # Write both agency and agency_section_ref tables
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
        print(f"Loaded {len(df)} records into table 'agency' and created 'agency_section_ref' from {filename}.")
    
    else:
        # For other endpoints, load the whole DataFrame
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        print(f"Loaded {len(df)} records into table '{table_name}' from {filename}.")



if __name__ == "__main__":
    # If run as a script, process all JSONL files in the data folder
    jsonl_files = glob.glob(os.path.join(data_dir, "*.jsonl"))
    if not jsonl_files:
        print("No JSONL files found in data directory.")
    else:
        # Using current date as default valid_from; adjust as needed
        from datetime import date
        valid_from = date.today().isoformat()
        
        # for filepath in jsonl_files:
        #     merge_jsonl_file(filepath, valid_from)

        # List the tables loaded
        tables = con.execute("SHOW TABLES").fetchall()
        print("\nTables in DuckDB database:")
        for t in tables:
            print(t[0])
            
        # # Building models (without dbt for now)
        print(f"Creating data models...")
        for model in models:
            query = models[model]["query"]
            if models[model]["stmt_type"] == "create":
                query = f'CREATE OR REPLACE TABLE {model} AS (' + query + ')'
                try:
                    con.execute(query)
                except Exception as e:
                    print(e)
                else:
                    print(f"Table {model} successfully created.")

    # Close the connection
    con.close()

