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
model_names = ["base__agency_section"]

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
                        md5(concat(title, '_surrogate_key_', chapter, part, subpart, HEAD, P)) AS id,
                        md5(concat(title, '_surrogate_key_', chapter, part, subpart)) AS subpart_id,
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
                        md5(concat(title, '_surrogate_key_', chapter, part, subpart, HEAD, P)) AS id,
                        md5(concat(title, '_surrogate_key_', chapter, part, subpart)) AS subpart_id,
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

# # Building models (without dbt for now)

models = {
    "base__agency_section": {
        "query": """
            SELECT 
                a.name AS agency,
                s.chapter,
                s.title,
                s.subtitle,
                s.part,
                s.subpart,
                s.subpart_id,
                s.HEAD AS header_text,
                s.p AS paragraph_text,
                COALESCE(s.HEAD || ' ' || s.p, '') AS full_text
            FROM agency a
            JOIN agency_section_ref r 
                ON a.slug = r.slug
            JOIN section s 
                ON CAST(s.cfr_ref_title AS INTEGER) = r.cfr_ref_title
               AND s.cfr_ref_chapter IS NOT DISTINCT FROM r.cfr_ref_chapter
            ORDER BY a.name, s.title, s.chapter
        """,
        "type": "table",
        "stmt_type": "create"
    },

    "stg__agg_chapter_part_doc_count": {
        "query": """
            WITH base_filtered AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    -- Combine all text for this agency + chapter + part
                    STRING_AGG(full_text, '\n\n') AS combined_full_text,
                    COUNT(*) AS section_count
                FROM base__agency_section
                GROUP BY agency, title, chapter, part
            ),

            -- 2) Expand tokens for counting
            expanded AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    section_count,
                    combined_full_text,
                    SPLIT(REPLACE(combined_full_text, '  ', ' '), ' ') AS all_tokens
                FROM base_filtered
            ),

            -- 3) Unnest into rows
            unnested AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    section_count,
                    TRIM(token) AS token
                FROM expanded
                CROSS JOIN UNNEST(all_tokens) AS t(token)
            ),

            -- 4) Exclude stopwords, count frequencies
            counts AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    section_count,
                    token,
                    COUNT(*) AS cnt
                FROM unnested
                WHERE 
                    NOT REGEXP_MATCHES(
                        token,
                        '^(§|a|about|above|after|again|against|all|am|an|and|any|are|aren''t|as|at|be|because|been|before|being|below|between|both|but|by|b|can|can''t|cannot|could|could''t|cfr|c|did|did''t|do|does|does''t|doing|don''t|down|during|d|each|e|few|for|from|further|had|had''t|has|has''t|have|haven''t|having|he|he''d|he''ll|he''s|her|here|here''s|hers|herself|him|himself|his|how|how''s|i|i''d|i''ll|i''m|i''ve|if|in|into|is|isn''t|it|it''s|its|itself|let''s|me|may|more|most|must|mustn''t|my|myself|no|nor|not|of|off|on|once|only|or|other|ought|our|ours|ourselves|out|over|own|paragraph|part|required|reserved|same|section|shall|shan''t|she|she''d|she''ll|she''s|should|shouldn''t|so|s|some|such|subpart|''#text'':|than|that|that''s|the|their|theirs|them|themselves|then|there|there''s|these|they|they''d|they''ll|they''re|they''ve|this|those|through|to|too|under|until|up|u|very|was|wasn''t|we|we''d|we''ll|we''re|we''ve|were|weren''t|what|what''s|when|when''s|where|where''s|which|while|who|who''s|whom|why|why''s|with|won''t|would|wouldn''t|you|you''d|you''ll|you''re|you''ve|your|yours|yourself|yourselves)$',
                        'i'
                    )
                    AND NOT REGEXP_MATCHES(token, '^(\\d+|{''[@A-Z]+'':)$')
                GROUP BY agency, title, chapter, part, section_count, token
            ),

            -- 5) Rank top tokens
            ranked AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    section_count,
                    token,
                    cnt,
                    ROW_NUMBER() OVER (
                        PARTITION BY agency, chapter, part
                        ORDER BY cnt DESC
                    ) AS rnk
                FROM counts
            ),

            top15 AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    section_count,
                    STRING_AGG(token || ' (' || cnt || ')', ', ') AS top_words
                FROM ranked
                WHERE rnk <= 15
                GROUP BY agency, title, chapter, part, section_count
            ),

            -- 6) Count total words
            word_counts AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    section_count,
                    SUM(array_length(all_tokens, 1)) AS total_word_count
                FROM expanded
                GROUP BY agency, title, chapter, part, section_count
            )

            -- 7) Combine them all
            SELECT
                wc.agency,
                wc.title,
                wc.chapter,
                wc.part,
                wc.section_count,
                wc.total_word_count,
                t.top_words,
                bf.combined_full_text
            FROM word_counts wc
            JOIN top15 t
              ON t.agency         = wc.agency
              AND t.title         = wc.title
             AND t.chapter        = wc.chapter
             AND t.part        = wc.part
             AND t.section_count = wc.section_count
            JOIN base_filtered bf
              ON bf.agency  = wc.agency
              AND bf.title  = wc.title
             AND bf.chapter = wc.chapter
             AND bf.part = wc.part
            ORDER BY total_word_count DESC

        """,
        "type": "table",
        "stmt_type": "create"
    },
    "stg__agg_chapter_part_doc_count_unnested": {
        "query": """
            SELECT
                agency,
                title,
                chapter,
                part,
                section_count,
                total_word_count,
                top_words,
                UNNEST(STRING_SPLIT(combined_full_text, '\n\n')) AS full_text
            FROM stg__agg_chapter_part_doc_count
        """,
        "type": "table",
        "stmt_type": "create"
    }
    
}

print(f"Creating data models and counting keywords per agency...")
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
