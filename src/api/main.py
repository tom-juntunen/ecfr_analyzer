import os
import logging
from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import duckdb
import pandas as pd
from collections import Counter
import nest_asyncio
from utils import get_keyword_stats_by_agency

# Allow nested event loops if needed
nest_asyncio.apply()

# Configure logging
logger = logging.getLogger(__name__)
# (If you run with uvicorn using --log-level debug, the effective level will be DEBUG.)
# Otherwise you can force it here by uncommenting the next line:
logger.setLevel(logging.DEBUG)

# Logging wrapper for the DuckDB connection
class LoggingConnection:
    def __init__(self, connection):
        self._connection = connection

    def execute(self, query, params=None, *args, **kwargs):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Executing SQL query: %s", query)
            if params:
                logger.debug("With parameters: %s", params)
        if params is not None:
            return self._connection.execute(query, params, *args, **kwargs)
        else:
            return self._connection.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        # Delegate any other attribute/method calls to the underlying connection
        return getattr(self._connection, name)

# Create the raw DuckDB connection and wrap it
raw_con = duckdb.connect(database='duck.db')
con = LoggingConnection(raw_con)

app = FastAPI(title="eCFR Analyzer API")

# List of approved models (using their Hugging Face repo IDs)
APPROVED_MODELS = [
    "gpt2",
    "bert-base-uncased",
]

# Pydantic Models
class SearchRequest(BaseModel):
    query: Optional[str] = ""
    mode: Optional[str] = "keyword"  # "keyword" or "semantic"

class ModelRequest(BaseModel):
    model_name: str

class SearchResult(BaseModel):
    id: int
    title: str
    snippet: str

class ModelResult(BaseModel):
    message: str
    error: str

class KPIData(BaseModel):
    metric: str
    value: float

class ChartData(BaseModel):
    labels: List[str]
    values: List[float]

class TableData(BaseModel):
    data: List[Dict[str, Any]]

# Helper Function to Append a Search Filter
def append_search_filter(sql: str, search: str) -> Tuple[str, Dict]:
    """
    Appends a search filter to the SQL query if a search term is provided.
    Uses a parameterized query for safety.
    """
    params = {}
    if search:
        filter_clause = "LOWER(COALESCE(s.HEAD || ' ' || s.p, '')) LIKE ?"
        # If there is already a WHERE clause, append with AND; otherwise add WHERE.
        if "WHERE" in sql.upper():
            sql += f" AND {filter_clause}"
        else:
            sql += f" WHERE {filter_clause}"
        params = (f"%{search.lower()}%",)
    return sql, params

# The base query used for /kpi, /chart, and /table apis
base_query = """
      SELECT 
        a.name,
        s.cfr_ref_title AS section_title,
        COALESCE(s.HEAD || ' ' || s.p, '') AS section_text,
        COUNT(s.id) OVER (PARTITION BY a.name) AS doc_count
      FROM agency a
      JOIN agency_section_ref r ON a.slug = r.slug
      JOIN section s 
        ON CAST(s.cfr_ref_title AS INTEGER) = r.cfr_ref_title
        AND s.cfr_ref_chapter IS NOT DISTINCT FROM r.cfr_ref_chapter
"""

# API Endpoints
@app.post("/api/download_model", response_model=ModelResult)
async def download_model(model_request: ModelRequest):
    """
    Download a model from Hugging Face if not already downloaded.
    """
    model_name = model_request.model_name
    if model_name not in APPROVED_MODELS:
        return ModelResult(message="", error=f"Model '{model_name}' is not in the approved list.")

    try:
        model_dir = os.path.join("models", model_name.replace("/", "_"))
        abs_path = os.path.abspath(model_dir)
        if not os.path.exists(model_dir):
            revision = None
            # Assumes download_model_files is implemented in hf_utils
            from hf_utils import download_model_files
            local_dir, downloaded_files = download_model_files(model_name, revision=revision)
            abs_path = os.path.abspath(local_dir)
            message = (
                f"The {model_name} model has been downloaded and saved to {abs_path}.\n"
                f"Files downloaded: {downloaded_files}"
            )
        else:
            message = f"The {model_name} model has already been downloaded and is available at {abs_path}."
        return ModelResult(message=message, error="")
    except Exception as e:
        return ModelResult(message="", error=str(e))


@app.post("/api/search", response_model=List[SearchResult])
async def search(search_req: SearchRequest):
    """
    Search for sections whose combined text (HEAD and P fields) matches the query.
    Returns a list of results with an ID, title, and snippet.
    """
    base_query = """
      SELECT 
          a.name,
          s.cfr_ref_title AS section_title,
          COALESCE(s.HEAD || ' ' || s.p, '') AS section_text
      FROM agency a
      JOIN agency_section_ref r ON a.slug = r.slug 
      JOIN section s 
        ON CAST(s.cfr_ref_title AS INTEGER) = r.cfr_ref_title
           AND s.cfr_ref_chapter IS NOT DISTINCT FROM r.cfr_ref_chapter
    """
    query_sql, params = append_search_filter(base_query, search_req.query)
    query_sql += " LIMIT 50"

    print('SEARCH QUERY: ' + query_sql + '; WITH PARAMS: ' + str(params))
    df: pd.DataFrame = con.execute(query_sql, params).fetchdf()

    results = []
    for i, row in df.iterrows():
        title = f"Section {row['section_title']} from {row['name']}"
        snippet = row['section_text'][:100000]
        results.append(SearchResult(id=i + 1, title=title, snippet=snippet))
    return results

@app.get("/api/kpi", response_model=List[KPIData])
async def get_kpi(search: str = Query("", description="Search filter for KPI data")):
    """
    Returns global KPI metrics. 
    If a search query is provided, only sections matching the filter are included.
    """
    # get_agency_data is assumed to be a helper that builds queries using the search filter."""
    query_sql, params = append_search_filter(base_query, search)
    query_sql += " LIMIT 1000"
    print('KPI QUERY: ' + query_sql + '; WITH PARAMS: ' + str(params))
    df: pd.DataFrame = con.execute(query_sql, params).fetchdf()
    global_doc_count = sum(df['doc_count'])
    keyword_stats = get_keyword_stats_by_agency(df)
    global_word_count = sum(keyword_stats[name]['total_word_count'] for name in keyword_stats.keys())

    print('global doc count,', global_doc_count)
    print('global_word_count,', global_word_count)
    
    # Dummy values for demonstration
    change_rate = 2.5
    alignment_score = 88.2
    
    kpi_data = [
        KPIData(metric="Section Count", value=float(global_doc_count)),
        KPIData(metric="Word Count", value=float(global_word_count)),
        KPIData(metric="Change Rate", value=change_rate),
        KPIData(metric="Alignment Score", value=alignment_score),
    ]
    return kpi_data

@app.get("/api/chart", response_model=ChartData)
async def get_chart(search: str = Query("", description="Search filter for chart data")):
    """
    Returns chart data aggregating document counts per agency.
    If a search query is provided, only sections matching the filter are included.
    """
    query_sql, params = append_search_filter(base_query, search)
    query_sql += " LIMIT 1000"

    print('CHART QUERY: ' + query_sql + '; WITH PARAMS: ' + str(params))
    rows = con.execute(query_sql, params).fetchall()
    labels = [row[0] for row in rows]
    values = [row[1] for row in rows]
    return ChartData(labels=labels, values=values)

@app.get("/api/table", response_model=TableData)
async def get_table(search: str = Query("", description="Search filter for table data")):
    """
    Returns per-agency table data including a representative section title,
    word count, top words, and document count. Data are filtered using the provided search query.
    """
    query_sql, params = append_search_filter(base_query, search)
    query_sql += " LIMIT 1000"
    print('TABLE QUERY: ' + query_sql + '; WITH PARAMS: ' + str(params))
    df: pd.DataFrame = con.execute(query_sql, params).fetchdf()
    keyword_stats = get_keyword_stats_by_agency(df)

    table_rows = []
    for agency, stats in keyword_stats.items():
        top_words = stats["counter"].most_common(10)
        # Assume exclusion_list is defined somewhere in your project.
        top_words_list = [f"{word} ({count})" for word, count in top_words]
        table_rows.append({
            "Agency": agency,
            "Section Title": stats["section_title"],
            "Word Count": stats["total_word_count"],
            "Top Words": top_words_list,
            "Document Count": stats["doc_count"]
        })
    table_rows = sorted(table_rows, key=lambda x: x["Word Count"], reverse=True)
    return TableData(data=table_rows)

@app.get("/api/refresh")
async def refresh_data():
    """
    Stub endpoint to simulate a data refresh.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {"detail": "Data refreshed successfully", "last_refreshed": now}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")
