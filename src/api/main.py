import os
import logging
from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import re
import duckdb
import pandas as pd
from collections import Counter
import nest_asyncio

nest_asyncio.apply()

# Logging connection
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class LoggingConnection:
    def __init__(self, connection):
        self._connection = connection

    def execute(self, query, params=None, *args, **kwargs):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Executing SQL query: %s", query)
            if params:
                logger.debug("With params: %s", params)
        if params is not None:
            return self._connection.execute(query, params, *args, **kwargs)
        else:
            return self._connection.execute(query, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._connection, name)

# Connect to DuckDB
# Set MotherDuck Access Token
# os.environ["MOTHERDUCK_TOKEN"] 

# Connect to MotherDuck database
raw_con = duckdb.connect(database="md:ecfr_analyzer", read_only=True)
con = LoggingConnection(raw_con)

# Verify connection
print(con.execute("SELECT 'Connected to MotherDuck!'").fetchone())

app = FastAPI(title="eCFR Analyzer")

# List of approved models (using their Hugging Face repo IDs)
APPROVED_MODELS = [
    "gpt2",
    "bert-base-uncased",
]

# Pydantic Data Models
class ModelRequest(BaseModel):
    model_name: str

class ModelResult(BaseModel):
    message: str
    error: str

class KPIData(BaseModel):
    metric: str
    value: float

class ChartData(BaseModel):
    labels: List[str]
    values: List[float]

class Agency(BaseModel):
    display_name: str
    short_name: Optional[str]
    cfr_references: Optional[List[Dict[str, Any]]]

class AgencyResponse(BaseModel):
    agencies: List[Agency]

# Table / Search models
class TableRow(BaseModel):
    agency: str
    title: str
    chapter: str
    part: str
    total_word_count: int
    top_words: List[str]
    section_count: int
    full_text: str

class TableResponse(BaseModel):
    total_count: int
    data: List[TableRow]

# Helpers
def append_search_filter(sql: str, search: str, column:str="{filter_column}") -> Tuple[str, Tuple]:
    """
    If 'search' is non-empty, append a LIKE filter on 'column'.
    """
    def sanitize(s: str) -> str:
        return re.sub(r"[;'\"]", "", s.strip())

    params = ()
    if search:
        s = sanitize(search)
        if "WHERE" in sql.upper():
            sql += f" AND LOWER({column}) LIKE ?"
        else:
            sql += f" WHERE LOWER({column}) LIKE ?"
        params = (f"%{s.lower()}%",)
    return sql, params

def apply_pagination(sql: str, params: Tuple, skip: int, limit: int) -> Tuple[str, Tuple]:
    sql += " LIMIT ? OFFSET ?"
    return sql, params + (limit, skip)

def get_total_count(sql: str, params: tuple) -> int:
    count_sql = f"SELECT COUNT(*) AS total_count FROM ({sql}) AS sub"
    df = con.execute(count_sql, params).fetchdf()
    return int(df.loc[0, "total_count"])

# # Endpoints
# @app.post("/api/download_model", response_model=ModelResult)
# async def download_model(model_request: ModelRequest):
#     """
#     Download a model from Hugging Face if not already downloaded.
#     """
#     model_name = model_request.model_name
#     if model_name not in APPROVED_MODELS:
#         return ModelResult(message="", error=f"Model '{model_name}' is not in the approved list.")

#     try:
#         model_dir = os.path.join("models", model_name.replace("/", "_"))
#         abs_path = os.path.abspath(model_dir)
#         if not os.path.exists(model_dir):
#             from hf_utils import download_model_files
#             local_dir, downloaded_files = download_model_files(model_name, revision=None)
#             abs_path = os.path.abspath(local_dir)
#             message = (
#                 f"The {model_name} model has been downloaded and saved to {abs_path}.\n"
#                 f"Files downloaded: {downloaded_files}"
#             )
#         else:
#             message = f"The {model_name} model is already downloaded at {abs_path}."
#         return ModelResult(message=message, error="")
#     except Exception as e:
#         return ModelResult(message="", error=str(e))

@app.get("/api/agency", response_model=AgencyResponse)
async def agency():
    """
    Return a list of all agencies for the filter UI
    """
    sql = """
        SELECT DISTINCT
            display_name,
            short_name,
            cfr_references
        FROM agency
        ORDER BY display_name
    """
    rows = con.execute(sql).fetchall()
    result = []
    for r in rows:
        result.append(Agency(
            display_name=r[0],
            short_name=r[1],
            cfr_references=r[2]
        ))
    return AgencyResponse(agencies=result)

@app.get("/api/kpi", response_model=List[KPIData])
async def get_kpi(
    search: str = Query(""),
    agencies: List[str] = Query([])
):
    """
    Show aggregated KPI metrics from stg__agg_chapter_part_doc_count
    (one row per (agency, chapter, part)), optionally filtering by agency & search.
    """
    base_sql = """
        SELECT
            agency,
            chapter,
            section_count,
            total_word_count,
            top_words,
            combined_full_text
        FROM stg__agg_chapter_part_doc_count
    """
    # Filter on combined_full_text if search
    query_sql, params = append_search_filter(base_sql, search, column="combined_full_text")

    # Agency filter
    if agencies:
        placeholders = ",".join("?" for _ in agencies)
        if "WHERE" in query_sql.upper():
            query_sql += f" AND agency IN ({placeholders})"
        else:
            query_sql += f" WHERE agency IN ({placeholders})"
        params += tuple(agencies)

    df = con.execute(query_sql, params).fetchdf()
    total_sections = df["section_count"].sum() if not df.empty else 0
    total_words = df["total_word_count"].sum() if not df.empty else 0

    # Dummy values for demonstration
    change_rate = 2.5
    alignment_score = 88.2

    return [
        KPIData(metric="Section Count", value=float(total_sections)),
        KPIData(metric="Word Count",    value=float(total_words)),
        KPIData(metric="Change Rate",   value=change_rate),
        KPIData(metric="Alignment Score", value=alignment_score)
    ]


@app.get("/api/chart", response_model=ChartData)
async def get_chart(
    search: str = Query(""),
    agencies: List[str] = Query([])
):
    """
    Return (labels=agency, values=section_count) from stg__agg_chapter_part_doc_count
    """
    base_sql = """
        SELECT
            agency,
            SUM(section_count) AS section_count
        FROM stg__agg_chapter_part_doc_count
    """
    query_sql, params = append_search_filter(base_sql, search, column="combined_full_text")

    if agencies:
        placeholders = ",".join("?" for _ in agencies)
        if "WHERE" in query_sql.upper():
            query_sql += f" AND agency IN ({placeholders})"
        else:
            query_sql += f" WHERE agency IN ({placeholders})"
        params += tuple(agencies)

    query_sql += " GROUP BY agency ORDER BY section_count DESC"

    rows = con.execute(query_sql, params).fetchall()
    chart_labels = [r[0] for r in rows]   # agency
    chart_values = [r[1] for r in rows]   # section_count
    return ChartData(labels=chart_labels, values=chart_values)

@app.get("/api/table", response_model=TableResponse)
async def get_table(
    search: str = Query(""),
    agencies: List[str] = Query([]),
    skip: int = 0,
    limit: int = 100
):
    """
    Return data from stg__agg_chapter_part_doc_count_unnested, 
    with optional search filter on 'full_text' and agency filter.
    Paginate with skip/limit.
    """
    base_sql = """
        SELECT
            agency,
            title,
            chapter,
            part,
            section_count,
            total_word_count,
            top_words,
            full_text
        FROM stg__agg_chapter_part_doc_count_unnested
    """
    query_sql, params = append_search_filter(base_sql, search, column="full_text")

    if agencies:
        placeholders = ",".join("?" for _ in agencies)
        if "WHERE" in query_sql.upper():
            query_sql += f" AND agency IN ({placeholders})"
        else:
            query_sql += f" WHERE agency IN ({placeholders})"
        params += tuple(agencies)

    # Now we get the total_count
    total_count = get_total_count(query_sql, params)

    # Pagination
    query_sql, params = apply_pagination(query_sql, params, skip, limit)
    df_page = con.execute(query_sql, params).fetchdf()

    # Build the response
    data_rows = []
    for _, row in df_page.iterrows():
        data_rows.append(TableRow(
            agency=row["agency"],
            title=row["title"],
            chapter=row["chapter"],
            part=row["part"],
            total_word_count=row["total_word_count"],
            top_words=row["top_words"].split(", ") if row["top_words"] else [],
            section_count=row["section_count"],
            full_text=row["full_text"]
        ))
    return TableResponse(total_count=total_count, data=data_rows)

@app.get("/api/refresh")
async def refresh_data():
    """
    Fake refresh endpoint
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {"detail": "Data refreshed successfully", "last_refreshed": now}


# Local run
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
