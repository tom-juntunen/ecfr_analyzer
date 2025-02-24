import os
import logging
from fastapi import FastAPI, Query, Header, HTTPException, status, Depends
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

# --------------------------
# API Key Dependency
# --------------------------
def verify_api_key(ecfr_api_key: str = Header(...)):
    """Dependency that validates the API key from the request header."""
    expected_key = os.getenv("ECFR_API_KEY")
    if not expected_key or ecfr_api_key != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API Key",
        )

# --------------------------
# Database Connection and Helpers
# --------------------------
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

# Connect to DuckDB (local or Motherduck)
raw_con = duckdb.connect(database="md:ecfr_analyzer", read_only=True)
# raw_con = duckdb.connect(database="ecfr_analyzer_local.db", read_only=True)
con = LoggingConnection(raw_con)

print(con.execute("SELECT 'Connected to MotherDuck!'").fetchone())

app = FastAPI(title="eCFR Analyzer")

# --------------------------
# Pydantic Models
# --------------------------
class KPIData(BaseModel):
    metric: str
    value: float

class ChartData(BaseModel):
    labels: List[str]
    series1: List[float]
    series2: List[float]

class Agency(BaseModel):
    name: str
    short_name: Optional[str]
    cfr_references: Optional[List[Dict[str, Any]]]

class AgencyResponse(BaseModel):
    agencies: List[Agency]

class TableRow(BaseModel):
    agency: str
    title: str
    chapter: str
    part: str
    total_word_count: int
    top_words: List[str]
    section_count: int
    full_text: str
    count_section_changes: Optional[int] = 0
    count_section_chars_changed: Optional[int] = 0
    rolling_60m_avg_sum_p_delta_chars: Optional[float] = 0.0
    rolling_60m_avg_count_p_deltas: Optional[float] = 0.0

class TableResponse(BaseModel):
    total_count: int
    data: List[TableRow]

# --------------------------
# Helpers
# --------------------------
class SQLBuilder:
    """Helper class to construct SQL queries with filters consistently"""
    def __init__(self, base_sql: str):
        self.sql = base_sql
        self.params = []
        self.where_conditions = []

    def add_search_filter(self, search: str, column: str) -> 'SQLBuilder':
        if search:
            sanitized = re.sub(r"[;'\"]", "", search.strip()).lower()
            self.where_conditions.append(f"LOWER({column}) LIKE ?")
            self.params.append(f"%{sanitized}%")
        return self

    def add_agency_filter(self, agencies: List[str]) -> 'SQLBuilder':
        if agencies:
            placeholders = ",".join("?" for _ in agencies)
            self.where_conditions.append(f"c.agency IN ({placeholders})")
            self.params.extend(agencies)
        return self

    def apply_filters(self) -> 'SQLBuilder':
        if self.where_conditions:
            self.sql += " WHERE " + " AND ".join(self.where_conditions)
        return self

    def add_pagination(self, skip: int, limit: int) -> 'SQLBuilder':
        self.sql += " LIMIT ? OFFSET ?"
        self.params.extend([limit, skip])
        return self

    def build(self) -> Tuple[str, List]:
        return self.sql, self.params

def get_total_count(con, sql: str, params: tuple) -> int:
    count_sql = f"SELECT COUNT(*) AS total FROM ({sql}) AS subquery"
    df = con.execute(count_sql, params).fetchdf()
    return int(df.loc[0, "total"]) if not df.empty else 0

# --------------------------
# Endpoints
# --------------------------
@app.get("/api/agency", response_model=AgencyResponse, dependencies=[Depends(verify_api_key)])
async def agency():
    sql = """
        SELECT DISTINCT
            name,
            short_name,
            cfr_references
        FROM agency
        ORDER BY name
    """
    rows = con.execute(sql).fetchall()
    result = []
    for r in rows:
        result.append(Agency(
            name=r[0],
            short_name=r[1],
            cfr_references=r[2]
        ))
    return AgencyResponse(agencies=result)

@app.get("/api/kpi", response_model=List[KPIData], dependencies=[Depends(verify_api_key)])
async def get_kpi(
    search: str = Query(""),
    agencies: List[str] = Query([])
):
    builder = SQLBuilder("""
        SELECT
            c.agency,
            c.section_count,
            c.total_word_count,
            c.combined_full_text,
            m.count_p_deltas,
            m.sum_p_delta_chars,
            DATE_TRUNC('MONTH', m.max_valid_from) AS month_trunc
        FROM stg__agg_chapter_part_doc_count c
        JOIN stg__agg_section_change_metrics m
            ON c.id = m.id
            AND c.agency IS NOT DISTINCT FROM m.agency
    """)
    builder.add_search_filter(search, "c.combined_full_text")
    builder.add_agency_filter(agencies)
    filtered_base_sql, params = builder.apply_filters().build()

    final_sql = f"""
    WITH base AS (
        {filtered_base_sql}
    ),
    aggregates AS (
        SELECT
            SUM(section_count) AS total_sections,
            SUM(total_word_count) AS total_words,
            SUM(count_p_deltas) AS sum_changes,
            SUM(sum_p_delta_chars) AS sum_length_changes,
            COUNT(DISTINCT month_trunc) AS month_count
        FROM base
    )
    SELECT
        COALESCE(total_sections, 0) AS total_sections,
        COALESCE(total_words, 0) AS total_words,
        COALESCE(sum_changes * 1.0 / NULLIF(month_count, 0), 0) AS changes_per_month,
        COALESCE(sum_length_changes * 1.0 / NULLIF(month_count, 0), 0) AS length_changes_per_month
    FROM aggregates
    """
    logger.info("Executing KPI query: %s", final_sql)
    logger.info("With parameters: %s", params)
    try:
        df = con.execute(final_sql, params).fetchdf()
    except duckdb.duckdb.InvalidInputException as e:
        logger.error("Query execution failed: %s. Query: %s", str(e), final_sql)
        raise

    if df.empty:
        return [
            KPIData(metric="Section Count", value=0),
            KPIData(metric="Word Count", value=0),
            KPIData(metric="Section Changes / Month", value=0),
            KPIData(metric="Text Length Change / Month", value=0),
        ]

    row = df.iloc[0]
    return [
        KPIData(metric="Section Count", value=int(row["total_sections"])),
        KPIData(metric="Word Count", value=int(row["total_words"])),
        KPIData(metric="Section Changes / Month", value=int(round(row["changes_per_month"], 0))),
        KPIData(metric="Text Length Change / Month", value=int(round(row["length_changes_per_month"], 0))),
    ]

@app.get("/api/chart", response_model=ChartData, dependencies=[Depends(verify_api_key)])
async def get_chart(
    search: str = Query(""),
    agencies: List[str] = Query([]),
    report_id: int = Query(1)
):
    # Choose the appropriate metrics based on report_id
    if report_id == 1:
        metric1 = "SUM(c.section_count)"
        metric2 = "SUM(c.total_word_count)"
    else:
        metric1 = "SUM(m.count_p_deltas)"
        metric2 = "SUM(m.sum_p_delta_chars)"
    
    base_sql = f"""
    SELECT 
        c.agency,
        {metric1} AS metric1_value,
        {metric2} AS metric2_value
    FROM stg__agg_chapter_part_doc_count c
    JOIN stg__agg_section_change_metrics m
        ON c.id = m.id
        AND c.agency IS NOT DISTINCT FROM m.agency
    """
    
    builder = SQLBuilder(base_sql)
    builder.add_search_filter(search, "c.combined_full_text")
    builder.add_agency_filter(agencies)
    final_sql, params = builder.apply_filters().build()
    final_sql += " GROUP BY c.agency ORDER BY metric1_value DESC"
    
    rows = con.execute(final_sql, params).fetchall()
    labels = [row[0] for row in rows]
    series1 = [float(row[1]) for row in rows]
    series2 = [float(row[2]) for row in rows]
    
    return ChartData(labels=labels, series1=series1, series2=series2)

@app.get("/api/table", response_model=TableResponse, dependencies=[Depends(verify_api_key)])
async def get_table(
    search: str = Query(""),
    agencies: List[str] = Query([]),
    report_id: int = Query(1),
    skip: int = 0,
    limit: int = 100,
    sort: Optional[str] = Query(None),
    sort_dir: Optional[str] = Query("asc")
):
    base_sql = """
    SELECT
        c.agency,
        c.title,
        c.chapter,
        c.part,
        c.section_count,
        c.total_word_count,
        c.top_words,
        c.full_text,
        COALESCE(m.count_p_deltas, 0) AS count_section_changes,
        COALESCE(m.sum_p_delta_chars, 0) AS count_section_chars_changed,
        COALESCE(m.rolling_60m_avg_sum_p_delta_chars, 0) AS rolling_60m_avg_sum_p_delta_chars,
        COALESCE(m.rolling_60m_avg_count_p_deltas, 0) AS rolling_60m_avg_count_p_deltas
    FROM stg__agg_chapter_part_doc_count_unnested c
    LEFT JOIN stg__agg_section_change_metrics m
        ON c.id = m.id
        AND c.agency IS NOT DISTINCT FROM m.agency
    """

    # Build filtered SQL for counting
    count_builder = SQLBuilder(base_sql)
    count_builder.add_search_filter(search, "c.full_text")
    count_builder.add_agency_filter(agencies)
    count_sql, count_params = count_builder.apply_filters().build()
    total_count = get_total_count(con, count_sql, tuple(count_params))

    # Build paginated SQL
    data_builder = SQLBuilder(base_sql)
    data_builder.add_search_filter(search, "c.full_text")
    data_builder.add_agency_filter(agencies)
    data_builder.apply_filters()
    
    # Allowed sort fields to prevent SQL injection
    allowed_sort_fields = {
        "agency": "c.agency",
        "title": "c.title",
        "chapter": "c.chapter",
        "part": "c.part",
        "section_count": "c.section_count",
        "total_word_count": "c.total_word_count"
    }
    if sort and sort in allowed_sort_fields:
        # Validate sort direction
        sort_direction = "ASC" if sort_dir.lower() != "desc" else "DESC"
        data_builder.sql += f" ORDER BY {allowed_sort_fields[sort]} {sort_direction}"
    else:
        data_builder.sql += " ORDER BY c.total_word_count, c.agency, c.chapter, c.part"
    
    data_builder.add_pagination(skip, limit)
    final_sql, params = data_builder.build()

    df = con.execute(final_sql, tuple(params)).fetchdf()
    data_rows = [
        TableRow(
            agency=row["agency"],
            title=str(row["title"]),
            chapter=str(row["chapter"]),
            part=str(row["part"]),
            section_count=int(row["section_count"]),
            total_word_count=int(row["total_word_count"]),
            top_words=row["top_words"].split(", ") if pd.notna(row["top_words"]) else [],
            full_text=row["full_text"] or "",
            count_section_changes=int(row["count_section_changes"]),
            count_section_chars_changed=int(row["count_section_chars_changed"]),
            rolling_60m_avg_sum_p_delta_chars=float(row["rolling_60m_avg_sum_p_delta_chars"]),
            rolling_60m_avg_count_p_deltas=float(row["rolling_60m_avg_count_p_deltas"])
        )
        for _, row in df.iterrows()
    ]

    return TableResponse(total_count=total_count, data=data_rows)

@app.get("/api/refresh", dependencies=[Depends(verify_api_key)])
async def refresh_data():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {"detail": "Data refreshed successfully", "last_refreshed": now}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
