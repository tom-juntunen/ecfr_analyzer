import streamlit as st
import pandas as pd
import httpx
import asyncio
import nest_asyncio
from math import ceil
import altair as alt
import re
from typing import Any, Dict, List, Tuple

st.set_page_config(
    page_title="eCFR Analyzer: Regulatory Insights",
    layout="wide",
    page_icon="🔎"
)

nest_asyncio.apply()

# Point this to your local or deployed FastAPI
API_URL = "https://ecfr-analyzer-api-a84b944f87af.herokuapp.com/api"
# API_URL = "http://localhost:8000/api"
TABLE_PAGE_SIZE = 50

# ---------------------------
# Helper Functions
# ---------------------------
def highlight_keyword(text: str, keyword: str) -> Tuple[str, bool]:
    if not keyword:
        return text, False
    pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    first_found = False

    def replacer(m):
        nonlocal first_found
        first_found = True
        return f"<span style='background-color: yellow; font-weight: bold;'>{m.group(0)}</span>"

    replaced = pattern.sub(replacer, text)
    return replaced, first_found

def show_full_text_with_highlights(text: str, query: str):
    h, _ = highlight_keyword(text, query)
    st.markdown(h, unsafe_allow_html=True)

def render_pagination(total_pages: int, current_page: int, key_prefix: str):
    if total_pages <= 1:
        return
    if total_pages <= 10:
        page_range = list(range(1, total_pages+1))
    else:
        if current_page <= 5:
            page_range = list(range(1,8)) + ["...", total_pages]
        elif current_page >= total_pages-4:
            page_range = [1, "..."] + list(range(total_pages-6, total_pages+1))
        else:
            page_range = [1, "..."] + list(range(current_page-2, current_page+3)) + ["...", total_pages]

    cols = st.columns(len(page_range)+2)
    # Prev
    if current_page > 1:
        if cols[0].button("<<", key=f"{key_prefix}_prev"):
            st.session_state[f"{key_prefix}_page"] = current_page - 1
    else:
        cols[0].write("")
    # Numbers
    idx = 1
    for p in page_range:
        if p == "...":
            cols[idx].write("...")
        else:
            if p == current_page:
                cols[idx].write(f"**{p}**")
            else:
                if cols[idx].button(str(p), key=f"{key_prefix}_page_{p}"):
                    st.session_state[f"{key_prefix}_page"] = p
        idx += 1
    # Next
    if current_page < total_pages:
        if cols[-1].button(">>", key=f"{key_prefix}_next"):
            st.session_state[f"{key_prefix}_page"] = current_page + 1
    else:
        cols[-1].write("")

# ---------------------------
# Async API calls
# ---------------------------
async def refresh_api():
    async with httpx.AsyncClient() as client:
        return await client.get(f"{API_URL}/refresh", timeout=30.0)

async def get_refresh_info():
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{API_URL}/refresh", timeout=30.0)
        if r.status_code == 200:
            return r.json().get("last_refreshed", "")
    return ""

async def get_agency_list():
    async with httpx.AsyncClient() as client:
        return await client.get(f"{API_URL}/agency", timeout=30.0)

async def get_kpi_data(search_query: str, agencies: List[str]):
    if not search_query and not agencies:
        return
    async with httpx.AsyncClient() as client:
        params = [("search", search_query)]
        for a in agencies:
            params.append(("agencies", a))
        return await client.get(f"{API_URL}/kpi", params=params, timeout=30.0)

async def get_chart_data(search_query: str, agencies: List[str], report_id: int):
    if not search_query and not agencies:
        return
    async with httpx.AsyncClient() as client:
        params = [("search", search_query), ("report_id", str(report_id))]
        for a in agencies:
            params.append(("agencies", a))
        return await client.get(f"{API_URL}/chart", params=params, timeout=30.0)

async def get_table_data(
    search_query: str,
    skip: int,
    limit: int,
    agencies: List[str],
    report_id: int,
    sort: str = None,
    sort_dir: str = "desc"
):
    if not search_query and not agencies:
        return
    async with httpx.AsyncClient() as client:
        params = [
            ("search", search_query),
            ("skip", str(skip)),
            ("limit", str(limit)),
            ("report_id", str(report_id))
        ]
        for a in agencies:
            params.append(("agencies", a))
        if sort and sort != "Default":
            params.append(("sort", sort))
            params.append(("sort_dir", sort_dir))
        return await client.get(f"{API_URL}/table", params=params, timeout=30.0)

# ---------------------------
# Caching Wrappers
# ---------------------------
@st.cache_data
def cached_get_agencies():
    resp = asyncio.run(get_agency_list())
    if resp and resp.status_code == 200:
        return resp.json()
    return {}

@st.cache_data
def cached_get_kpis(q: str, agencies: List[str]):
    resp = asyncio.run(get_kpi_data(q, agencies))
    if resp and resp.status_code == 200:
        return resp.json()
    return []

@st.cache_data
def cached_get_chart(q: str, agencies: List[str], report_id: int):
    resp = asyncio.run(get_chart_data(q, agencies, report_id))
    if resp and resp.status_code == 200:
        return resp.json()
    return {}

@st.cache_data
def cached_get_table(q: str, skip: int, limit: int, agencies: List[str], report_id: int, sort: str = "Default", sort_dir: str = "asc"):
    resp = asyncio.run(get_table_data(q, skip, limit, agencies, report_id, sort, sort_dir))
    if resp and resp.status_code == 200:
        return resp.json()
    return {}

# ---------------------------
# Session State Defaults
# ---------------------------
if "data_refreshed" not in st.session_state:
    st.session_state["data_refreshed"] = False
if "search" not in st.session_state:
    st.session_state["search"] = ""
if "table_page" not in st.session_state:
    st.session_state["table_page"] = 1
if "keyword_query" not in st.session_state:
    st.session_state["keyword_query"] = ""
if "semantic_query" not in st.session_state:
    st.session_state["semantic_query"] = ""
if "report_id" not in st.session_state:
    st.session_state["report_id"] = 1
if "table_sort" not in st.session_state:
    st.session_state["table_sort"] = "Default"
if "table_sort_dir" not in st.session_state:
    st.session_state["table_sort_dir"] = "asc"

# ---------------------------
# Callbacks
# ---------------------------
def on_refresh_click():
    r = asyncio.run(refresh_api())
    if r and r.status_code == 200:
        st.success("Data refreshed!")
        st.session_state["data_refreshed"] = True
        last_ts = asyncio.run(get_refresh_info())
        st.write(f"Last refreshed at: {last_ts}")
    else:
        st.error("Refresh call failed.")

def on_search_submit():
    k = st.session_state["keyword_query"].strip()
    s = st.session_state["semantic_query"].strip()
    st.session_state["search"] = k if k else s
    st.session_state["table_page"] = 1

def on_report_select():
    st.session_state["table_page"] = 1

def on_sort_change():
    st.session_state["table_page"] = 1
    cached_get_table.clear()

# ---------------------------
# Sidebar - User Controls
# ---------------------------
with st.sidebar:
    st.title("Controls")
    st.button("Refresh Data", on_click=on_refresh_click)
    
    with st.form("search_form"):
        tab1, tab2 = st.tabs(["Keyword Search", "Semantic Search"])
        with tab1:
            st.text_input("Enter keyword(s)", key="keyword_query")
        with tab2:
            st.text_input("Enter semantic query", key="semantic_query", disabled=True)
        st.form_submit_button("Search", on_click=on_search_submit)
    
    # Agency filter
    agencies_info = cached_get_agencies()
    agency_list = [a["name"] for a in agencies_info.get("agencies", [])]
    selected_agencies = st.multiselect("Filter by Agency", agency_list, default=[])
    st.session_state["selected_agencies"] = selected_agencies
    
    # Report selection
    report_id_label = {1: "Core Document Stats", 2: "Section Change Stats"}
    st.session_state["report_id"] = st.selectbox(
        "Select Report",
        [1, 2],
        index=0 if st.session_state["report_id"] == 1 else 1,
        format_func=lambda x: report_id_label[x],
        on_change=on_report_select
    )
    
    # Sort Options
    sort_options = ["Default", "agency", "title", "chapter", "part", "section_count", "total_word_count"]
    st.session_state["table_sort"] = st.selectbox(
        "Sort by",
        sort_options,
        index=sort_options.index(st.session_state.get("table_sort", "Default")),
        on_change=on_sort_change
    )
    st.session_state["table_sort_dir"] = st.radio(
        "Sort direction",
        ["desc", "asc"],
        index=0 if st.session_state.get("table_sort_dir", "desc") == "desc" else 1,
        on_change=on_sort_change
    )

# ---------------------------
# Main Container - Data Displays
# ---------------------------
st.title("eCFR Analyzer")

# KPI Metrics
st.subheader("KPI Metrics")
kpi_data = cached_get_kpis(st.session_state["search"], st.session_state["selected_agencies"])
if kpi_data:
    ccols = st.columns(len(kpi_data))
    for i, met in enumerate(kpi_data):
        val = met["value"]
        lab = met["metric"]
        ccols[i].metric(lab, f"{int(val):,}")
else:
    st.write("No KPI data returned.")

# Chart
st.subheader("Chart")
chart_info = cached_get_chart(
    st.session_state["search"],
    st.session_state["selected_agencies"],
    st.session_state["report_id"]
)
if chart_info:
    # chart_info is expected to have: labels, series1, and series2.
    df_chart = pd.DataFrame({
        "Agency": chart_info["labels"],
        "Bar": chart_info["series1"],
        "Line": chart_info["series2"]
    })
    # Sort the dataframe by the bar metric (series1)
    sort_col = "Bar"
    df_chart = df_chart.sort_values(by=sort_col, ascending=False)
    
    # Create a bar chart for series1 and a line chart for series2
    if st.session_state["report_id"] == 1:
        series_1_title = 'Section Count'
        series_2_title = 'Word Count'
    else:
        series_1_title = 'Section Changes'
        series_2_title = 'Length Change'

    bar = alt.Chart(df_chart).mark_bar().encode(
        x=alt.X("Agency:N", axis=alt.Axis(labelAngle=-45)),
        y=alt.Y("Bar:Q", title=series_1_title)
    )
    line = alt.Chart(df_chart).mark_line(point=True, color="red").encode(
        x="Agency:N",
        y=alt.Y("Line:Q", title=series_2_title)
    )
    
    # Layer the charts and allow independent y scales
    layered = alt.layer(bar, line).resolve_scale(y='independent').properties(width=800, height=400)
    st.altair_chart(layered, use_container_width=True)
else:
    st.write("No chart data available.")

# Table
st.subheader("Results Table")
st.text("Select a row in the table using the checkbox in the first column.")
page_num = st.session_state["table_page"]
skip = (page_num - 1) * TABLE_PAGE_SIZE

table_resp = cached_get_table(
    st.session_state["search"],
    skip,
    TABLE_PAGE_SIZE,
    st.session_state["selected_agencies"],
    st.session_state["report_id"],
    st.session_state["table_sort"],
    st.session_state["table_sort_dir"]
)

if table_resp:
    tot = table_resp.get("total_count", 0)
    rows = table_resp.get("data", [])
    if rows:
        df = pd.DataFrame(rows)
        event = st.dataframe(
            df,
            use_container_width=True,
            hide_index=False,
            selection_mode="single-row",
            on_select="rerun",
            key="table_sel"
        )
        total_pages = ceil(tot / TABLE_PAGE_SIZE) if tot else 1
        render_pagination(total_pages, page_num, "table")
        selected_rows = event.selection.rows
        if selected_rows:
            idx = selected_rows[0]
            chosen = df.iloc[idx]
            st.subheader("Search Results - Detailed")
            st.write(f"**Title:** {chosen['title']}")
            st.write(f"**Agency:** {chosen['agency']}")
            st.write(f"**Chapter:** {chosen['chapter']}")
            st.write(f"**Part:** {chosen['part']}")
            raw_text = chosen["full_text"]
            st.markdown("---")
            st.markdown("**Section Chunk (Highlighted):**")
            show_full_text_with_highlights(raw_text, st.session_state["search"])
    else:
        st.write("No rows found in table.")
else:
    st.write("No table response received.")

st.markdown("---")
st.write("© 2025 eCFR Analyzer (Chapters).")
