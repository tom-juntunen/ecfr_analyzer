import streamlit as st
import pandas as pd
import httpx
import asyncio
import nest_asyncio
from math import ceil
import altair as alt
import re
from hashlib import md5
from typing import Any, Dict, List, Tuple

st.set_page_config(
    page_title="eCFR Analyzer",
    layout="wide",
    page_icon="🔎"
)

nest_asyncio.apply()

API_URL = "http://localhost:8000/api"
APPROVED_MODELS = ["gpt2", "bert-base-uncased"]
TABLE_PAGE_SIZE = 50

# Helper: highlight keywords
def highlight_keyword(text: str, keyword: str) -> Tuple[str, bool]:
    if not keyword:
        return text, False
    pattern = re.compile(re.escape(keyword), re.IGNORECASE)
    first_found = False

    def replacer(m):
        nonlocal first_found
        highlighted = f"<span style='background-color: yellow; font-weight: bold;'>{m.group(0)}</span>"
        return highlighted

    replaced = pattern.sub(replacer, text)
    return replaced, first_found

def show_full_text_with_highlights(text: str, query: str):
    """
    Beautify the text, highlight the query, then display in markdown.
    """
    h, found = highlight_keyword(text, query)
    st.markdown(h, unsafe_allow_html=True)

# Pagination
def render_pagination(total_pages: int, current_page: int, key_prefix: str):
    if total_pages <= 1:
        return
    if total_pages <= 10:
        page_range = list(range(1, total_pages+1))
    else:
        if current_page <= 5:
            page_range = list(range(1,8)) + ["...", total_pages]
        elif current_page >= total_pages-4:
            page_range = [1, "..."] + list(range(total_pages-6,total_pages+1))
        else:
            page_range = [1, "..."] + list(range(current_page-2,current_page+3)) + ["...", total_pages]

    cols = st.columns(len(page_range)+2)
    if current_page > 1:
        if cols[0].button("<<", key=f"{key_prefix}_prev"):
            st.session_state[f"{key_prefix}_page"] = current_page - 1
    else:
        cols[0].write("")

    idx=1
    for p in page_range:
        if p == "...":
            cols[idx].write("...")
        else:
            if p == current_page:
                cols[idx].write(f"**{p}**")
            else:
                if cols[idx].button(str(p), key=f"{key_prefix}_page_{p}"):
                    st.session_state[f"{key_prefix}_page"] = p
        idx+=1

    if current_page < total_pages:
        if cols[-1].button(">>", key=f"{key_prefix}_next"):
            st.session_state[f"{key_prefix}_page"] = current_page + 1
    else:
        cols[-1].write("")

# Async calls
async def refresh_api():
    async with httpx.AsyncClient() as client:
        return await client.get(f"{API_URL}/refresh")

async def get_refresh_info():
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{API_URL}/refresh")
        if r.status_code == 200:
            return r.json().get("last_refreshed", "")
    return ""

async def get_agency_list():
    async with httpx.AsyncClient() as client:
        return await client.get(f"{API_URL}/agency")

async def get_kpi_data(search_query: str, agencies: List[str]):
    async with httpx.AsyncClient() as client:
        params = [("search", search_query)]
        for a in agencies:
            params.append(("agencies", a))
        return await client.get(f"{API_URL}/kpi", params=params)

async def get_chart_data(search_query: str, agencies: List[str]):
    async with httpx.AsyncClient() as client:
        params = [("search", search_query)]
        for a in agencies:
            params.append(("agencies", a))
        return await client.get(f"{API_URL}/chart", params=params)

async def get_table_data(search_query: str, skip: int, limit: int, agencies: List[str]):
    async with httpx.AsyncClient() as client:
        params = [
            ("search", search_query),
            ("skip", str(skip)),
            ("limit", str(limit))
        ]
        for a in agencies:
            params.append(("agencies", a))
        return await client.get(f"{API_URL}/table", params=params)

# Caching wrappers
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
def cached_get_chart(q: str, agencies: List[str]):
    resp = asyncio.run(get_chart_data(q, agencies))
    if resp and resp.status_code == 200:
        return resp.json()
    return {}

@st.cache_data
def cached_get_table(q: str, skip: int, limit: int, agencies: List[str]):
    resp = asyncio.run(get_table_data(q, skip, limit, agencies))
    if resp and resp.status_code == 200:
        return resp.json()
    return {}

# Session defaults
# if "model_downloaded" not in st.session_state:
#     st.session_state["model_downloaded"] = False
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

# Callbacks
def on_refresh_click():
    # if not st.session_state["model_downloaded"]:
    #     st.warning("Please download a model first.")
    #     return
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
    if k:
        st.session_state["search"] = k
        st.info(f"Keyword search for {k}")
    elif s:
        st.session_state["search"] = s
        st.info(f"Semantic search for {s}")
    else:
        st.session_state["search"] = ""
    st.session_state["table_page"] = 1

# Layout
st.title("eCFR Analyzer")

col1, col2 = st.columns([3,1])

st.button("Refresh Data", on_click=on_refresh_click)

with st.form("search_form"):
    tab1, tab2 = st.tabs(["Keyword Search", "Semantic Search"])
    with tab1:
        st.text_input("Enter keyword(s)", key="keyword_query", value=st.session_state["keyword_query"])
    with tab2:
        st.text_input("Enter semantic query", key="semantic_query", value=st.session_state["semantic_query"], disabled=True)
    st.form_submit_button("Search", on_click=on_search_submit)

# Agency filter
agencies_info = cached_get_agencies()
agency_list = [a["display_name"] for a in agencies_info.get("agencies", [])]
selected_agencies = st.multiselect("Filter by Agency", agency_list, default=[])
st.session_state["selected_agencies"] = selected_agencies

# KPIs
st.subheader("KPI Metrics")
kpi_data = cached_get_kpis(st.session_state["search"], selected_agencies)
if kpi_data:
    ccols = st.columns(len(kpi_data))
    for i, met in enumerate(kpi_data):
        val = met["value"]
        lab = met["metric"]
        if lab in ["Section Count", "Word Count"]:
            ccols[i].metric(lab, f"{int(val):,}")
        elif lab == "Change Rate":
            ccols[i].metric(lab, f"{val:.1f}%")
        elif lab == "Alignment Score":
            ccols[i].metric(lab, f"{val:.1f}")
        else:
            ccols[i].metric(lab, str(val))
else:
    st.write("No KPI data returned.")

# Chart
st.subheader("Chart")
chart_info = cached_get_chart(st.session_state["search"], selected_agencies)
if chart_info:
    labs = chart_info.get("labels", [])
    vals = chart_info.get("values", [])
    if labs and vals and len(labs) == len(vals):
        cdf = pd.DataFrame({"Agency": labs, "SectionCount": vals})
        cdf = cdf.sort_values("SectionCount", ascending=False)
        ch = (
            alt.Chart(cdf)
            .mark_line(point=True)
            .encode(
                x=alt.X(
                    "Agency", 
                    sort=alt.SortField(field="SectionCount", order="ascending"),
                    axis=alt.Axis(labelAngle=-45, labelPadding=10, labelLimit=300, labelOverlap=False)  # Increased padding for more room
                ),
                y="SectionCount"
            )
            .properties(width=1000, height=500)  # Increase width for extra space
        )
        st.altair_chart(ch, use_container_width=True)
    else:
        st.write("No chart data or mismatch in lengths.")
else:
    st.write("No chart data available.")


# Table
st.subheader("Results Table")
page_num = st.session_state["table_page"]
skip = (page_num - 1) * TABLE_PAGE_SIZE
table_resp = cached_get_table(st.session_state["search"], skip, TABLE_PAGE_SIZE, selected_agencies)

if table_resp:
    tot = table_resp.get("total_count", 0)
    rows = table_resp.get("data", [])
    if rows:
        df = pd.DataFrame(rows)
        # for display
        df_disp = df.copy()
        if "section_count" in df_disp.columns:
            df_disp["section_count"] = df_disp["section_count"].apply(lambda x: f"{x:,}")
        if "total_word_count" in df_disp.columns:
            df_disp["total_word_count"] = df_disp["total_word_count"].apply(lambda x: f"{x:,}")

        # Show the table
        event = st.dataframe(
            df_disp,
            use_container_width=True,
            hide_index=True,
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
