import streamlit as st
import pandas as pd
import httpx
import asyncio
import nest_asyncio
from datetime import datetime
import altair as alt

st.set_page_config(page_title="eCFR Analyzer", layout="wide")
nest_asyncio.apply()

API_URL = "http://localhost:8000/api"
APPROVED_MODELS = ["gpt2", "bert-base-uncased"]

async def get_refresh_info():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{API_URL}/refresh")
            if response.status_code == 200:
                return response.json().get("last_refreshed", "Unknown")
    except Exception as e:
        st.error(f"Error fetching refresh info: {e}")
    return "Unknown"

async def search_api(search_query, mode):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{API_URL}/search", json={"query": search_query, "mode": mode}
            )
            if response.status_code == 200:
                return response.json()
    except Exception as e:
        st.error(f"Error during search: {e}")
    return []

async def get_kpi_data(search_query: str = ""):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{API_URL}/kpi", params={"search": search_query}, timeout=300
            )
            if response.status_code == 200:
                return response.json()
    except Exception as e:
        st.error(f"Error fetching KPI data: {e}")
    return []

async def get_chart_data(search_query: str = ""):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{API_URL}/chart", params={"search": search_query}, timeout=300
            )
            if response.status_code == 200:
                return response.json()
    except Exception as e:
        st.error(f"Error fetching chart data: {e}")
    return {}

async def get_table_data(search_query: str = ""):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{API_URL}/table", params={"search": search_query}, timeout=300
            )
            if response.status_code == 200:
                return response.json().get("data", [])
    except Exception as e:
        st.error(f"Error fetching table data: {e}")
    return []

def cached_get_kpi_data(search: str):
    return asyncio.run(get_kpi_data(search))

def cached_get_chart_data(search: str):
    return asyncio.run(get_chart_data(search))

def cached_get_table_data(search: str):
    return asyncio.run(get_table_data(search))

if "model_downloaded" not in st.session_state:
    st.session_state["model_downloaded"] = False
if "data_refreshed" not in st.session_state:
    st.session_state["data_refreshed"] = False
if "search" not in st.session_state:
    st.session_state["search"] = ""

st.markdown(
    """
    <style>
    div[role="progressbar"] { display: none; }
    .header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 10px 20px;
        background-color: #f0f2f6;
        border-bottom: 1px solid #ddd;
    }
    .header .title {
        display: flex;
        align-items: center;
    }
    .header img {
        height: 40px;
        margin-right: 10px;
    }
    .main { padding: 20px; }
    .refresh-button { margin-top: 10px; }
    .footer {
        text-align: center;
        padding: 10px;
        border-top: 1px solid #ddd;
        margin-top: 20px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown("<div class='header'>", unsafe_allow_html=True)
col_logo_title, col_model, col_refresh = st.columns([3, 3, 1])
with col_logo_title:
    st.markdown(
        """
        <div class="title">
            <img src="https://via.placeholder.com/40" alt="Logo">
            <h1>eCFR Analyzer</h1>
        </div>
        """,
        unsafe_allow_html=True
    )
with col_model:
    selected_model = st.selectbox("Select Model", APPROVED_MODELS, key="selected_model")
    if st.button("Download Model", disabled=True):
        async def download_model_async():
            try:
                async with httpx.AsyncClient(timeout=300) as client:
                    response = await client.post(
                        f"{API_URL}/download_model", json={"model_name": selected_model}
                    )
                if response.status_code == 200:
                    result = response.json()
                    if result.get("error"):
                        st.error(f"Download Error: {result.get('error')}")
                    else:
                        st.success(result.get("message"))
                        st.session_state["model_downloaded"] = True
                else:
                    st.error("Failed to reach the download API.")
            except Exception as e:
                st.error(f"Exception during download: {e}")
        asyncio.run(download_model_async())
with col_refresh:
    refresh_disabled = not st.session_state.get("model_downloaded", False)
    st.markdown("<div class='refresh-button'>", unsafe_allow_html=True)
    if st.button("Refresh", disabled=refresh_disabled):
        async def refresh_async():
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(f"{API_URL}/refresh")
                if response.status_code == 200:
                    st.success("Data refreshed!")
                    st.session_state["data_refreshed"] = True
                else:
                    st.error("Refresh failed")
            except Exception as e:
                st.error(f"Error triggering refresh: {e}")
        asyncio.run(refresh_async())
        last_refreshed = asyncio.run(get_refresh_info())
        st.markdown(
            f"<div style='text-align: right;'>Last refreshed at: {last_refreshed}</div>",
            unsafe_allow_html=True,
        )
    st.markdown("</div>", unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

st.markdown("<div class='main'>", unsafe_allow_html=True)
if 1 == 0:
    pass
# if not st.session_state["model_downloaded"] and not st.session_state["data_refreshed"]:
#     st.warning("Please download a model and refresh the data first to enable searching, charts, and data.")
else:
    with st.form("search_form", clear_on_submit=False):
        tabs = st.tabs(["Keyword", "Semantic"])
        with tabs[0]:
            query_keyword = st.text_input(
                "Enter your search query",
                key="keyword_query",
                value=st.session_state.get("search", "")
            )
        with tabs[1]:
            query_semantic = st.text_input(
                "Enter your search query",
                key="semantic_query",
                value=st.session_state.get("search", ""),
                disabled=True
            )
        submitted = st.form_submit_button("Search →")
    if submitted:
        if query_keyword.strip():
            search_mode = "keyword"
            query = query_keyword.strip()
        elif query_semantic.strip():
            search_mode = "semantic"
            query = query_semantic.strip()
        else:
            st.warning("Please enter a search query.")
            query = ""
        if query:
            st.info(f"Searching ({search_mode}): {query}")
            st.session_state["search"] = query
            search_results = asyncio.run(search_api(query, search_mode))
            st.session_state["search_results"] = search_results

    current_search = st.session_state.get("search", "")
    st.subheader("Charts")
    kpi_data = cached_get_kpi_data(current_search)
    if kpi_data and isinstance(kpi_data, list) and len(kpi_data) >= 1:
        kpi_cols = st.columns(len(kpi_data))
        for idx, col in enumerate(kpi_cols):
            col.metric(label=kpi_data[idx]["metric"], value=kpi_data[idx]["value"])
    else:
        st.write("No KPI data available.")

    chart_data = cached_get_chart_data(current_search)
    if chart_data:
        df_chart = pd.DataFrame({
            "Labels": chart_data.get("labels", []),
            "Values": chart_data.get("values", [])
        })
        if not df_chart.empty:
            chart = alt.Chart(df_chart).mark_line(point=True).encode(
                x=alt.X("Labels", axis=alt.Axis(labelAngle=45, title="Labels")),
                y=alt.Y("Values", title="Values")
            ).properties(width=700, height=400)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.write("No chart data available.")
    else:
        st.write("No chart data available.")

    st.subheader("Results Table")
    table_data = cached_get_table_data(current_search)
    if table_data:
        df_table = pd.DataFrame(table_data)
        st.dataframe(df_table)
    else:
        st.write("No table data available.")

    if "search_results" in st.session_state:
        st.subheader("Search Results")
        for res in st.session_state["search_results"]:
            st.markdown(f"**{res['title']}**")
            st.write(res['snippet'])
st.markdown("</div>", unsafe_allow_html=True)

st.markdown(
    """
    <div class="footer">
        <p>© 2025 eCFR Analyzer. All rights reserved.</p>
    </div>
    """,
    unsafe_allow_html=True
)
