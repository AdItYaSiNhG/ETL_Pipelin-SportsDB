"""
Streamlit dashboard for the cricket ETL pipeline.

Reads Parquet data from MinIO via DuckDB and renders an analytics UI
covering leagues, teams, matches, and cricket-specific win margins.

Run from the project root with:

    streamlit run scripts/streamlit_app.py
"""

from __future__ import annotations

import os
from typing import Optional

import duckdb
import pandas as pd
import streamlit as st

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
ANALYTICS_ZONE_BUCKET = os.getenv("ANALYTICS_ZONE_BUCKET", "analytics-zone")


def _host_port(endpoint: str) -> str:
    return endpoint.replace("http://", "").replace("https://", "").rstrip("/")


@st.cache_resource
def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    home_dir = os.path.expanduser("~") or "/tmp"
    os.makedirs(home_dir, exist_ok=True)
    con = duckdb.connect(database=":memory:", read_only=False)
    con.execute(f"SET home_directory='{home_dir}';")
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{_host_port(MINIO_ENDPOINT)}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    return con


@st.cache_data(ttl=600)
def load_table(_con: duckdb.DuckDBPyConnection, table: str) -> pd.DataFrame:
    base = f"s3://{ANALYTICS_ZONE_BUCKET}/cricket"
    paths = {
        "leagues": f"{base}/leagues_clean.parquet",
        "teams": f"{base}/teams_clean.parquet",
        "events": f"{base}/events_clean.parquet",
    }
    if table not in paths:
        return pd.DataFrame()
    try:
        return _con.execute(f"SELECT * FROM read_parquet('{paths[table]}')").fetchdf()
    except Exception as exc:
        st.error(f"Error reading {table}: {exc}")
        return pd.DataFrame()


def render_header() -> None:
    st.set_page_config(page_title="Cricket Analytics", page_icon="🏏", layout="wide")
    st.title("🏏 Cricket Analytics Dashboard")
    st.markdown(
        "End-to-end analytics powered by **TheSportsDB Cricket API** → **MinIO** → "
        "**DuckDB** → **dbt** → **Streamlit**."
    )


def render_kpis(leagues: pd.DataFrame, teams: pd.DataFrame, events: pd.DataFrame) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Leagues", leagues["league_id"].nunique() if "league_id" in leagues.columns and not leagues.empty else 0)
    c2.metric("Teams", teams["team_id"].nunique() if "team_id" in teams.columns and not teams.empty else 0)
    c3.metric("Matches", events["event_id"].nunique() if "event_id" in events.columns and not events.empty else 0)
    if not events.empty and "home_score" in events.columns:
        avg_total = (events["home_score"].fillna(0) + events["away_score"].fillna(0)).mean()
        c4.metric("Avg. Runs / Match", f"{avg_total:.1f}")
    else:
        c4.metric("Avg. Runs / Match", "–")


def render_league_chart(events: pd.DataFrame) -> None:
    st.subheader("Matches by League")
    if events.empty or "league_name" not in events.columns:
        st.info("No match data yet.")
        return
    counts = (
        events.groupby("league_name")["event_id"]
        .nunique()
        .reset_index(name="match_count")
        .sort_values("match_count", ascending=False)
    )
    st.bar_chart(counts, x="league_name", y="match_count", height=400)


def render_runs_by_league(events: pd.DataFrame) -> None:
    st.subheader("Average Runs per Game by League")
    if events.empty:
        st.info("No data to chart yet.")
        return
    df = events.copy()
    df["total_runs"] = df["home_score"].fillna(0) + df["away_score"].fillna(0)
    grouped = df.groupby("league_name")["total_runs"].mean().reset_index()
    st.bar_chart(grouped, x="league_name", y="total_runs", height=400)


def render_win_margin_type(events: pd.DataFrame) -> None:
    st.subheader("Win-Margin Type Distribution (cricket-specific)")
    if events.empty or "result_description" not in events.columns:
        st.info("No result_description field available.")
        return
    df = events.copy()
    df["result_description"] = df["result_description"].fillna("")
    df["margin_type"] = df["result_description"].str.lower().apply(
        lambda x: "runs"
        if "won by" in x and ("run" in x)
        else "wickets"
        if "won by" in x and ("wicket" in x or "wkt" in x)
        else "tied"
        if "tied" in x
        else "no result"
        if "no result" in x or "abandoned" in x
        else "other"
    )
    counts = df["margin_type"].value_counts().reset_index()
    counts.columns = ["margin_type", "match_count"]
    st.bar_chart(counts, x="margin_type", y="match_count", height=400)


def render_recent_matches(events: pd.DataFrame) -> None:
    st.subheader("Recent Matches")
    if events.empty or "event_date" not in events.columns:
        st.info("No match data available.")
        return
    cols = [
        "event_date",
        "league_name",
        "home_team_name",
        "home_score",
        "away_score",
        "away_team_name",
        "venue",
        "status",
        "result_description",
    ]
    cols = [c for c in cols if c in events.columns]
    df = events[cols].copy()
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df = df.sort_values("event_date", ascending=False).head(20)
    st.dataframe(df, use_container_width=True)


def render_team_explorer(teams: pd.DataFrame) -> None:
    st.subheader("Team Explorer")
    if teams.empty or "team_name" not in teams.columns:
        st.info("No team data available.")
        return
    league = st.selectbox(
        "Filter by league",
        ["All"] + sorted(teams["league_name"].dropna().unique().tolist())
        if "league_name" in teams.columns
        else ["All"],
    )
    filtered = teams if league == "All" else teams[teams["league_name"] == league]
    cols = [c for c in ["team_name", "league_name", "country", "gender", "stadium_name", "keywords", "description_en"] if c in filtered.columns]
    st.dataframe(filtered[cols], use_container_width=True, height=400)


def main() -> None:
    render_header()
    con = get_duckdb_connection()
    leagues = load_table(con, "leagues")
    teams = load_table(con, "teams")
    events = load_table(con, "events")

    render_kpis(leagues, teams, events)
    st.divider()
    render_league_chart(events)
    render_runs_by_league(events)
    render_win_margin_type(events)
    st.divider()
    render_recent_matches(events)
    st.divider()
    render_team_explorer(teams)

    with st.expander("Raw Data Viewer"):
        table = st.selectbox("Table", ["leagues", "teams", "events"])
        df = load_table(con, table)
        st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()
