"""
DuckDB-based transformation for cricket data.

Reads raw JSON files from MinIO (raw-zone/cricket/), normalizes nested
structures, adds cricket-specific derived columns, and writes a clean
Parquet dataset back to MinIO (analytics-zone/cricket/).

Input  : s3://raw-zone/cricket/{leagues,teams_by_league,team_detail,events,events_next}/
Output : s3://analytics-zone/cricket/{leagues,teams,events}_clean.parquet
"""

from __future__ import annotations

import io
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("duckdb_process")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
RAW_BUCKET = os.getenv("RAW_ZONE_BUCKET", "raw-zone")
ANALYTICS_BUCKET = os.getenv("ANALYTICS_ZONE_BUCKET", "analytics-zone")


def build_s3_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_keys(s3_client: Any, prefix: str) -> List[str]:
    keys: List[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def download_json(s3_client: Any, key: str) -> Dict[str, Any]:
    obj = s3_client.get_object(Bucket=RAW_BUCKET, Key=key)
    body = obj["Body"].read()
    return json.loads(body.decode("utf-8")) if isinstance(body, bytes) else json.loads(body)


import json  # noqa: E402


def upload_parquet(s3_client: Any, key: str, table: pa.Table) -> None:
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    s3_client.put_object(
        Bucket=ANALYTICS_BUCKET,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info("Wrote s3://%s/%s (%d rows)", ANALYTICS_BUCKET, key, table.num_rows)


def collect_leagues(s3_client: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set = set()

    def _add(lg_obj: Dict[str, Any]) -> None:
        lid = lg_obj.get("idLeague")
        if lid and lid in seen:
            return
        if lid:
            seen.add(lid)
        rows.append(lg_obj)

    for key in list_keys(s3_client, "cricket/leagues/"):
        if not key.endswith(".json"):
            continue
        try:
            payload = download_json(s3_client, key)
        except Exception as exc:
            logger.warning("Failed to read %s: %s", key, exc)
            continue
        for lg in payload.get("leagues", []) or []:
            _add(lg)
        for lg in payload.get("countries", []) or []:
            _add(lg)
    logger.info("Collected %d league records from raw-zone (deduped)", len(rows))
    return rows


def collect_teams(s3_client: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set = set()

    def _add(team_obj: Dict[str, Any]) -> None:
        tid = team_obj.get("idTeam")
        if tid and tid in seen:
            return
        if tid:
            seen.add(tid)
        rows.append(team_obj)

    for key in list_keys(s3_client, "cricket/team_detail/"):
        if not key.endswith(".json"):
            continue
        try:
            payload = download_json(s3_client, key)
        except Exception as exc:
            logger.warning("Failed to read %s: %s", key, exc)
            continue
        for t in payload.get("teams", []) or []:
            _add(t)

    for key in list_keys(s3_client, "cricket/teams_by_league/"):
        if not key.endswith(".json"):
            continue
        try:
            payload = download_json(s3_client, key)
        except Exception as exc:
            logger.warning("Failed to read %s: %s", key, exc)
            continue
        for t in payload.get("teams", []) or []:
            _add(t)
        for lg in payload.get("leagues", []) or []:
            for t in (lg.get("teams") or []):
                _add(t) if isinstance(t, dict) else None
    logger.info("Collected %d team records from raw-zone (deduped)", len(rows))
    return rows


def collect_events(s3_client: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    seen: set = set()

    def _add(ev: Dict[str, Any]) -> None:
        eid = ev.get("idEvent")
        if eid and eid in seen:
            return
        if eid:
            seen.add(eid)
        rows.append(ev)

    for key in list_keys(s3_client, "cricket/events/"):
        if not key.endswith(".json"):
            continue
        try:
            payload = download_json(s3_client, key)
        except Exception as exc:
            logger.warning("Failed to read %s: %s", key, exc)
            continue
        for ev in payload.get("results", []) or []:
            _add(ev)

    for key in list_keys(s3_client, "cricket/events_next/"):
        if not key.endswith(".json"):
            continue
        try:
            payload = download_json(s3_client, key)
        except Exception as exc:
            logger.warning("Failed to read %s: %s", key, exc)
            continue
        for ev in payload.get("events", []) or []:
            _add(ev)
    logger.info("Collected %d event records from raw-zone (deduped)", len(rows))
    return rows


def build_duckdb() -> duckdb.DuckDBPyConnection:
    home_dir = os.path.expanduser("~") or "/tmp"
    if not os.path.isdir(home_dir):
        os.makedirs(home_dir, exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute(f"SET home_directory='{home_dir}';")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint='{os.getenv("S3_ENDPOINT", "minio:9000")}';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
        SET s3_access_key_id='{MINIO_ACCESS_KEY}';
        SET s3_secret_access_key='{MINIO_SECRET_KEY}';
        SET s3_region='us-east-1';
    """)
    return con


def process_leagues(con: duckdb.DuckDBPyConnection, rows: List[Dict[str, Any]]) -> pa.Table:
    if not rows:
        raise ValueError("No league records found in raw-zone/cricket/leagues/")
    canonical_keys = [
        "idLeague", "idAPIfootball", "idAPIfootballv3", "strSport", "strLeague",
        "strLeagueAlternate", "intDivision", "idCup", "strCurrentSeason", "intFormedYear",
        "dateFirstEvent", "strGender", "strCountry", "strWebsite", "strFacebook",
        "strInstagram", "strTwitter", "strYoutube", "strRSS", "strDescriptionEN",
    ]
    normalized = [{k: r.get(k) for k in canonical_keys} for r in rows]
    con.register("leagues_json", pa.Table.from_pylist(normalized))
    sql = """
        SELECT
            CAST(idLeague AS VARCHAR)            AS league_id,
            strLeague                            AS league_name,
            strLeagueAlternate                   AS league_alternate_name,
            strSport                             AS sport_name,
            strCurrentSeason                     AS current_season,
            strCountry                           AS country,
            TRY_CAST(intFormedYear AS INTEGER)   AS formed_year,
            dateFirstEvent                       AS first_event_date_str,
            TRY_CAST(dateFirstEvent AS DATE)     AS first_event_date,
            strWebsite                           AS website,
            strFacebook                          AS facebook,
            strTwitter                           AS twitter,
            strYoutube                           AS youtube,
            strDescriptionEN                     AS description_en,
            CURRENT_TIMESTAMP                    AS load_timestamp,
            CURRENT_DATE                         AS load_date
        FROM leagues_json
        WHERE strSport = 'Cricket' OR strSport IS NULL
    """
    return con.execute(sql).arrow()


def process_teams(con: duckdb.DuckDBPyConnection, rows: List[Dict[str, Any]]) -> pa.Table:
    if not rows:
        raise ValueError("No team records found in raw-zone/cricket/")
    canonical_keys = [
        "idTeam", "idESPN", "idAPIfootball", "intLoved", "strTeam", "strTeamAlternate",
        "strTeamShort", "intFormedYear", "strSport", "strLeague", "idLeague",
        "strLeague2", "idLeague2", "strLeague3", "idLeague3", "strLeague4", "idLeague4",
        "strLeague5", "idLeague5", "strLeague6", "idLeague6", "strLeague7", "idLeague7",
        "strDivision", "idVenue", "strStadium", "strKeywords", "strRSS", "strLocation",
        "intStadiumCapacity", "strWebsite", "strFacebook", "strTwitter", "strInstagram",
        "strYoutube", "strDescriptionEN", "strCountry", "strGender", "strBadge",
        "strBanner", "strEquipment", "strFanart1", "strFanart2", "strFanart3", "strFanart4",
    ]
    normalized = [{k: r.get(k) for k in canonical_keys} for r in rows]
    con.register("teams_json", pa.Table.from_pylist(normalized))
    sql = """
        SELECT
            CAST(idTeam AS VARCHAR)              AS team_id,
            strTeam                              AS team_name,
            strTeamShort                         AS team_short,
            strTeamAlternate                     AS team_alternate,
            strSport                             AS sport_name,
            strLeague                            AS league_name,
            CAST(idLeague AS VARCHAR)            AS league_id,
            strCountry                           AS country,
            TRY_CAST(intFormedYear AS INTEGER)   AS formed_year,
            strStadium                           AS stadium_name,
            strLocation                          AS stadium_location,
            TRY_CAST(intStadiumCapacity AS BIGINT) AS stadium_capacity,
            strWebsite                           AS website,
            strKeywords                          AS keywords,
            strBadge                             AS team_badge_url,
            strGender                            AS gender,
            strDescriptionEN                     AS description_en,
            CURRENT_TIMESTAMP                    AS load_timestamp,
            CURRENT_DATE                         AS load_date
        FROM teams_json
        WHERE strSport = 'Cricket' OR strSport IS NULL
    """
    return con.execute(sql).arrow()


def process_events(con: duckdb.DuckDBPyConnection, rows: List[Dict[str, Any]]) -> pa.Table:
    if not rows:
        raise ValueError("No event records found in raw-zone/cricket/")
    canonical_keys = [
        "idEvent", "idAPIfootball", "strTimestamp", "strEvent", "strEventAlternate",
        "strFilename", "strSport", "idLeague", "strLeague", "strLeagueBadge",
        "strSeason", "strDescriptionEN", "strHomeTeam", "strAwayTeam", "intHomeScore",
        "intRound", "intAwayScore", "intSpectators", "strOfficial", "strWeather",
        "dateEvent", "dateEventLocal", "strTime", "strTimeLocal", "strGroup",
        "idHomeTeam", "strHomeTeamBadge", "idAwayTeam", "strAwayTeamBadge", "intScore",
        "intScoreVotes", "strResult", "idVenue", "strVenue", "strCountry", "strCity",
        "strPoster", "strSquare", "strFanart", "strThumb", "strVideo", "strStatus",
    ]
    normalized = [{k: r.get(k) for k in canonical_keys} for r in rows]
    con.register("events_json", pa.Table.from_pylist(normalized))
    sql = """
        WITH src AS (
            SELECT * FROM events_json
        )
        SELECT
            CAST(idEvent AS VARCHAR)             AS event_id,
            strEvent                             AS event_name,
            strLeague                            AS league_name,
            CAST(idLeague AS VARCHAR)            AS league_id,
            strSeason                            AS season,
            strHomeTeam                          AS home_team_name,
            CAST(idHomeTeam AS VARCHAR)          AS home_team_id,
            strAwayTeam                          AS away_team_name,
            CAST(idAwayTeam AS VARCHAR)          AS away_team_id,
            TRY_CAST(intHomeScore AS INTEGER)    AS home_score,
            TRY_CAST(intAwayScore AS INTEGER)    AS away_score,
            dateEvent                            AS event_date_str,
            strTime                              AS event_time,
            TRY_CAST(dateEvent AS DATE)          AS event_date,
            strVenue                             AS venue,
            CAST(idVenue AS VARCHAR)             AS venue_id,
            strCountry                           AS country,
            strCity                              AS city,
            strStatus                            AS status,
            strResult                            AS result_description,
            strGroup                             AS group_name,
            strSport                             AS sport_name,
            strThumb                             AS thumbnail_url,
            strVideo                             AS video_url,
            CASE
                WHEN strResult IS NULL OR strResult = ''                  THEN 'no_result'
                WHEN strResult ILIKE '%tied%'                              THEN 'tied'
                WHEN strResult ILIKE '%no result%' OR strResult ILIKE '%abandoned%' OR strResult ILIKE '%cancelled%' THEN 'no_result'
                WHEN strResult ILIKE '%won by%runs%'                       THEN 'runs'
                WHEN strResult ILIKE '%won by%wickets%'                    THEN 'wickets'
                ELSE 'other'
            END AS win_margin_type,
            CASE
                WHEN strResult ILIKE '%won by%runs%'
                    THEN TRY_CAST(REGEXP_EXTRACT(strResult, 'won by ([0-9]+) runs?', 1) AS INTEGER)
                WHEN strResult ILIKE '%won by%wickets%'
                    THEN TRY_CAST(REGEXP_EXTRACT(strResult, 'won by ([0-9]+) wickets?', 1) AS INTEGER)
                ELSE NULL
            END AS win_margin_value,
            COALESCE(TRY_CAST(intHomeScore AS INTEGER), 0)
                + COALESCE(TRY_CAST(intAwayScore AS INTEGER), 0)           AS total_runs,
            CASE
                WHEN strHomeTeam IS NOT NULL AND strResult ILIKE '%' || strHomeTeam || '%' THEN 'home'
                WHEN strAwayTeam IS NOT NULL AND strResult ILIKE '%' || strAwayTeam || '%' THEN 'away'
                ELSE 'unknown'
            END AS winner,
            CASE
                WHEN strResult ILIKE '%won by%runs%'    THEN 'completed'
                WHEN strResult ILIKE '%won by%wickets%' THEN 'completed'
                WHEN strResult ILIKE '%tied%'           THEN 'completed'
                WHEN strResult IS NULL OR strResult = '' THEN 'scheduled'
                ELSE 'completed'
            END AS result_status,
            CURRENT_TIMESTAMP                    AS load_timestamp,
            CURRENT_DATE                         AS load_date
        FROM src
        WHERE strSport = 'Cricket' OR strSport IS NULL
    """
    return con.execute(sql).arrow()


def main() -> int:
    started = datetime.now(timezone.utc)
    logger.info("Starting cricket DuckDB transformation job")
    s3 = build_s3_client()

    leagues = collect_leagues(s3)
    teams = collect_teams(s3)
    events = collect_events(s3)

    con = build_duckdb()
    try:
        league_table = process_leagues(con, leagues)
        team_table = process_teams(con, teams)
        event_table = process_events(con, events)
    finally:
        con.close()

    upload_parquet(s3, "cricket/leagues_clean.parquet", league_table)
    upload_parquet(s3, "cricket/teams_clean.parquet", team_table)
    upload_parquet(s3, "cricket/events_clean.parquet", event_table)

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    logger.info(
        "Transformation complete in %.1fs: leagues=%d, teams=%d, events=%d",
        elapsed,
        league_table.num_rows,
        team_table.num_rows,
        event_table.num_rows,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
