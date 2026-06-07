"""
Cricket data ingestion: TheSportsDB API -> MinIO raw-zone.

Pulls cricket leagues, teams, recent matches, and upcoming matches, then
stores raw JSON in the MinIO `raw-zone` bucket, date-partitioned.

Confirmed-working endpoints (with paid test key 3, the public free key
'1' currently returns HTTP 400 for all calls):

  /all_leagues.php                                    - all leagues
  /search_all_leagues.php?s=Cricket                   - cricket leagues only
  /lookupleague.php?id=<id>                           - single league
  /searchteams.php?t=<name>                           - team search
  /lookupteam.php?id=<id>                             - single team
  /eventslast.php?id=<team_id>                        - last events for a team
  /eventsnext.php?id=<team_id>                        - next events for a team
  /lookupleague.php?id=<id>                           - league metadata

Reference: https://www.thesportsdb.com/api.php
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
import requests
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("cricket_ingest")

THESPORTSDB_API_KEY = os.getenv("THESPORTSDB_API_KEY", "3")
BASE_URL = f"https://www.thesportsdb.com/api/v1/json/{THESPORTSDB_API_KEY}"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
RAW_ZONE_BUCKET = os.getenv("RAW_ZONE_BUCKET", "raw-zone")

CRICKET_LEAGUES: List[Dict[str, str]] = [
    {"id": "4461", "name": "Australian Big Bash League", "country": "Australia"},
    {"id": "5176", "name": "Caribbean Premier League", "country": "Barbados"},
    {"id": "5529", "name": "Bangladesh Premier League", "country": "Bangladesh"},
    {"id": "5530", "name": "Sheffield Shield", "country": "Australia"},
    {"id": "5534", "name": "Shpageeza Cricket League", "country": "Afghanistan"},
]

CRICKET_TEAM_IDS: List[str] = [
    "137142",  # England Cricket
    "137143",  # India Cricket
    "137144",  # Pakistan Cricket
    "137145",  # New Zealand Cricket
    "137146",  # Australia Cricket
    "137147",  # Afghanistan Cricket
    "137148",  # Ireland Cricket
    "137149",  # Sri Lanka Cricket
    "137150",  # South Africa Cricket
    "137151",  # West Indies Cricket
    "137152",  # Bangladesh Cricket
    "139467",  # Namibia Cricket
    "139468",  # Netherlands Cricket
    "139469",  # USA Cricket
    "139470",  # Scotland Cricket
    "139471",  # UAE Cricket
    "139472",  # Zimbabwe Cricket
    "142380",  # Uganda Cricket
    "142381",  # Oman Cricket
    "142402",  # Nepal Cricket
    "142404",  # Hong Kong Cricket
    "142405",  # Papua New Guinea Cricket
    "142406",  # Kenya Cricket
]

REQUEST_TIMEOUT = 30
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2


def build_s3_client() -> Any:
    endpoint = MINIO_ENDPOINT
    if endpoint.startswith("http://minio"):
        endpoint = "http://minio:9000"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": RETRY_ATTEMPTS, "mode": "standard"},
        ),
        region_name="us-east-1",
    )


def ensure_bucket(s3_client: Any, bucket_name: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info("Bucket '%s' already exists", bucket_name)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in {"404", "NoSuchBucket", "NotFound"}:
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info("Bucket '%s' created", bucket_name)
        else:
            raise


def fetch_json(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    url = f"{BASE_URL}/{endpoint.lstrip('/')}"
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            logger.info("GET %s params=%s (attempt %d)", url, params, attempt)
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            if not payload:
                logger.warning("Empty payload for %s", url)
                return None
            return payload
        except requests.exceptions.RequestException as exc:
            logger.warning("Request failed (%s): %s", url, exc)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF ** attempt)
            else:
                logger.error("Giving up on %s after %d attempts", url, RETRY_ATTEMPTS)
                return None
        except ValueError as exc:
            logger.error("Invalid JSON from %s: %s", url, exc)
            return None
    return None


def upload_json(
    s3_client: Any,
    bucket: str,
    data_type: str,
    data: Dict[str, Any],
    partition_subdir: Optional[str] = None,
) -> str:
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%S")
    sub = f"/{partition_subdir}" if partition_subdir else ""
    key = f"cricket/{data_type}{sub}/{date_path}/{data_type}_{timestamp}.json"
    body = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )
    logger.info("Stored s3://%s/%s (%d bytes)", bucket, key, len(body))
    return key


def ingest_leagues(s3_client: Any) -> List[str]:
    keys: List[str] = []
    payload = fetch_json("search_all_leagues.php", params={"s": "Cricket"})
    if payload:
        key = upload_json(s3_client, RAW_ZONE_BUCKET, "leagues", payload)
        keys.append(key)
    for league in CRICKET_LEAGUES:
        detail = fetch_json("lookupleague.php", params={"id": league["id"]})
        if detail and detail.get("leagues"):
            slug = league["name"].lower().replace(" ", "_")
            key = upload_json(
                s3_client, RAW_ZONE_BUCKET, "leagues", detail, partition_subdir=slug
            )
            keys.append(key)
        else:
            logger.warning("League detail not found for %s", league["name"])
    return keys


def ingest_teams(s3_client: Any) -> List[str]:
    keys: List[str] = []
    for league in CRICKET_LEAGUES:
        detail = fetch_json("lookupleague.php", params={"id": league["id"]})
        if not detail or not detail.get("leagues"):
            continue
        league_info = detail["leagues"][0]
        slug = league["name"].lower().replace(" ", "_")
        key = upload_json(
            s3_client,
            RAW_ZONE_BUCKET,
            "teams_by_league",
            detail,
            partition_subdir=slug,
        )
        keys.append(key)
    for team_id in CRICKET_TEAM_IDS:
        detail = fetch_json("lookupteam.php", params={"id": team_id})
        if detail and detail.get("teams"):
            t = detail["teams"][0]
            slug = (t.get("strTeam") or f"team_{team_id}").lower().replace(" ", "_")
            key = upload_json(
                s3_client,
                RAW_ZONE_BUCKET,
                "team_detail",
                detail,
                partition_subdir=slug,
            )
            keys.append(key)
        else:
            logger.warning("Team detail not found for id %s", team_id)
    return keys


def ingest_last_events(s3_client: Any) -> List[str]:
    keys: List[str] = []
    for team_id in CRICKET_TEAM_IDS:
        payload = fetch_json("eventslast.php", params={"id": team_id})
        if payload and payload.get("results"):
            key = upload_json(
                s3_client,
                RAW_ZONE_BUCKET,
                "events",
                payload,
                partition_subdir=f"team_{team_id}",
            )
            keys.append(key)
        else:
            logger.warning("No last events for team %s", team_id)
    return keys


def ingest_next_events(s3_client: Any) -> List[str]:
    keys: List[str] = []
    for team_id in CRICKET_TEAM_IDS:
        payload = fetch_json("eventsnext.php", params={"id": team_id})
        if payload and payload.get("events"):
            key = upload_json(
                s3_client,
                RAW_ZONE_BUCKET,
                "events_next",
                payload,
                partition_subdir=f"team_{team_id}",
            )
            keys.append(key)
        else:
            logger.warning("No next events for team %s", team_id)
    return keys


def main() -> int:
    logger.info("Starting cricket ingestion run")
    s3 = build_s3_client()
    ensure_bucket(s3, RAW_ZONE_BUCKET)

    summary: Dict[str, List[str]] = {
        "leagues": ingest_leagues(s3),
        "teams": ingest_teams(s3),
        "events_last": ingest_last_events(s3),
        "events_next": ingest_next_events(s3),
    }
    total = sum(len(v) for v in summary.values())
    logger.info(
        "Ingestion finished. Files written: leagues=%d, teams=%d, events_last=%d, events_next=%d (total=%d)",
        len(summary["leagues"]),
        len(summary["teams"]),
        len(summary["events_last"]),
        len(summary["events_next"]),
        total,
    )
    return 0 if total > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
