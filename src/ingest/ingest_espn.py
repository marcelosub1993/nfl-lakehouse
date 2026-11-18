"""
Ingest raw ESPN NFL JSON into the landing volume

Usage:
    Python ingest_espn.py --volume-path /Volumes/<catalog>/<schema>/<volume> [--date YYYYMMDD] [--skip-rosters]
"""
import argparse
import requests
import time
import json
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl"
MAX_RETRIES = 3
TIMEOUT_SECONDS = 30
PAUSE_BETWEEN_CALLS = 0.5 # seconds, be polite to the API

def build_url(endpoint: str) -> str:
    """'teams' -> https://.../nfl/teams"""
    return f"{BASE_URL}/{endpoint.strip('/')}"

def build_output_path(volume_path: str, entity:str, partition:str, ts:str, suffix:str = "") -> Path:
    """<volume>/<entity>/<partition>/<entity>[_suffix]_<ts>.json"""
    name = f"{entity}_{suffix}_{ts}.json" if suffix else f"{entity}_{ts}.json"
    return Path(volume_path) / entity / partition / name

def extract_team_ids(teams_payload: dict) -> list[str]:
    """Pull the team IDs out of the /teams response."""
    teams = teams_payload["sports"][0]["leagues"][0]["teams"]
    return [t["team"]["id"] for t in teams]

def fetch_json(url: str, params:dict | None = None) -> dict:
    """GET with timeout and simple retry (1s, 2s, 4s backoff)."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as err:
            if attempt == MAX_RETRIES:
                raise
            wait = 2 ** (attempt -1)
            print(f"WARN {url} failed ({err}); retry {attempt}/{MAX_RETRIES - 1} in {wait}s")
            time.sleep(wait)

def write_json(data: dict, target: Path) -> None:
    """Write to a hidden temp file, then rename, so readers never see a partial file."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f"_tmp_{target.name}")
    tmp.write_text(json.dumps(data))
    tmp.rename(target)
    print(f"WROTE {target}")

# ------------- entry point ---------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest ESPN data into a landing volume")
    parser.add_argument("--volume-path", required=True, help="Root folder, e.g /Volumes/cat/schema/vol")
    parser.add_argument("--date", default=None, help="Scoreboard date YYYYMMDD (default: today UTC)")
    parser.add_argument("--skip-rosters", action="store_true", help="Skip the 32 roster calls")
    args = parser.parse_args()

    # Job parameters can arrive as an empty string, so treat "" like "not given"
    if not args.date:
        args.date = datetime.now(timezone.utc).strftime("%Y%m%d")
    datetime.strptime(args.date, "%Y%m%d") #fail fast on a bad format
    return args

def main() -> None:
    args = parse_args()
    now = datetime.now(timezone.utc)
    ts = now.strftime("%Y%m%dT%H%M%SZ")
    ingest_partition = f"ingest_date={now:%Y-%m-%d}"

    # 1) Teams
    teams = fetch_json(build_url("teams"))
    write_json(teams, build_output_path(args.volume_path, "teams", ingest_partition, ts))

    # 2) Scoreboard for one game date
    scoreboard = fetch_json(build_url("scoreboard"), params={"dates": args.date})
    write_json(scoreboard, build_output_path(args.volume_path, "scoreboard", f"game_date={args.date}", ts))

    # 3) Rosters, one file per team
    if not args.skip_rosters:
        for team_id in extract_team_ids(teams):
            time.sleep(PAUSE_BETWEEN_CALLS)
            roster = fetch_json(build_url(f"teams/{team_id}/roster"))
            write_json(roster, build_output_path(args.volume_path, "rosters", ingest_partition, ts, f"team_{team_id}"))

    print(f"DONE date={args.date} ts={ts}")

if __name__ == "__main__":
    main()