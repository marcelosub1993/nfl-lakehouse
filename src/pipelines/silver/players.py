from pyspark import pipelines as dp
from pyspark.sql import functions as F

from nfl_transforms.players import (
    check_snapshot,
    keep_newest_file_per_team,
    next_version,
    parse_roster,
    snapshot_versions,
)

SILVER_SCHEMA = spark.conf.get("silver_schema")
LANDING_PATH = spark.conf.get("landing_path")
ROSTERS_PATH = f"{LANDING_PATH}/rosters"


def read_snapshot(version):
    raw = (
        spark.read
        .schema("athletes STRING, team STRING")
        .json(f"{ROSTERS_PATH}/ingest_date={version}")
        .select(
            "athletes",
            "team",
            F.col("_metadata.file_name").alias("_file_name"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        )
    )
    return keep_newest_file_per_team(raw)


def next_snapshot_and_version(latest_version):
    names = [f.name for f in dbutils.fs.ls(ROSTERS_PATH)]
    version = next_version(snapshot_versions(names), latest_version)
    if version is None:
        return None
    players = parse_roster(read_snapshot(version))
    check_snapshot(players, version)
    return (players, version)


dp.create_streaming_table(
    name=f"{SILVER_SCHEMA}.players",
    comment="NFL players with roster history (SCD type 2, from daily roster snapshots)",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_from_snapshot_flow(
    target=f"{SILVER_SCHEMA}.players",
    source=next_snapshot_and_version,
    keys=["player_id"],
    stored_as_scd_type=2,
    track_history_column_list=["team_id", "jersey", "position", "roster_group", "status"],
)