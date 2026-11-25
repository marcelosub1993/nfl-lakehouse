"""Block 0: imports and configuration"""

import os

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

SILVER_SCHEMA = spark.conf.get("silver_schema")
LANDING_PATH = spark.conf.get("landing_path")
ROSTER_PATH = f"{LANDING_PATH}/rosters"
EXPECTED_TEAMS = 32

ATHLETES_SCHEMA = """
    ARRAY<STRUCT<
        position: STRING,
        items: ARRAY<STRUCT<
            id: STRING,
            fullName: STRING,
            firstName: STRING,
            lastName: STRING,
            jersey: STRING,
            dateOfBirth: STRING,
            weight: DOUBLE,
            height: DOUBLE,
            position: STRUCT<abbreviation: STRING>,
            status: STRUCT<type: STRING>,
            experience: STRUCT<years: INT>,
            college: STRUCT<name: STRING>
        >>
    >>
"""

TEAM_SCHEMA = "STRUCT<id: STRING, abbreviation: STRING>"

"""Block 1: find the snapshots"""

def list_snapshot_versions(base_path):
    """Return the sorted ingest dates found under base_path, e.g. ['2026-11-18', ...]."""
    try:
        names = [f.name.rstrip("/") for f in dbutils.fs.ls(base_path)]
    except NameError: # local Pyspark has no dbutils
        names = os.listdir(base_path)
    return sorted(n.split("=", 1)[1] for n in names if n.startswith("ingest_date="))

"""Block 2: read one snapshot, keeping one file per team"""

def read_snapshot(version):
    """Raw rows of one snapshot folder, keeping only the newest file per team"""
    raw = (
        spark.read
        .schema("athletes STRING, team STRING")
        .json(f"{ROSTER_PATH}/ingest_date={version}")
        .select(
            "athletes",
            "team",
            F.col("_metadata.file_name").alias("_file_name"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        )
    )
    newest_first = Window.partitionBy("_team_id").orderBy(F.col("_file_name").desc())
    return(
        raw
        .withColumn("_team_id", F.from_json("team", TEAM_SCHEMA)["id"])
        .withColumn("_rn", F.row_number().over(newest_first))
        .filter("_rn = 1")
        .drop("_rn", "_team_id", "_file_name")
    )

""" Block 3: parse"""

def parse_roster(raw):
    return(
        raw
        .select(
            F.explode(F.from_json("athletes", ATHLETES_SCHEMA)).alias("grp"),
            F.from_json("team", TEAM_SCHEMA)["id"].alias("team_id"),
            "_file_modified_at",
        )
        .select(
            F.explode("grp.items").alias("p"),
            F.col("grp.position").alias("roster_group"),
            "team_id",
            "_file_modified_at",
        )
        .select(
            F.col("p.id").alias("player_id"),
            F.col("p.fullName").alias("full_name"),
            F.col("p.firstName").alias("first_name"),
            F.col("p.lastName").alias("last_name"),
            F.col("team_id"),
            F.col("p.jersey").alias("jersey"),
            F.col("p.position.abbreviation").alias("position"),
            F.col("roster_group"),
            F.col("p.status.type").alias("status"),
            F.to_date(F.substring("p.dateOfBirth", 1, 10)).alias("birth_date"),
            F.col("p.weight").cast("int").alias("weight_lbs"),
            F.col("p.height").cast("int").alias("height_in"),
            F.col("p.experience.years").alias("experience_years"),
            F.col("p.college.name").alias("college"),
            F.col("_file_modified_at"),
        )
        .filter("player_id IS NOT NULL AND team_id IS NOT NULL")
    )

"""Block 4: guards (a snapshot must be complete)"""

def check_snapshot(players, version):
    teams = players.select("team_id").distinct().count()
    if teams != EXPECTED_TEAMS:
        raise ValueError(
            f"Roster snapshot {version} has {teams} teams, expected {EXPECTED_TEAMS}. "
            "Re-run the ingest for that date before refreshing"
        )
    duplicates = players.groupBy("player_id").count().filter("count > 1").count()
    if duplicates > 0:
        raise ValueError(f"Roster snapshot {version} has {duplicates} duplicated player_id values.")

"""Block 5: the source function that AUTO CDC calls"""

def next_snapshot_and_version(latest_version):
    """Return (DataFrame, version) for the next unprocessed snapshot, or None"""
    pending = [v for v in list_snapshot_versions(ROSTER_PATH)
               if latest_version is None or v > latest_version]
    if not pending:
        return None
    version = pending[0]
    players = parse_roster(read_snapshot(version))
    check_snapshot(players, version)
    return (players, version)

"""Block 6: target and flow"""

dp.create_streaming_table(
    name = f"{SILVER_SCHEMA}.players",
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