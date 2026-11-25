"""Pure transformations for silver.players (no spark, no dbutils, no dp)."""

from pyspark.sql import functions as F
from pyspark.sql import Window

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


def snapshot_versions(folder_names):
    """['ingest_date=2026-11-20/', 'ingest_date=2026-11-18/', 'x'] -> ['2026-11-18', '2026-11-20']"""
    names = [n.rstrip("/") for n in folder_names]
    return sorted(n.split("=", 1)[1] for n in names if n.startswith("ingest_date="))


def next_version(versions, latest_version):
    """The first version after latest_version, or None when there is nothing new."""
    pending = [v for v in sorted(versions) if latest_version is None or v > latest_version]
    return pending[0] if pending else None


def keep_newest_file_per_team(raw):
    """One row per team: the row from the newest file (file names end with a timestamp)."""
    newest_first = Window.partitionBy("_team_id").orderBy(F.col("_file_name").desc())
    return (
        raw
        .withColumn("_team_id", F.from_json("team", TEAM_SCHEMA)["id"])
        .withColumn("_rn", F.row_number().over(newest_first))
        .filter("_rn = 1")
        .drop("_rn", "_team_id", "_file_name")
    )


def parse_roster(raw):
    return (
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


def check_snapshot(players, version, expected_teams=EXPECTED_TEAMS):
    teams = players.select("team_id").distinct().count()
    if teams != expected_teams:
        raise ValueError(
            f"Roster snapshot {version} has {teams} teams, expected {expected_teams}. "
            "Re-run the ingest for that date before refreshing."
        )
    duplicates = players.groupBy("player_id").count().filter("count > 1").count()
    if duplicates > 0:
        raise ValueError(f"Roster snapshot {version} has {duplicates} duplicated player_id values.")