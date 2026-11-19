from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER_SCHEMA = spark.conf.get("silver_schema")

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

ROSTER_GROUPS = "('offense', 'defense', 'specialTeam', 'practiceSquad', 'injuredReserveOrOut', 'suspended')"

@dp.temporary_view(name="players_parsed")
@dp.expect_or_drop("valid_player_id", "player_id IS NOT NULL")
@dp.expect_or_drop("has_team", "team_id IS NOT NULL")
@dp.expect("known_roster_group", f"roster_group IN {ROSTER_GROUPS}")
def players_parsed():
    return(
        spark.readStream.table("rosters_raw")
        .select(
            F.explode(F.from_json("athletes", ATHLETES_SCHEMA)).alias("grp"),
            F.from_json("team", TEAM_SCHEMA)['id'].alias("team_id"),
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
    )

dp.create_streaming_table(
    name = f"{SILVER_SCHEMA}.players",
    comment = "NFL players with rosters history (SCD type 2)",
    table_properties = {"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target = f"{SILVER_SCHEMA}.players",
    source = "players_parsed",
    keys = ["player_id"],
    sequence_by = "_file_modified_at",
    stored_as_scd_type = 2,
    track_history_column_list = ["team_id", "jersey", "position", "roster_group", "status"],
)