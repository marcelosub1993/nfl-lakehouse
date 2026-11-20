from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER_SCHEMA = spark.conf.get("silver_schema")

SPORTS_SCHEMA = """
    ARRAY<STRUCT<
        leagues: ARRAY<STRUCT<
            teams: ARRAY<STRUCT<
                team: STRUCT <
                    id: STRING,
                    abbreviation: STRING,
                    displayName: STRING,
                    shortDisplayName: STRING,
                    location: STRING,
                    name: STRING,
                    color: STRING,
                    alternateColor: STRING,
                    isActive: BOOLEAN,
                    logos: ARRAY<STRUCT<
                        href: STRING
                    >>
                >
            >>
        >>
    >>
"""

@dp.temporary_view(name="teams_parsed")
@dp.expect_or_drop("valid_team_id", "team_id IS NOT NULL")
@dp.expect_or_drop("has_abbreviation", "abbreviation IS NOT NULL")
@dp.expect("valid_color", "color RLIKE '^[0-9a-fA-F]{6}$'")

def teams_parsed():
    return(
        spark.readStream.table("teams_raw")
        .select(
            F.explode(F.from_json("sports", SPORTS_SCHEMA)[0]['leagues'][0]['teams']['team']).alias("team"),
            "_file_modified_at",
        )
        .select(
            F.col("team.id").alias("team_id"),
            F.col("team.abbreviation").alias("abbreviation"),
            F.col("team.displayName").alias("display_name"),
            F.col("team.shortDisplayName").alias("short_name"),
            F.col("team.location").alias("location"),
            F.col("team.name").alias("team_name"),
            F.col("team.color").alias("color"),
            F.col("team.alternateColor").alias("alternate_color"),
            F.col("team.isActive").alias("is_active"),
            F.col("team.logos")['href'][0].alias("logo_url"),
            F.col("_file_modified_at"),
        )
    )

dp.create_streaming_table(
    name = f"{SILVER_SCHEMA}.teams",
    comment = "One row per NFL team, latest known state (SCD Type 1)",
    table_properties = {"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target = f"{SILVER_SCHEMA}.teams",
    source = "teams_parsed",
    keys = ["team_id"],
    sequence_by = "_file_modified_at",
    stored_as_scd_type = 1,
)
