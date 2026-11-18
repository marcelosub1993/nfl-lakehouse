from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER_SCHEMA = spark.conf.get("silver_schema")

EVENTS_SCHEMA = """
ARRAY<STRUCT<
  id: STRING,
  date: STRING,
  season: STRUCT<year: INT, type: INT>,
  week: STRUCT<number: INT>,
  competitions: ARRAY<STRUCT<
    neutralSite: BOOLEAN,
    venue: STRUCT<fullName: STRING>,
    status: STRUCT<type: STRUCT<name: STRING, completed: BOOLEAN>>,
    competitors: ARRAY<STRUCT<
      homeAway: STRING,
      score: STRING,
      team: STRUCT<id: STRING>
    >>
  >>
>>
"""

@dp.temporary_view(name="games_parsed")
@dp.expect_or_fail("kickoff_parsed", "kickoff_at IS NOT NULL")
@dp.expect_or_drop("valid_game_id", "game_id IS NOT NULL")
@dp.expect_or_drop("has_both_teams", "home_team_id IS NOT NULL AND away_team_id IS NOT NULL")
@dp.expect("scores_not_negative", "coalesce(home_score, 0) >= 0 AND coalesce(away_score, 0) >=0")

def games_parsed():
    events = (
        spark.readStream.table("scoreboard_raw")
        .select(
            F.explode(F.from_json("events", EVENTS_SCHEMA)).alias("event"),
            "_file_modified_at",
        )
        .select(
            F.col("event.id").alias("game_id"),
            F.to_timestamp("event.date", "yyyy-MM-dd'T'HH:mmX").alias("kickoff_at"),
            F.col("event.season.year").alias("season_year"),
            F.col("event.season.type").alias("season_type"),
            F.col("event.week.number").alias("week"),
            F.col("event.competitions")[0].alias("comp"),
            "_file_modified_at",        
        )      
    )

    home = F.filter("comp.competitors", lambda c: c["homeAway"] == "home")[0]
    away = F.filter("comp.competitors", lambda c: c["homeAway"] == "away")[0]

    return events.select(
        "game_id",
        "kickoff_at",
        "season_year",
        "season_type",
        "week",
        F.col("comp.status.type.name").alias("status"),
        F.col("comp.status.type.completed").alias("is_completed"),
        F.col("comp.neutralSite").alias("neutral_site"),
        F.col("comp.venue.fullName").alias("stadium_name"),
        home["team"]["id"].alias("home_team_id"),
        away["team"]["id"].alias("away_team_id"),
        home["score"].cast("int").alias("home_score"),
        away["score"].cast("int").alias("away_score"),
        "_file_modified_at",
    )

dp.create_streaming_table(
    name = f"{SILVER_SCHEMA}.games",
    comment = "One row per NFL game, latest known state (SCD Type 1)",
    table_properties = {"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target= f"{SILVER_SCHEMA}.games",
    source = "games_parsed",
    keys = ["game_id"],
    sequence_by = "_file_modified_at",
    stored_as_scd_type = 1,
)