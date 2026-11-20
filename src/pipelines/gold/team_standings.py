from pyspark import pipelines as dp
from pyspark.sql import functions as F


SILVER_SCHEMA = spark.conf.get("silver_schema") 
GOLD_SCHEMA = spark.conf.get("gold_schema")

REGULAR_SEASON = 2

def team_games(games):
    """Each game becomes 2 rows: the home team's view and the away team's view"""
    finished = games.filter(F.col("is_completed") & (F.col("season_type") == REGULAR_SEASON))

    home = finished.select(
        "game_id", "season_year", "week",
        F.col("home_team_id").alias("team_id"),
        F.col("away_team_id").alias("opponent_id"),
        F.col("home_score").alias("points_for"),
        F.col("away_score").alias("points_against"),
        F.lit(True).alias("is_home")
    )

    away = finished.select(
        "game_id", "season_year", "week",
        F.col("away_team_id").alias("team_id"),
        F.col("home_team_id").alias("opponent_id"),
        F.col("away_score").alias("points_for"),
        F.col("home_score").alias("points_against"),
        F.lit(False).alias("is_home")
    )

    return home.unionByName(away)

def build_standings(games, teams):
    tg = team_games(games).withColumn(
        "result",
        F.when(F.col("points_for") > F.col("points_against"), "W")
         .when(F.col("points_for") < F.col("points_against"), "L")
         .otherwise("T"),
    )
    standings = (
        tg.groupBy("season_year", "team_id")
        .agg(
            F.count("*").alias("games_played"),
            F.count_if(F.col("result") == "W").alias("wins"),
            F.count_if(F.col("result") == "L").alias("losses"),
            F.count_if(F.col("result") == "T").alias("ties"),
            F.sum("points_for").alias("points_for"),
            F.sum("points_against").alias("points_against"),
            F.max("week").alias("last_week"),
        )
        .withColumn("point_diff", F.col("points_for") - F.col("points_against"))
        .withColumn("win_pct", F.round((F.col("wins") + 0.5 * F.col("ties")) / F.col("games_played"),3))
    )
    return standings.join(
        teams.select("team_id", "abbreviation", "display_name"), on = "team_id", how = "left"
    )

@dp.materialized_view(
    name = f"{GOLD_SCHEMA}.team_standings",
    comment = "Regular-season standings per team and season (finished games only)",
    table_properties= {"quality" : "gold"},
)
@dp.expect("team_has_name", "abbreviation IS NOT NULL")
@dp.expect("games_add_up", "wins + losses + ties = games_played")
@dp.expect("valid_win_pct", "win_pct BETWEEN 0 AND 1")
def team_standings():
    return build_standings(
        spark.read.table(f"{SILVER_SCHEMA}.games"),
        spark.read.table(f"{SILVER_SCHEMA}.teams"),
    )



