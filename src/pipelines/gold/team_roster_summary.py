from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER_SCHEMA = spark.conf.get("silver_schema") 
GOLD_SCHEMA = spark.conf.get("gold_schema")


def join_teams_player(teams, players):
    return teams.join(players, on = "team_id", how = "left")

def build_team_roster_summary(teams, players):
    # Rename _file_modified_at from player to resolve ambiguous reference
    players = players.withColumnRenamed("_file_modified_at", "_player_file_modified_at")
    # keeps only the current version
    players = players.filter(F.col("__END_AT").isNull())

    joined_tables = join_teams_player(teams, players)
    team_roster_summary = (
        joined_tables.groupBy("team_id", "abbreviation", "display_name").agg(
            F.count(F.col("player_id")).alias("total_players"),
            F.count_if(F.col("roster_group") == 'offense').alias("offense"),
            F.count_if(F.col("roster_group") == 'defense').alias("defense"),
            F.count_if(F.col("roster_group") == 'specialTeam').alias("special_team"),
            F.count_if(F.col("roster_group") == 'practiceSquad').alias("practice_squad"),
            F.count_if(F.col("roster_group") == 'injuredReserveOrOut').alias("injured_reserve"),
            F.max(F.col("_player_file_modified_at")).alias("roster_as_of")
        ).withColumn("active_roster", F.col("offense") + F.col("defense") + F.col("special_team"))
    )

    return team_roster_summary.select(
        "team_id",
        "abbreviation",
        "display_name",
        "total_players",
        "offense",
        "defense",
        "special_team",
        "active_roster",
        "practice_squad",
        "injured_reserve",
        "roster_as_of",
    )

@dp.materialized_view(
    name= f"{GOLD_SCHEMA}.team_roster_summary",
    comment= "Summary roster by team",
    table_properties= {"quality": "gold"}
)
@dp.expect("has_players", "total_players > 0")
@dp.expect("roster_adds_up", "active_roster + practice_squad + injured_reserve = total_players")
@dp.expect("team_has_name", "display_name IS NOT NULL")
def team_roster_summary():
    return build_team_roster_summary(
        spark.read.table(f"{SILVER_SCHEMA}.teams"),
        spark.read.table(f"{SILVER_SCHEMA}.players")
    )
