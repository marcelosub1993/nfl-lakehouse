"""Pure transformation for gold.team_roster_summary."""

from pyspark.sql import functions as F


def build_team_roster_summary(teams, players):
    # Rename _file_modified_at from players to avoid an ambiguous reference after the join
    players = players.withColumnRenamed("_file_modified_at", "_player_file_modified_at")
    # Keep only the current version of each player
    players = players.filter(F.col("__END_AT").isNull())

    joined = teams.join(players, on="team_id", how="left")
    summary = (
        joined.groupBy("team_id", "abbreviation", "display_name").agg(
            F.count(F.col("player_id")).alias("total_players"),
            F.count_if(F.col("roster_group") == "offense").alias("offense"),
            F.count_if(F.col("roster_group") == "defense").alias("defense"),
            F.count_if(F.col("roster_group") == "specialTeam").alias("special_team"),
            F.count_if(F.col("roster_group") == "practiceSquad").alias("practice_squad"),
            F.count_if(F.col("roster_group") == "injuredReserveOrOut").alias("injured_reserve"),
            F.max(F.col("_player_file_modified_at")).alias("roster_as_of"),
        ).withColumn("active_roster", F.col("offense") + F.col("defense") + F.col("special_team"))
    )

    return summary.select(
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