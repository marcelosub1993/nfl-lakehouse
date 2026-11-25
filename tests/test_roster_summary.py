from datetime import datetime

from nfl_transforms.roster_summary import build_team_roster_summary

TEAMS_SCHEMA = "team_id STRING, abbreviation STRING, display_name STRING, _file_modified_at TIMESTAMP"
PLAYERS_SCHEMA = (
    "player_id STRING, team_id STRING, roster_group STRING, "
    "_file_modified_at TIMESTAMP, __END_AT STRING"
)
NOV_23 = datetime(2026, 11, 23, 17, 18)


def test_roster_summary_counts_only_current_players(spark):
    teams = spark.createDataFrame(
        [("1", "CIN", "Cincinnati Bengals", NOV_23), ("2", "KC", "Kansas City Chiefs", NOV_23)],
        TEAMS_SCHEMA,
    )
    players = spark.createDataFrame(
        [
            ("10", "1", "offense", NOV_23, None),
            ("11", "1", "defense", NOV_23, None),
            ("12", "1", "practiceSquad", NOV_23, None),
            ("13", "1", "injuredReserveOrOut", NOV_23, None),
            ("14", "1", "offense", NOV_23, "2026-11-20"),  # released: closed SCD2 row
        ],
        PLAYERS_SCHEMA,
    )

    result = {r["team_id"]: r for r in build_team_roster_summary(teams, players).collect()}

    cin = result["1"]
    assert cin["total_players"] == 5  # the released player is not counted
    assert cin["active_roster"] == 2
    assert cin["active_roster"] + cin["practice_squad"] + cin["injured_reserve"] == cin["total_players"]
    assert cin["roster_as_of"] == NOV_23


def test_roster_summary_keeps_team_without_players(spark):
    teams = spark.createDataFrame([("2", "KC", "Kansas City Chiefs", NOV_23)], TEAMS_SCHEMA)
    players = spark.createDataFrame([], PLAYERS_SCHEMA)

    [kc] = build_team_roster_summary(teams, players).collect()

    assert kc["total_players"] == 0  # left join keeps the team
    assert kc["roster_as_of"] is None