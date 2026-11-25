import json

import pytest

from nfl_transforms.players import (
    check_snapshot,
    keep_newest_file_per_team,
    next_version,
    parse_roster,
    snapshot_versions,
)

RAW_SCHEMA = "athletes STRING, team STRING, _file_name STRING, _file_modified_at TIMESTAMP"


def player(pid, name="Test Player", position="QB"):
    return {
        "id": pid,
        "fullName": name,
        "firstName": name.split()[0],
        "lastName": name.split()[-1],
        "jersey": "12",
        "dateOfBirth": "1999-05-01T07:00Z",
        "weight": 225.0,
        "height": 76.0,
        "position": {"abbreviation": position},
        "status": {"type": "active"},
        "experience": {"years": 3},
        "college": {"name": "Some College"},
    }


def raw_row(team_id, groups, file_name="rosters_team_1_20261123T171847Z.json"):
    """One raw row = one roster file: groups is {'offense': [player, ...], ...}."""
    athletes = [{"position": grp, "items": items} for grp, items in groups.items()]
    team = {"id": team_id, "abbreviation": f"T{team_id}"}
    return (json.dumps(athletes), json.dumps(team), file_name, None)


# --- snapshot versions -----------------------------------------------------

def test_snapshot_versions_sorts_and_ignores_other_folders():
    names = ["ingest_date=2026-11-23/", "_checkpoint/", "ingest_date=2026-11-18/"]
    assert snapshot_versions(names) == ["2026-11-18", "2026-11-23"]


@pytest.mark.parametrize(
    "latest, expected",
    [
        (None, "2026-11-18"),          # first call, or after a full refresh
        ("2026-11-18", "2026-11-20"),  # normal daily update
        ("2026-11-23", None),          # nothing new -> AUTO CDC stops calling
    ],
)
def test_next_version(latest, expected):
    versions = ["2026-11-18", "2026-11-20", "2026-11-23"]
    assert next_version(versions, latest) == expected


# --- one file per team -----------------------------------------------------

def test_keep_newest_file_per_team(spark):
    old = raw_row("1", {"offense": [player("10")]}, "rosters_team_1_20261123T100000Z.json")
    new = raw_row("1", {"offense": [player("11")]}, "rosters_team_1_20261123T170000Z.json")
    other = raw_row("2", {"offense": [player("20")]}, "rosters_team_2_20261123T100000Z.json")
    raw = spark.createDataFrame([old, new, other], RAW_SCHEMA)

    result = keep_newest_file_per_team(raw)

    assert result.count() == 2
    assert "_file_name" not in result.columns
    team1_athletes = result.filter("team LIKE '%\"id\": \"1\"%'").first()["athletes"]
    assert '"id": "11"' in team1_athletes  # player from the newer file


# --- parse ------------------------------------------------------------------

def test_parse_roster_flattens_groups_and_players(spark):
    row = raw_row("1", {
        "offense": [player("10", "Joe Burrow", "QB"), player("11", "Ja'Marr Chase", "WR")],
        "practiceSquad": [player("12", "Some Rookie", "TE")],
    })
    raw = spark.createDataFrame([row], RAW_SCHEMA).drop("_file_name")

    result = {r["player_id"]: r for r in parse_roster(raw).collect()}

    assert set(result) == {"10", "11", "12"}
    burrow = result["10"]
    assert burrow["team_id"] == "1"
    assert burrow["position"] == "QB"
    assert burrow["roster_group"] == "offense"
    assert str(burrow["birth_date"]) == "1999-05-01"
    assert burrow["weight_lbs"] == 225
    assert result["12"]["roster_group"] == "practiceSquad"


def test_parse_roster_drops_players_without_id(spark):
    no_id = player(None)
    row = raw_row("1", {"offense": [player("10"), no_id]})
    raw = spark.createDataFrame([row], RAW_SCHEMA).drop("_file_name")

    assert [r["player_id"] for r in parse_roster(raw).collect()] == ["10"]


# --- guards -----------------------------------------------------------------

def players_df(spark, rows):
    return spark.createDataFrame(rows, "player_id STRING, team_id STRING")


def test_check_snapshot_passes_when_complete(spark):
    players = players_df(spark, [("10", "1"), ("20", "2")])
    check_snapshot(players, "2026-11-23", expected_teams=2)  # no error


def test_check_snapshot_fails_when_a_team_is_missing(spark):
    players = players_df(spark, [("10", "1"), ("11", "1")])
    with pytest.raises(ValueError, match="has 1 teams, expected 2"):
        check_snapshot(players, "2026-11-23", expected_teams=2)


def test_check_snapshot_fails_on_duplicate_player(spark):
    players = players_df(spark, [("10", "1"), ("10", "2")])
    with pytest.raises(ValueError, match="1 duplicated player_id"):
        check_snapshot(players, "2026-11-23", expected_teams=2)