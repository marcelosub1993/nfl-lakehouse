from pyspark import pipelines as dp

from nfl_transforms.roster_summary import build_team_roster_summary

SILVER_SCHEMA = spark.conf.get("silver_schema")
GOLD_SCHEMA = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.team_roster_summary",
    comment="Summary roster by team",
    table_properties={"quality": "gold"},
)
@dp.expect("has_players", "total_players > 0")
@dp.expect("roster_adds_up", "active_roster + practice_squad + injured_reserve = total_players")
@dp.expect("team_has_name", "display_name IS NOT NULL")
def team_roster_summary():
    return build_team_roster_summary(
        spark.read.table(f"{SILVER_SCHEMA}.teams"),
        spark.read.table(f"{SILVER_SCHEMA}.players"),
    )