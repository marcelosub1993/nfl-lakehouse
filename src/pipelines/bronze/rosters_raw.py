from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = spark.conf.get("landing_path")

@dp.table(
    name = "rosters_raw",
    comment = "Raw ESPN /teams/{id}/roster responses, one row per team file",
    table_properties = {"quality": "bronze"},
)

def rosters_raw():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.partitionColumns", "ingest_date")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{LANDING_PATH}/rosters/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )