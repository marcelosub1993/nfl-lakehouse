from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING_PATH = spark.conf.get("landing_path")

@dp.table(
    name = "teams_raw",
    comment = "Raw ESPN /teams responses, one row per landed file",
    table_properties = {"quality": "bronze"},
)

def teams_raw():
    return(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.partitionColumns", "ingest_date")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{LANDING_PATH}/teams/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )