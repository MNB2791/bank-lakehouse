from pyspark import pipelines as dp
from pyspark.sql import functions as F

CATALOG = spark.conf.get("bank_catalog", "bank_dev")
LANDING = f"/Volumes/{CATALOG}/raw/landing"
ENTITIES = ["branches", "customers", "accounts", "transactions"]


def _autoloader(entity: str):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")      # keep Bronze raw: all strings
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # unexpected columns land in _rescued_data
        .load(f"{LANDING}/{entity}")
        .select(
            "*",
            F.current_timestamp().alias("_ingested_at"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        )
    )


def _make_bronze(entity: str):
    @dp.table(
        name=f"{CATALOG}.bronze.{entity}",
        comment=f"Raw {entity} ingested with Auto Loader",
        table_properties={"quality": "bronze"},
    )
    def _bronze():
        return _autoloader(entity)


for _e in ENTITIES:
    _make_bronze(_e)