from pathlib import Path
 
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
 
from etl_pipeline.schemas import EVENTS_SCHEMA, IMPRESSIONS_SCHEMA
 
 
def read_csv(
        spark: SparkSession,
        path: str | Path,
        schema: StructType
        ) -> DataFrame:
    
    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(str(path))
    )
 
 
def read_impressions(spark: SparkSession, path: str | Path) -> DataFrame:
    return read_csv(spark, path, IMPRESSIONS_SCHEMA)
 
 
def read_events(spark: SparkSession, path: str | Path) -> DataFrame:
    return read_csv(spark, path, EVENTS_SCHEMA)
