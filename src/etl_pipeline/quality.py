from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
import logging

from etl_pipeline.config import BRONZE_DIR

logger = logging.getLogger(__name__)



def check_required_columns(
        df: DataFrame,
        required_columns: list[str],
        dataframe_name: str
        ) -> None:
    
    missing_columns = [col for col in required_columns if col not in df.columns]
 
    if missing_columns:
        logger.error(f"{dataframe_name} is missing required columns: {missing_columns}" )
        raise ValueError(f"{dataframe_name} is missing required columns: {missing_columns}")
 
def bronze_layer_exists() -> bool:
    return (
        (BRONZE_DIR / "impressions").exists()
        and (BRONZE_DIR / "events").exists()
    )

def find_duplicated_impression_uids(impressions_df: DataFrame) -> DataFrame:
    
    return (
        impressions_df
        .select("uid")
        .groupBy("uid")
        .agg(F.count("*").alias("impression_count"))
        .filter(F.col("impression_count") > 1)
    )
 
def find_duplicated_event_pairs(events_df: DataFrame) -> DataFrame:

    return(
        events_df
        .groupBy("uid", "tag")
        .agg(F.count("*").alias("event_count"))
        .filter(F.col("event_count") > 1)
    )
 
def find_orphan_events(
        impressions_df: DataFrame,
        events_df: DataFrame
        ) -> DataFrame:
    
    impression_uids = impressions_df.select("uid").dropDuplicates()
 
    return events_df.join(impression_uids, on="uid", how="left_anti")
 
 
def build_run_metrics(
        spark: SparkSession,
        process_date: str,
        metrics: dict[str, object] ## ?
        ) -> DataFrame:
    """Convert pipeline metrics into a small Spark DataFrame."""

    rows = [
        (process_date, metric_name, str(metric_value))
        for metric_name, metric_value in metrics.items()
    ]
 
    return spark.createDataFrame(
        rows,
        schema=["process_date", "metric_name", "metric_value"]
        )