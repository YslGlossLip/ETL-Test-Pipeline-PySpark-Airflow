import os
import argparse
import logging
import sys
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_PATH = PROJECT_ROOT / "src"

sys.path.append(str(SRC_PATH))



from etl_pipeline.config import (
    BRONZE_DIR,
    EVENTS_FILE_NAME,
    EVENTS_INPUT_PATH,
    GOLD_DIR,
    IMPRESSIONS_FILE_NAME,
    IMPRESSIONS_INPUT_PATH,
    MART_DEFINITIONS,
    QUALITY_DIR,
    REQUIRED_EVENT_COLUMNS,
    REQUIRED_IMPRESSION_COLUMNS,
    SILVER_DIR
)

from etl_pipeline.quality import (
    build_run_metrics,
    check_required_columns,
    bronze_layer_exists,
    find_duplicated_event_pairs,
    find_duplicated_impression_uids,
    find_orphan_events
)

from etl_pipeline.read import read_events, read_impressions

from etl_pipeline.transform import (
    add_bronze_metadata,
    build_all_marts,
    deduplicate_impressions,
    filter_impressions_by_process_date,
    prepare_events,
    prepare_impressions
)

from etl_pipeline.write import write_all_marts, write_parquet


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s |%(message)s"
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build daily AdTech marts from impressions and events."
    )
    parser.add_argument(
        "--process-date",
        required=False,
        help="Date to process in YYYY-MM-DD format. If omitted, all dates are processed."
    )
    return parser.parse_args()


def create_spark_session() -> SparkSession:
    logger.info("Creating SparkSession")

    try:
        spark = (
            SparkSession.builder
            .appName("etl_pipeline_daily")
            .master("local[*]")
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .getOrCreate()
        )
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        logger.info("SparkSession created successfully.")

        return spark
    
    except Exception:
            logger.exception("Failed to create SparkSession")
            raise




def main() -> None:

    args = parse_args()
    process_date = args.process_date
    metrics_process_date = process_date if process_date is not None else "ALL"

    spark = create_spark_session()

    logger.info(f"Pipeline started. process_date = {metrics_process_date}")

    logger.info("Reading input files")
    impressions_raw = read_impressions(spark, IMPRESSIONS_INPUT_PATH)
    events_raw = read_events(spark, EVENTS_INPUT_PATH)

    logger.info("Checking required columns")
    check_required_columns(
        impressions_raw,
        REQUIRED_IMPRESSION_COLUMNS,
        "impressions_raw"
    )
    check_required_columns(
        events_raw,
        REQUIRED_EVENT_COLUMNS,
        "events_raw"
    )


    logger.info("Counting input rows")
    metrics = {
        "impressions_input_rows": impressions_raw.count(),
        "events_input_rows": events_raw.count()
    }

    logger.info("Input rows count: impressions = %s, events = %s",
                metrics["impressions_input_rows"],
                metrics["events_input_rows"]
                )

    if bronze_layer_exists():
        logger.info("Bronze Layer alreay exists. Skipping bronze layer step.")
        impressions_bronze = spark.read.parquet(str(BRONZE_DIR / "impressions"))
        events_bronze = spark.read.parquet(str(BRONZE_DIR / "events"))
    else:
        logger.info("Building bronze layer")
        impressions_bronze = add_bronze_metadata(
            impressions_raw,
            IMPRESSIONS_FILE_NAME,
            metrics_process_date
        )
        events_bronze = add_bronze_metadata(
            events_raw,
            EVENTS_FILE_NAME,
            metrics_process_date
        )

        logger.info("Writing bronze layer")
        write_parquet(impressions_bronze, BRONZE_DIR / "impressions")
        write_parquet(events_bronze, BRONZE_DIR / "events")
       

    logger.info("Building silver layer")

    events_silver = prepare_events(events_bronze)

    impressions_silver_all_before_dedup = prepare_impressions(impressions_bronze)

    total_duplicated_impression_uids = find_duplicated_impression_uids(impressions_silver_all_before_dedup)

    _, total_deduplicated_impression_rows = deduplicate_impressions(impressions_silver_all_before_dedup)


    orphan_events = find_orphan_events(impressions_silver_all_before_dedup, events_silver)

    impressions_silver_before_dedup = filter_impressions_by_process_date(
        impressions_silver_all_before_dedup, process_date)
   
    duplicate_impression_uids = find_duplicated_impression_uids(impressions_silver_before_dedup)

    duplicate_event_pairs = find_duplicated_event_pairs(events_silver)


    impressions_silver, deduplicated_impression_rows = deduplicate_impressions(
        impressions_silver_before_dedup
    )
    events_silver_for_process = events_silver.join(
        impressions_silver.select("uid").dropDuplicates(),
        on="uid", how="left_semi")
    
    orphan_events_for_process = find_orphan_events(impressions_silver, events_silver_for_process)
    duplicate_event_pairs_for_process = find_duplicated_event_pairs(events_silver_for_process)


   
    logger.info("Writing Silver Layer")

    write_parquet(
        duplicate_impression_uids
        .withColumn("process_date", F.lit(metrics_process_date))
        , QUALITY_DIR / "duplicate_impression_uids"
        , partition_cols=["process_date"]
    )
    write_parquet(
        deduplicated_impression_rows
        .withColumn( "process_date", F.lit(metrics_process_date))
        , QUALITY_DIR / "deduplicated_impression_rows"
        , partition_cols=["process_date"]
    )
    write_parquet(
        duplicate_event_pairs_for_process
        .withColumn("process_date", F.lit(metrics_process_date))
        , QUALITY_DIR / "duplicate_event_pairs"
        , partition_cols=["process_date"]
    )
    write_parquet(
        orphan_events
        .withColumn("process_date", F.lit(metrics_process_date))
        , QUALITY_DIR / "orphan_events"
        , partition_cols=["process_date"]
    )

    write_parquet(
        impressions_silver
        , SILVER_DIR / "impressions_clean"
        , partition_cols=["reg_date"]
    )
    write_parquet(
        events_silver_for_process
        , SILVER_DIR / "events_clean"
        , partition_cols=["reg_date"])
    logger.info("Writing Silver Layer is finished")

    logger.info("Writing Data Marts")
    marts = build_all_marts(
        impression_clean=impressions_silver,
        events_df=events_silver,
        mart_definitions=MART_DEFINITIONS
    )

    write_all_marts(marts, GOLD_DIR)

    metrics.update({
        "total_duplicated_impression_uids": total_duplicated_impression_uids.count(),
        "total_deduplicated_impression_rows": total_deduplicated_impression_rows.count(),
        "total_orphan_events": orphan_events.count(),
        "duplicated_event_pairs": duplicate_event_pairs.count(), ## might be right
        "impressions_silver_rows_for_process_date": impressions_silver.count(),
        "events_for_proccesed_date": events_silver_for_process.count(),
        "duplicated_impression_uids_for_process_date": duplicate_impression_uids.count(),
        "duplicated_impression_rows_for_process_date": deduplicated_impression_rows.count(),
        "duplicated_event_pair_groups_for_process_date": duplicate_event_pairs_for_process.count(),
        "orphan_event_rows_for_process_date": orphan_events_for_process.count()
    })

    for mart_name, mart_df in marts.items():
        metrics[f"{mart_name}_rows"] = mart_df.count()

    metrics_df = build_run_metrics(
        spark=spark,
        process_date=metrics_process_date,
        metrics=metrics
    )

    write_parquet(
        metrics_df
        .withColumn("process_date",F.lit(metrics_process_date))
        , QUALITY_DIR / "run_metrics"
        , partition_cols=["process_date"]
        )

    logger.info("Pipeline finished successfully.")
    for metric_name, metric_value in metrics.items():
        logger.info("%s=%s", metric_name, metric_value)

    spark.stop()


if __name__ == "__main__":
    main()
