import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
QUALITY_DIR = PROJECT_ROOT / "data" / "quality"
PREVIEW_DIR = PROJECT_ROOT / "data" / "preview"


def export_parquet_preview(
    spark: SparkSession,
    input_path: Path,
    output_file: Path,
    limit: int = 1000,
) -> None:
    if not input_path.exists():
        print(f"Skipped. Path does not exist: {input_path}")
        return

    output_file.parent.mkdir(parents=True, exist_ok=True)

    df = spark.read.parquet(str(input_path))

    if "reg_date" in df.columns:
        df = df.orderBy("reg_date")

    pdf = df.toPandas()
    pdf.to_csv(output_file, index=False)

    print(f"Exported: {output_file}")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("export-adtech-results-to-csv")
        .master("local[*]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    marts = [
       # "mart_by_mm_dma",
       # "mart_by_site_id",
        "mart_by_hardware",
    ]

    # for mart_name in marts:
    #     export_parquet_preview(
    #         spark=spark,
    #         input_path=GOLD_DIR / mart_name,
    #         output_file=PREVIEW_DIR / f"{mart_name}.csv",
    #     )
    
    export_parquet_preview(spark = spark, 
                           input_path = PROJECT_ROOT / "data" / "silver" / "events_clean", 
                           output_file = PREVIEW_DIR / "events_clean.csv")
    export_parquet_preview(spark = spark, 
                           input_path = PROJECT_ROOT / "data" / "silver" / "impressions_clean", 
                           output_file = PREVIEW_DIR / "impressions_clean.csv")
    

    quality_tables = [
        "duplicate_impression_uids",
        "deduplicatet_impression_rows",
        "duplicate_event_pairs",
        "orphan_events",
        "run_metrics",
    ]

    # for table_name in quality_tables:
    #     export_parquet_preview(
    #         spark=spark,
    #         input_path=QUALITY_DIR / table_name,
    #         output_file=PREVIEW_DIR / "quality" / f"{table_name}.csv",
    # )

    spark.stop()


if __name__ == "__main__":
    main()