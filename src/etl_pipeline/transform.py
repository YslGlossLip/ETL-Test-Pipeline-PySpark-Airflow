from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
 
 
def add_bronze_metadata(
        df: DataFrame, 
        source_file: str,
        process_date: str
        ) -> DataFrame:

    return (
        df
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.lit(source_file))
        .withColumn("pipeline_process_date", F.lit(process_date))
        )
 
def prepare_impressions(impressions_df: DataFrame) -> DataFrame:

    impressions_df = (
        impressions_df
        .withColumn(
            "hardware",
            F.when(
                F.col("hardware").isNull() | (F.trim(F.col("hardware")) == ""),
                F.lit("unknown")
            ).otherwise(F.lower(F.trim(F.col("hardware"))))
        )
        .withColumn(
            "osName",
            F.when(
                F.col("osName").isNull() | (F.trim(F.col("osName")) == ""),
                F.lit("unknown")
            ).otherwise(F.lower(F.trim(F.col("osName"))))
        )
        .withColumn(
            "model",
            F.when(
                F.col("model").isNull() | (F.trim(F.col("model")) == ""),
                F.lit("unknown")
            ).otherwise(F.lower(F.trim(F.col("model"))))
        )

    )


    impressions_df = impressions_df.withColumn(
        "hardware",
        F.regexp_replace(F.col("hardware"), r"\+", " ")
    )

    impressions_df = impressions_df.withColumn(
        "hardware",
        F.when(F.col("hardware").isin("mobile phone", "mobile"), F.lit("mobile_phone"))
        .when(F.col("hardware") == "desktop", F.lit("desktop"))
        .when(F.col("hardware") == "tablet", F.lit("tablet"))
        .when(F.col("hardware") == "media player", F.lit("connected_device"))
        .when(F.col("hardware").isin("refrigerator", "digital home assistant"), F.lit("iot_device"))
        .when(F.col("hardware") == "data collection terminal", F.lit("other_device"))
        .otherwise(F.col("hardware"))
    )



    return (
        impressions_df
        .select(
            "reg_time",
            "uid",
            "fc_imp_chk",
            "fc_time_chk",
            "utmtr",
            "mm_dma",
            "osName",
            "model",
            "hardware",
            "site_id"
            )
        .withColumn(
            "site_id",
            F.regexp_replace(
                F.lower(F.trim(F.col("site_id"))),
                r"^www\.",
                ""
            )
        )
        .withColumn(
            "reg_time_ts",
            F.to_timestamp(F.col("reg_time"), "yyyy-MM-dd HH:mm:ss") ##mandatory??
            )
        .withColumn("reg_date", F.to_date(F.col("reg_time_ts")))
        )
 
def prepare_events(events_df: DataFrame) -> DataFrame:

    return (
        events_df
        .select("uid", "tag")
        .withColumn("uid", F.trim(F.col("uid")))
        .withColumn("tag", F.lower(F.trim(F.col("tag"))))
    )



def _is_filled(col_name: str):
    normalized = F.lower(F.trim(F.col(col_name).cast("string")))

    return (
        F.col(col_name).isNotNull()
        & (~normalized.isin("", "null", "none", "nan", "unknown"))
    )

def build_completeness_score(columns: list[str]):
    score = F.lit(0)

    for col_name in columns:
        score = score + F.when(_is_filled(col_name), F.lit(1)).otherwise(F.lit(0))

    return score


def deduplicate_impressions(impressions_df: DataFrame) -> tuple[DataFrame, DataFrame]:

    quality_columns = [
        "site_id",
        "hardware",
        "osName",
        "model",
        "mm_dma",
        "utmtr",
        "fc_imp_chk",
        "fc_time_chk",
        "reg_time_ts",
    ]

    window = Window.partitionBy("uid").orderBy(
        F.desc("_quality_score"),
        F.col("reg_time_ts").desc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
        F.col("hardware").asc_nulls_last(),
        F.col("osName").asc_nulls_last(),
        F.col("model").asc_nulls_last(),
        F.col("mm_dma").asc_nulls_last(),
    )

    ranked = (
        impressions_df
        .withColumn("_quality_score", build_completeness_score(quality_columns))
        .withColumn("_dedup_rank", F.row_number().over(window))
    )

    canonical = (
        ranked
        .filter(F.col("_dedup_rank") == 1)
        .drop("_quality_score", "_dedup_rank")
    )

    duplicate_losers = (
        ranked
        .filter(F.col("_dedup_rank") > 1)
    )

    return canonical, duplicate_losers


def filter_impressions_by_process_date(
        impressions_df: DataFrame,
        process_date: str | None
        ) -> DataFrame:
    
    if process_date is None:
        return impressions_df

    return impressions_df.filter(F.col("reg_date") == F.to_date(F.lit(process_date))) 

def build_mart(
        impression_clean: DataFrame,
        events_df: DataFrame,
        dimension_col: str
        ) -> DataFrame:

    group_cols = ["reg_date", dimension_col]

    impression_counts = (
        impression_clean
        .groupBy(*group_cols)
        .agg(F.count("uid").alias("impression_count"))
    )

    events_with_dimensions = (
        events_df
        .select("uid", "tag")
        .join(impression_clean, on="uid", how="inner")
    )

    event_counts = (
        events_with_dimensions
        .groupBy(*group_cols)
        .pivot("tag")
        .agg(F.count(F.lit(1)))
        .fillna(0)
    )

    return (
        impression_counts
        .join(event_counts, on=group_cols, how="left")
        .fillna(0)
        .orderBy(*group_cols)
    )


def build_all_marts(
        impression_clean: DataFrame,
        events_df: DataFrame,
        mart_definitions: dict[str, str]
        ) -> dict[str, DataFrame]:

    return {
        mart_name: build_mart(
            impression_clean=impression_clean,
            events_df=events_df,
            dimension_col=dimension_col
        )
        for mart_name, dimension_col in mart_definitions.items()
    }