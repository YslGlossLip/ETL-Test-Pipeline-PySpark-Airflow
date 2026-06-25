from pathlib import Path
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

def write_parquet(
        df: DataFrame,
        path: str | Path,
        mode: str = "overwrite",
        partition_cols: list[str] | None = None
        ) -> None:

    try:
        writer = df.write.mode(mode) # еременая в которую кладем обьект DataFrameWriter

        if partition_cols:
            writer = writer.partitionBy(*partition_cols) # чтобы тут использовать

        writer.parquet(str(path))
        logger.info(f"DataFrame successfully written to {path} with mode {mode}")
    except Exception:
        logger.exception(f"Failed to write DataFrame to Parquet. path = {path}")
        raise

def write_all_marts(
        marts: dict[str, DataFrame],
        gold_dir: str | Path
        ) -> None:

    for mart_name, mart_df in marts.items():
        write_parquet(
            df=mart_df,
            path=Path(gold_dir) / mart_name,
            partition_cols=["reg_date"]
            )
