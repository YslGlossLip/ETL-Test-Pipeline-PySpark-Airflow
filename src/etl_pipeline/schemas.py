from pyspark.sql.types import StructField, StructType, IntegerType, StringType


IMPRESSIONS_SCHEMA = StructType([
    StructField("reg_time", StringType(), True),
    StructField("uid", StringType(), True),
    StructField("fc_imp_chk", IntegerType(), True),
    StructField("fc_time_chk", IntegerType(), True),
    StructField("utmtr", IntegerType(), True),
    StructField("mm_dma", IntegerType(), True),
    StructField("osName", StringType(), True),
    StructField("model", StringType(), True),
    StructField("hardware", StringType(), True),
    StructField("site_id", StringType(), True)
    ])


EVENTS_SCHEMA = StructType([
    StructField("uid", StringType(), True),
    StructField("tag", StringType(), True)
    ])