from pyspark.sql import SparkSession
from bigquery_utils import write_bq
from conf import *

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()

    df = spark.sql("""
      SELECT STRUCT(1 as a, 2 as b) as s
      UNION ALL
      SELECT NULL 
    """)

    write_bq(spark, df, GS_BUCKET_NAME, BQ_PROJECT_NAME, JSON_KEYFILE_PATH, DATASET_NAME, TABLE_NAME)

