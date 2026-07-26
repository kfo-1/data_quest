from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options

@dp.table(
    comment="BLS PR seasonal codes - Seasonal adjustment status"
)
@dp.expect_or_fail("valid_seasonal_code", "seasonal_code IS NOT NULL AND LENGTH(seasonal_code) = 1")
def brz_pr_seasonal():
    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "seasonal_code STRING, seasonal_text STRING"
    df = spark.read.options(**bls_options).schema(schema).csv('/Volumes/datasets/default/bls/pr/pr.seasonal')
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())
    return df
