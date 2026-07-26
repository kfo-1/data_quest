from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options

@dp.table(
    comment="BLS PR period codes - Time period definitions"
)
@dp.expect_or_fail("valid_period", "period IS NOT NULL AND LENGTH(period) = 3")
def brz_pr_period():
    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "period STRING, period_abbr STRING, period_name STRING"
    df = spark.read.options(**bls_options).schema(schema).csv('/Volumes/datasets/default/bls/pr/pr.period')
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())
    return df
