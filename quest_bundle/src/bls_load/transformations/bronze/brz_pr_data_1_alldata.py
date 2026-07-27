from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options

@dp.table(
    comment="BLS PR complete historical data"
)
@dp.expect_or_fail("valid_series_id", "series_id IS NOT NULL AND LENGTH(series_id) = 17")
@dp.expect_or_fail("valid_year", "year >= 1900 AND year <= 2100")
@dp.expect_or_fail("valid_period", "period IS NOT NULL")
def brz_pr_data_1_alldata():
    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "series_id STRING, year INT, period STRING, value DOUBLE, footnote_codes STRING"
    df = spark.read.options(**bls_options).schema(schema).csv('/Volumes/datasets/default/bls/pr/pr.data.1.AllData')
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())
    return df
