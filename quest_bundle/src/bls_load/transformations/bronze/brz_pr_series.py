from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options

@dp.table(
    comment="BLS PR series metadata - Time series definitions"
)
@dp.expect_or_fail("valid_series_id", "series_id IS NOT NULL AND LENGTH(series_id) = 17")
@dp.expect("valid_begin_year", "begin_year IS NOT NULL AND LENGTH(begin_year) = 4")
def brz_pr_series():
    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "series_id STRING, sector_code STRING, class_code STRING, measure_code STRING, duration_code STRING, seasonal STRING, base_year STRING, footnote_codes STRING, begin_year STRING, begin_period STRING, end_year STRING, end_period STRING"
    df = spark.read.options(**bls_options).schema(schema).csv('/Volumes/datasets/default/bls/pr/pr.series')
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())
    return df
