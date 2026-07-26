from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options

@dp.table(
    comment="BLS PR measure codes - Productivity metrics"
)
@dp.expect_or_fail("valid_measure_code", "measure_code IS NOT NULL AND LENGTH(measure_code) = 2")
@dp.expect("valid_selectable", "selectable IN ('T', 'F')")
def brz_pr_measure():
    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "measure_code STRING, measure_text STRING, display_level INT, selectable STRING, sort_sequence INT"
    df = spark.read.options(**bls_options).schema(schema).csv('/Volumes/datasets/default/bls/pr/pr.measure')
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())
    return df
