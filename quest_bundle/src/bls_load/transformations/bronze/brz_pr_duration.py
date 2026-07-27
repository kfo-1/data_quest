from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options

@dp.table(
    comment="BLS PR duration codes - Percent changes vs indexes"
)
@dp.expect_or_fail("valid_duration_code", "duration_code IS NOT NULL AND LENGTH(duration_code) = 1")
@dp.expect("valid_selectable", "selectable IN ('T', 'F')")
def brz_pr_duration():
    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "duration_code STRING, duration_text STRING, display_level INT, selectable STRING, sort_sequence INT"
    df = spark.read.options(**bls_options).schema(schema).csv('/Volumes/datasets/default/bls/pr/pr.duration')
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())
    return df
