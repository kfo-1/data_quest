from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options

@dp.table(
    comment="BLS PR class codes - Employee groups"
)
@dp.expect_or_fail("valid_class_code", "class_code IS NOT NULL AND LENGTH(class_code) = 1")
@dp.expect("valid_selectable", "selectable IN ('T', 'F')")
def brz_pr_class():
    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "class_code STRING, class_text STRING, display_level INT, selectable STRING, sort_sequence INT"
    df = spark.read.options(**bls_options).schema(schema).csv('/Volumes/datasets/default/bls/pr/pr.class')
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())
    return df
