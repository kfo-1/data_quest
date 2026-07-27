from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options

@dp.table(
    comment="BLS PR footnote codes - Data annotations"
)
@dp.expect_or_fail("valid_footnote_code", "footnote_code IS NOT NULL")
def brz_pr_footnote():
    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "footnote_code STRING, footnote_text STRING"
    df = spark.read.options(**bls_options).schema(schema).csv('/Volumes/datasets/default/bls/pr/pr.footnote')
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())
    return df
