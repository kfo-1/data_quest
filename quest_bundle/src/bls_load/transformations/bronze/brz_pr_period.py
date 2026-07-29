from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options, clean_column_names

@dp.table(
    comment="BLS PR period codes - Time period definitions"
)
@dp.expect_or_fail("valid_period", "period IS NOT NULL")
def brz_pr_period():
    # Get catalog and schema from pipeline configuration
    catalog = spark.conf.get("source_catalog")
    schema_name = spark.conf.get("source_schema")
    
    # Build parameterized volume path
    file_path = f"/Volumes/{catalog}/{schema_name}/quest_data/bls/pr/pr.period"

    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "period STRING, period_abbr STRING, period_name STRING"
    df = spark.read.options(**bls_options).schema(schema).csv(file_path)
    
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())

    # Clean column names
    df = clean_column_names(df)
    return df
