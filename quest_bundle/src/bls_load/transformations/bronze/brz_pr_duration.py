from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options, clean_column_names

@dp.table(
    comment="BLS PR duration codes - Percent changes vs indexes"
)
@dp.expect_or_fail("valid_duration_code", "duration_code IS NOT NULL")
@dp.expect("valid_selectable", "selectable IN ('T', 'F')")
def brz_pr_duration():
    # Get catalog and schema from pipeline configuration
    catalog = spark.conf.get("source_catalog")
    schema_name = spark.conf.get("source_schema")
    
    # Build parameterized volume path
    file_path = f"/Volumes/{catalog}/{schema_name}/quest_data/bls/pr/pr.duration"

    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "duration_code STRING, duration_text STRING, display_level INT, selectable STRING, sort_sequence INT"
    df = spark.read.options(**bls_options).schema(schema).csv(file_path)
    
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())

    # Clean column names
    df = clean_column_names(df)
    return df
