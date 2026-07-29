from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options, clean_column_names

@dp.table(
    comment="BLS PR series metadata - Time series definitions"
)
@dp.expect_or_fail("valid_series_id", "series_id IS NOT NULL")
@dp.expect("valid_begin_year", "begin_year IS NOT NULL")
def brz_pr_series():
    # Get catalog and schema from pipeline configuration
    catalog = spark.conf.get("source_catalog")
    schema_name = spark.conf.get("source_schema")
    
    # Build parameterized volume path
    file_path = f"/Volumes/{catalog}/{schema_name}/quest_data/bls/pr/pr.series"
    
    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "series_id STRING, sector_code STRING, class_code STRING, measure_code STRING, duration_code STRING, seasonal STRING, base_year STRING, footnote_codes STRING, begin_year STRING, begin_period STRING, end_year STRING, end_period STRING"
    df = spark.read.options(**bls_options).schema(schema).csv(file_path)
    
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())

    # Clean column names
    df = clean_column_names(df)
    return df
