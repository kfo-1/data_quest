from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
from utilities.utils import bls_options, clean_column_names

@dp.table(
    comment="BLS PR sector codes - Economic sectors"
)
@dp.expect_or_fail("valid_sector_code", "sector_code IS NOT NULL AND LENGTH(sector_code) = 4")
@dp.expect("valid_selectable", "selectable IN ('T', 'F')")
def brz_pr_sector():
    # Get catalog and schema from pipeline configuration
    catalog = spark.conf.get("source_catalog")
    schema_name = spark.conf.get("source_schema")
    
    # Build parameterized volume path
    file_path = f"/Volumes/{catalog}/{schema_name}/quest_data/bls/pr/pr.sector"
    
    # Explicit schema from pr.txt - preserves leading zeros in code columns
    schema = "sector_code STRING, sector_name STRING, display_level INT, selectable STRING, sort_sequence INT"
    df = spark.read.options(**bls_options).schema(schema).csv(file_path)
    
    # Add load_timestamp after reading
    df = df.withColumn("load_timestamp", current_timestamp())

    # Clean column names
    df = clean_column_names(df)
    return df
