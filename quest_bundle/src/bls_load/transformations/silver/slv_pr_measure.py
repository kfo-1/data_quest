from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim

@dp.materialized_view(
    comment="Silver dimension - Productivity measures (cleaned and standardized for gold layer)"
)
@dp.expect_or_fail("no_nulls_in_key", "measure_code IS NOT NULL")
def slv_pr_measure():
    df = spark.read.table("brz_pr_measure")
    
    # Standardize text fields - trim whitespace
    for col_name in df.columns:
        if col_name.endswith('_text') or col_name.endswith('_name'):
            df = df.withColumn(col_name, trim(col(col_name)))
    
    # Remove load_timestamp (not needed in silver dimensions)
    df = df.drop("load_timestamp")

    
    return df
