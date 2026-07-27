from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, when

@dp.materialized_view(
    comment="Silver dimension - Period definitions (cleaned and standardized for gold layer)"
)
@dp.expect_or_fail("no_nulls_in_key", "period IS NOT NULL")
def slv_pr_period_dim():
    df = spark.read.table("brz_pr_period")
    
    # Standardize text fields - trim whitespace
    for col_name in df.columns:
        if col_name.endswith('_text') or col_name.endswith('_name'):
            df = df.withColumn(col_name, trim(col(col_name)))
    
    # Remove load_timestamp (not needed in silver dimensions)
    df = df.drop("load_timestamp")

    # Add period type classification for easier filtering
    df = df.withColumn(
        "period_type",
        when(col("period").startswith("Q"), "Quarterly")
        .when(col("period").startswith("M"), "Monthly")
        .when(col("period").startswith("S"), "Semi-Annual")
        .otherwise("Unknown")
    )
    
    # Add is_annual flag (Q05, M13, S03 indicate annual averages)
    df = df.withColumn(
        "is_annual_average",
        col("period").isin("Q05", "M13", "S03")
    )
    
    return df
