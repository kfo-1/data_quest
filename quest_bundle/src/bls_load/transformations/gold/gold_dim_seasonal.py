from pyspark import pipelines as dp

@dp.table(
    name="gold_dim_seasonal",
    comment="Gold table - Seasonal dimension",
    schema="""
        seasonal_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
        seasonal_code STRING COMMENT 'Identifies whether the data are seasonally adjusted.',
        seasonal_text STRING COMMENT 'Names seasonal adjustment status.'
    """,
    table_properties={
        "pipelines.primaryKey": "seasonal_sk"
    }
)
def gold_dim_seasonal():
    df = spark.read.table("slv_pr_seasonal")
    return df
