from pyspark import pipelines as dp
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

@dp.temporary_view()
def gold_dim_seasonal_transformed():
    df = spark.readStream.table("slv_pr_seasonal")
    return df

dp.create_streaming_table(
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

dp.create_auto_cdc_flow(
    target="gold_dim_seasonal",
    source="gold_dim_seasonal_transformed",
    keys=["seasonal_code"],
    sequence_by="seasonal_code",
    stored_as_scd_type=1,
    ignore_null_updates=False
)


