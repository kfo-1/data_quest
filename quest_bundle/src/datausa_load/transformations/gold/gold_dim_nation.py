from pyspark import pipelines as dp
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

@dp.temporary_view()
def gold_dim_nation_transformed():
    df = spark.readStream.table("slv_population_nation")
    # Rename nation to name for consistency
    df = df.withColumnRenamed("nation", "name")
    return df.select("nation_id", "name")

dp.create_streaming_table(
    name="gold_dim_nation",
    comment="Gold table - Nation dimension",
    schema="""
        nation_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
        nation_id STRING COMMENT 'Business key',
        name STRING COMMENT 'Nation name'
    """,
    table_properties={
        "pipelines.primaryKey": "nation_sk"
    }
)

dp.create_auto_cdc_flow(
    target="gold_dim_nation",
    source="gold_dim_nation_transformed",
    keys=["nation_id"],
    sequence_by="nation_id",
    stored_as_scd_type=1,
    ignore_null_updates=False
)

