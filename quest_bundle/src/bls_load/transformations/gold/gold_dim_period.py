from pyspark import pipelines as dp
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

@dp.temporary_view()
def gold_dim_period_transformed():
    df = spark.readStream.table("slv_pr_period")
    return df

dp.create_streaming_table(
    name="gold_dim_period",
    comment="Gold table - Period dimension",
    schema="""
        period_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
        period STRING COMMENT 'Identifies period to which the data observation refers.',
        period_abbr STRING COMMENT 'Abbreviation of period name to which the data observation refers.',
        period_name STRING COMMENT 'Names period to which the data observation refers.',
        period_type STRING COMMENT 'Names the type of period to which the data observation referes.',
        is_annual_average BOOLEAN COMMENT 'True/False indicator if the observation is an annual average.'
    """,
    table_properties={
        "pipelines.primaryKey": "period_sk"
    }
)

dp.create_auto_cdc_flow(
    target="gold_dim_period",
    source="gold_dim_period_transformed",
    keys=["period"],
    sequence_by="period",
    stored_as_scd_type=1,
    ignore_null_updates=False
)


