from pyspark import pipelines as dp
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

@dp.temporary_view()
def gold_dim_footnote_transformed():
    df = spark.readStream.table("slv_pr_footnote")
    return df

dp.create_streaming_table(
    name="gold_dim_footnote",
    comment="Gold table - Footnote dimension",
    schema="""
        footnote_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
        footnote_code STRING COMMENT 'Identifies an individual footnote.',
        footnote_text STRING COMMENT 'Identifies full set of footnotes that apply to a data point or series, in a comma-separated list.'
    """,
    table_properties={
        "pipelines.primaryKey": "footnote_sk"
    }
)

dp.create_auto_cdc_flow(
    target="gold_dim_footnote",
    source="gold_dim_footnote_transformed",
    keys=["footnote_code"],
    sequence_by="footnote_code",
    stored_as_scd_type=1,
    ignore_null_updates=False
)


