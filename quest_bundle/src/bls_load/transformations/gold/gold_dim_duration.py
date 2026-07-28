from pyspark import pipelines as dp
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

@dp.temporary_view()
def gold_dim_duration_transformed():
    df = spark.readStream.table("slv_pr_duration")
    return df

dp.create_streaming_table(
    name="gold_dim_duration",
    comment="Gold table - Duration dimension",
    schema="""
        duration_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
        duration_code STRING COMMENT 'Identifies whether data are percent changes or indexes.',
        duration_text STRING COMMENT 'Names percent changes or indexes.',
        display_level INT COMMENT 'Identifies hierarchical structure of data when combined with sort_sequence information.',
        selectable STRING COMMENT 'Identifies items that may be selected in query tools (T) or items that are titles only and not selectable (F).',
        sort_sequence INT COMMENT 'Identifies order that items will appear in single-screen query tool and informs hierarchical structure of data when combined with display_level information.'
    """,
    table_properties={
        "pipelines.primaryKey": "duration_sk"
    }
)

dp.create_auto_cdc_flow(
    target="gold_dim_duration",
    source="gold_dim_duration_transformed",
    keys=["duration_code"],
    sequence_by="duration_code",
    stored_as_scd_type=1,
    ignore_null_updates=False
)


