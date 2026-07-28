from pyspark import pipelines as dp
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

@dp.temporary_view()
def gold_dim_sector_transformed():
    df = spark.readStream.table("slv_pr_sector")
    return df

dp.create_streaming_table(
    name="gold_dim_sector",
    comment="Gold table - Sector dimension",
    schema="""
        sector_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
        sector_code STRING COMMENT 'Identifies the sector of the economy to which the data observation refers.',
        sector_name STRING COMMENT 'Names economic sector to which the data observation refers.',
        display_level INT COMMENT 'Identifies hierarchical structure of data when combined with sort_sequence information.',
        selectable STRING COMMENT 'Identifies items that may be selected in query tools (T) or items that are titles only and not selectable (F).',
        sort_sequence INT COMMENT 'Identifies order that items will appear in single-screen query tool and informs hierarchical structure of data when combined with display_level information.'
    """,
    table_properties={
        "pipelines.primaryKey": "sector_sk"
    }
)

dp.create_auto_cdc_flow(
    target="gold_dim_sector",
    source="gold_dim_sector_transformed",
    keys=["sector_code"],
    sequence_by="sector_code",
    stored_as_scd_type=1,
    ignore_null_updates=False
)


