from pyspark import pipelines as dp

@dp.table(
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
@dp.expect("sector_code is not null", "sector_code IS NOT NULL")
def gold_dim_sector():
    df = spark.read.table("slv_pr_sector")
    return df



