from pyspark import pipelines as dp

@dp.table(
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
def gold_dim_duration():
    df = spark.read.table("slv_pr_duration")
    return df


