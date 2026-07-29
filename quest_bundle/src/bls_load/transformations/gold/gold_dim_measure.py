from pyspark import pipelines as dp

@dp.table(
    name="gold_dim_measure",
    comment="Gold table - Measure dimension",
    schema="""
        measure_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
        measure_code STRING COMMENT 'Identifies specific factor measured.',
        measure_text STRING COMMENT 'Names specific factor measured.',
        display_level INT COMMENT 'Identifies hierarchical structure of data when combined with sort_sequence information.',
        selectable STRING COMMENT 'Identifies items that may be selected in query tools (T) or items that are titles only and not selectable (F).',
        sort_sequence INT COMMENT 'Identifies order that items will appear in single-screen query tool and informs hierarchical structure of data when combined with display_level information.'
    """,
    table_properties={
        "pipelines.primaryKey": "measure_sk"
    }
)
@dp.expect("measure_code is not null", "measure_code IS NOT NULL")
def gold_dim_measure():
    df = spark.read.table("slv_pr_measure")
    return df