from pyspark import pipelines as dp

@dp.table(
    name="gold_dim_class",
    comment="Gold table - Class dimension",
    schema="""
        class_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
        class_code STRING COMMENT 'Identifies employee group to which data pertain.',
        class_text STRING COMMENT 'Names employee group to which data pertain.',
        display_level INT COMMENT 'Identifies hierarchical structure of data when combined with sort_sequence information.',
        selectable STRING COMMENT 'Identifies items that may be selected in query tools (T) or items that are titles only and not selectable (F).',
        sort_sequence INT COMMENT 'Identifies order that items will appear in single-screen query tool and informs hierarchical structure of data when combined with display_level information.'
    """,
    table_properties={
        "pipelines.primaryKey": "class_sk"
    }    
)
@dp.expect("class_code is not null", "class_code IS NOT NULL")
def gold_dim_class():
    df = spark.read.table("slv_pr_class")
    return df

### Example to turn gold into streaming table for scd type tables
# @dp.temporary_view()
# def gold_dim_class_transformed():
#     df = spark.readStream.table("slv_pr_class")
#     return df

# dp.create_streaming_table(
#     name="gold_dim_class",
#     comment="Gold table - Class dimension",
#     schema="""
#         class_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
#         class_code STRING COMMENT 'Identifies employee group to which data pertain.',
#         class_text STRING COMMENT 'Names employee group to which data pertain.',
#         display_level INT COMMENT 'Identifies hierarchical structure of data when combined with sort_sequence information.',
#         selectable STRING COMMENT 'Identifies items that may be selected in query tools (T) or items that are titles only and not selectable (F).',
#         sort_sequence INT COMMENT 'Identifies order that items will appear in single-screen query tool and informs hierarchical structure of data when combined with display_level information.'
#     """,
#     table_properties={
#         "pipelines.primaryKey": "class_sk"
#     }
# )

# dp.create_auto_cdc_flow(
#     target="gold_dim_class",
#     source="gold_dim_class_transformed",
#     keys=["class_code"],
#     sequence_by="class_code",
#     stored_as_scd_type=1,
#     ignore_null_updates=False
# )


