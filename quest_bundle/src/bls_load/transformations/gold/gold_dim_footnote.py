from pyspark import pipelines as dp

@dp.table(
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
@dp.expect("footnote_code is not null", "footnote_code IS NOT NULL")
def gold_dim_footnote():
    df = spark.read.table("slv_pr_footnote")
    return df