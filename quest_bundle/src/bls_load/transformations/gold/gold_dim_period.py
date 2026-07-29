from pyspark import pipelines as dp

@dp.table(
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
@dp.expect("period is not null", "period IS NOT NULL")
def gold_dim_period():
    df = spark.read.table("slv_pr_period")
    return df

