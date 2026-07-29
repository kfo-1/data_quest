from pyspark import pipelines as dp

@dp.table(
    name="gold_dim_nation",
    comment="Gold table - Nation dimension",
    schema="""
        nation_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
        nation_id STRING COMMENT 'Business key',
        name STRING COMMENT 'Nation name'
    """,
    table_properties={
        "pipelines.primaryKey": "nation_sk"
    }
)
@dp.expect(
    "nation_sk_populated",
    "nation_sk IS NOT NULL"
)
def gold_dim_class():
    df = spark.read.table("slv_population_nation")
    # Rename nation to name for consistency
    df = df.withColumnRenamed("nation", "name")
    return df.select("nation_id", "name")

