from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim

@dp.table(
    name="gold_dim_series",
    comment="Gold table - Series dimension",
    schema="""
        series_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key',
        series_id STRING COMMENT 'Identifies the series of the economy to which the data observation refers.',
        base_year STRING COMMENT 'Names economic series to which the data observation refers.',
        footnote_codes STRING COMMENT 'Identifies hierarchical structure of data when combined with sort_sequence information.',
        begin_year string COMMENT 'Identifies items that may be selected in query tools (T) or items that are titles only and not selectable (F).',
        begin_period STRING COMMENT 'Identifies order that items will appear in single-screen query tool and informs hierarchical structure of data when combined with display_level information.',
        end_year STRING COMMENT 'Identifies last year for which data is available for a given time series.',
        end_period STRING COMMENT 'Identifies last data observation within the last year for which data is available for a given time series.',
        survey_code STRING COMMENT 'Identifies the type of survey.',
        seasonal_code STRING COMMENT 'Identifies whether the data are seasonally adjusted.',
        class_code STRING COMMENT 'Identifies employee group to which data pertain.',
        sector_code STRING COMMENT 'Identifies the sector of the economy to which the data observation refers.',
        measure_code STRING COMMENT 'Identifies specific factor measured.',
        duration_code STRING COMMENT 'Identifies whether data are percent changes or indexes.'
    """,
    table_properties={
        "pipelines.primaryKey": "series_sk"
    }
)
def gold_dim_series():
    df = spark.read.table("slv_pr_series")
    return df.select(
        trim("series_id").alias("series_id"),
        "base_year",
        "footnote_codes",
        "begin_year",
        "begin_period",
        "end_year",
        "end_period",
        "survey_code",
        col("seasonal_code_parsed").alias("seasonal_code"),
        col("class_code_parsed").alias("class_code"),
        col("sector_code_parsed").alias("sector_code"),
        col("measure_code_parsed").alias("measure_code"),
        col("duration_code_parsed").alias("duration_code")
    )




