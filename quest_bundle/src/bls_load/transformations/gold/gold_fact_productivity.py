from pyspark import pipelines as dp
from pyspark.sql.functions import trim

@dp.table(
    name="gold_fact_productivity",
    comment="Gold table - Productivity",
    schema="""
        seasonal_code STRING COMMENT 'Indicator of the type of record.',
        series_id STRING COMMENT 'Identifies the specific series.',
        class_sk BIGINT COMMENT 'Class surrogate key (FK)',
        duration_sk BIGINT COMMENT 'Duration surrogate key (FK)',
        footnote_sk BIGINT COMMENT 'Footnote surrogate key (FK)',
        measure_sk BIGINT COMMENT 'Measure surrogate key (FK)',
        period_sk BIGINT COMMENT 'Period surrogate key (FK)',
        sector_sk BIGINT COMMENT 'Sector surrogate key (FK)',
        year INT COMMENT 'Year',
        value DOUBLE COMMENT 'Value',
        observation_date DATE COMMENT 'Date of observation',
        observation_year INT COMMENT 'Year of observation',
        observation_quarter INT COMMENT 'Quarter of observation',
        observation_month INT COMMENT 'Month of observation',
        is_annual_average BOOLEAN COMMENT 'Is annual average'
    """
)
@dp.expect(
    "all_dimension_keys_resolved",
    "series_id IS NOT NULL AND class_sk IS NOT NULL AND duration_sk IS NOT NULL AND footnote_sk IS NOT NULL AND measure_sk IS NOT NULL AND period_sk IS NOT NULL AND sector_sk IS NOT NULL"
)
def gold_fact_productivity():
    """
    Transform silver population facts to gold:
    - Join with dimensions to get surrogate keys
    """
    df = spark.read.table("slv_pr_productivity")
    
    # Stream-static join with dimensions to get surrogate key
    class_dim = spark.read.table("gold_dim_class").select("class_sk", "class_code")
    df = df.join(class_dim, "class_code", "left")
    duration_dim = spark.read.table("gold_dim_duration").select("duration_sk", "duration_code")
    df = df.join(duration_dim, "duration_code", "left")
    footnote_dim = spark.read.table("gold_dim_footnote").select("footnote_sk", "footnote_code")
    df = df.join(footnote_dim, [df.footnote_codes == footnote_dim.footnote_code], "left")
    measure_dim = spark.read.table("gold_dim_measure").select("measure_sk", "measure_code")
    df = df.join(measure_dim, "measure_code", "left")
    period_dim = spark.read.table("gold_dim_period").select("period_sk", "period")
    df = df.join(period_dim, "period", "left")
    seasonal_dim = spark.read.table("gold_dim_seasonal").select("seasonal_sk", "seasonal_code")
    df = df.join(seasonal_dim, "seasonal_code", "left")
    sector_dim = spark.read.table("gold_dim_sector").select("sector_sk", "sector_code")
    df = df.join(sector_dim, "sector_code", "left")

    # Select columns
    df = df.select(df.seasonal_code, trim(df.series_id).alias("series_id"), class_dim.class_sk, duration_dim.duration_sk, footnote_dim.footnote_sk, measure_dim.measure_sk, period_dim.period_sk, sector_dim.sector_sk, df.year, df.value, df.observation_date, df.observation_year, df.observation_quarter, df.observation_month, df.is_annual_average)

    return df

