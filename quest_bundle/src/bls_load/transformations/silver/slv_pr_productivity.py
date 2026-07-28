from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, substring, when, concat, lit, to_date, 
    year as spark_year, quarter, month, last_day
)

# Step 1: Temporary view that unions both Current and AllData with transformations
@dp.temporary_view()
def pr_data_combined():
    """Combine Current and AllData bronze streams with all transformations"""
    # Read both bronze sources
    current_df = spark.readStream.table("brz_pr_data_0_current")
    alldata_df = spark.readStream.table("brz_pr_data_1_alldata")
    
    # Union both streams
    df = current_df.unionByName(alldata_df, allowMissingColumns=True)
    
    # Parse series_id components
    df = df.withColumn("survey_code", substring(col("series_id"), 1, 2))
    df = df.withColumn("seasonal_code", substring(col("series_id"), 3, 1))
    df = df.withColumn("sector_code", substring(col("series_id"), 4, 4))
    df = df.withColumn("class_code", substring(col("series_id"), 8, 1))
    df = df.withColumn("measure_code", substring(col("series_id"), 9, 2))
    df = df.withColumn("duration_code", substring(col("series_id"), 11, 1))
    
    # Extract period number
    df = df.withColumn("period_num", substring(col("period"), 2, 2).cast("int"))
    
    # Convert period codes to standard dates
    df = df.withColumn(
        "observation_date",
        when(col("period").startswith("Q"),
            when(col("period_num") == 1, to_date(concat(col("year"), lit("-03-31"))))
            .when(col("period_num") == 2, to_date(concat(col("year"), lit("-06-30"))))
            .when(col("period_num") == 3, to_date(concat(col("year"), lit("-09-30"))))
            .otherwise(to_date(concat(col("year"), lit("-12-31"))))
        )
        .when(col("period").startswith("M"),
            when(col("period_num") == 13, to_date(concat(col("year"), lit("-12-31"))))
            .otherwise(last_day(to_date(concat(col("year"), lit("-"), col("period_num"), lit("-01")))))
        )
        .when(col("period").startswith("S"),
            when(col("period_num") == 1, to_date(concat(col("year"), lit("-06-30"))))
            .otherwise(to_date(concat(col("year"), lit("-12-31"))))
        )
    )
    
    # Add derived date columns
    df = df.withColumn("observation_year", spark_year(col("observation_date")))
    df = df.withColumn("observation_quarter", quarter(col("observation_date")))
    df = df.withColumn("observation_month", month(col("observation_date")))
    
    # Add period type
    df = df.withColumn(
        "period_type",
        when(col("period").startswith("Q"), "Quarterly")
        .when(col("period").startswith("M"), "Monthly")
        .when(col("period").startswith("S"), "Semi-Annual")
        .otherwise("Unknown")
    )
    
    # Flag annual averages
    df = df.withColumn("is_annual_average", col("period").isin("Q05", "M13", "S03"))
    
    # Drop temporary columns
    df = df.drop("period_num")
    
    return df

# Step 2: Create target streaming table with explicit schema (enforced at silver)
dp.create_streaming_table(
    name="slv_pr_productivity",
    comment="Silver fact table - Deduplicated productivity observations with standard dates",
    schema="""
        series_id STRING,
        year INT,
        period STRING,
        value DOUBLE,
        footnote_codes STRING,
        survey_code STRING,
        seasonal_code STRING,
        sector_code STRING,
        class_code STRING,
        measure_code STRING,
        duration_code STRING,
        observation_date DATE,
        observation_year INT,
        observation_quarter INT,
        observation_month INT,
        period_type STRING,
        is_annual_average BOOLEAN,
        load_timestamp TIMESTAMP
    """
)

# Step 3: Auto CDC flow with deduplication on (series_id, year, period)
dp.create_auto_cdc_flow(
    target="slv_pr_productivity",
    source="pr_data_combined",
    keys=["series_id", "year", "period"],  # Composite key for deduplication
    sequence_by="load_timestamp",  # Latest load wins
    stored_as_scd_type=1,  # Keep only latest value (no history)
    ignore_null_updates=True  # Don't overwrite with nulls
)
