from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, substring, when, concat, lit, to_date, 
    year as spark_year, quarter, month, last_day,
    trim
)

# Step 1: Materialized view that unions both Current and AllData with transformations
@dp.table(
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
@dp.expect_or_fail(
    "unique_business_key",
    "COUNT(*) = COUNT(DISTINCT (series_id, year, period))"
)
@dp.expect_or_fail(
    "non_null_critical_fields",
    "series_id IS NOT NULL AND year IS NOT NULL AND period IS NOT NULL"
)
@dp.expect(
    "valid_value",
    "value IS NOT NULL"
)
def pr_data_combined():
    """Combine Current and AllData bronze streams with all transformations"""
    # Read both bronze sources
    current_df = spark.read.table("brz_pr_data_0_current")
    alldata_df = spark.read.table("brz_pr_data_1_alldata")
    
    # Union both streams
    df = current_df.unionByName(alldata_df, allowMissingColumns=True).dropDuplicates(["series_id", "year", "period"])
    
    # Parse series_id components
    df = df.withColumns({
        "series_id": trim(col("series_id")),
        "survey_code": substring(col("series_id"), 1, 2),
        "seasonal_code": substring(col("series_id"), 3, 1),
        "sector_code": substring(col("series_id"), 4, 4),
        "class_code": substring(col("series_id"), 8, 1),
        "measure_code": substring(col("series_id"), 9, 2),
        "duration_code": substring(col("series_id"), 11, 1),
        # Extract period number
        "period_num": substring(col("period"), 2, 2).cast("int"),
        # Convert period codes to standard dates
        "observation_date": when(col("period").startswith("Q"),
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
            ),
        # Add derived date columns
        "observation_year": spark_year(col("observation_date")),
        "observation_quarter": quarter(col("observation_date")),
        "observation_month": month(col("observation_date")),
        # Add period type
        "period_type": when(col("period").startswith("Q"), "Quarterly")
            .when(col("period").startswith("M"), "Monthly")
            .when(col("period").startswith("S"), "Semi-Annual")
            .otherwise("Unknown"),
        # Flag annual averages
        "is_annual_average": col("period").isin("Q05", "M13", "S03")
    })

    # Drop temporary columns
    df = df.drop("period_num")
    
    return df

