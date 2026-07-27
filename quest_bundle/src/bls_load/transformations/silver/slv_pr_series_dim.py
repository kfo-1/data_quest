from pyspark import pipelines as dp
from pyspark.sql.functions import col, substring

@dp.materialized_view(
    comment="Silver dimension - Series metadata with parsed components and enriched descriptions"
)
@dp.expect_or_fail("valid_series_id", "series_id IS NOT NULL AND LENGTH(series_id) = 17")
def slv_pr_series_dim():
    # Read bronze series
    df = spark.read.table("brz_pr_series")
    
    # Parse series_id components (PR + seasonal(1) + sector(4) + class(1) + measure(2) + duration(1))
    # Example: PRS30006011 = PR + S + 3000 + 6 + 01 + 1
    df = df.withColumn("survey_code", substring(col("series_id"), 1, 2))  # PR
    df = df.withColumn("seasonal_code_parsed", substring(col("series_id"), 3, 1))  # S or U
    df = df.withColumn("sector_code_parsed", substring(col("series_id"), 4, 4))  # 3000
    df = df.withColumn("class_code_parsed", substring(col("series_id"), 8, 1))  # 6
    df = df.withColumn("measure_code_parsed", substring(col("series_id"), 9, 2))  # 01
    df = df.withColumn("duration_code_parsed", substring(col("series_id"), 11, 1))  # 1
    
    # Convert year columns to integers for easier filtering
    df = df.withColumn("begin_year_int", col("begin_year").cast("int"))
    df = df.withColumn("end_year_int", col("end_year").cast("int"))
    
    # Enrich with dimension lookups
    sector_dim = spark.read.table("slv_pr_sector_dim")
    df = df.join(
        sector_dim.select("sector_code", col("sector_name").alias("sector_name_desc")), 
        df["sector_code_parsed"] == sector_dim["sector_code"], 
        "left"
    ).drop(sector_dim["sector_code"])
    
    class_dim = spark.read.table("slv_pr_class_dim")
    df = df.join(
        class_dim.select("class_code", col("class_text").alias("class_desc")), 
        df["class_code_parsed"] == class_dim["class_code"], 
        "left"
    ).drop(class_dim["class_code"])
    
    measure_dim = spark.read.table("slv_pr_measure_dim")
    df = df.join(
        measure_dim.select("measure_code", col("measure_text").alias("measure_desc")), 
        df["measure_code_parsed"] == measure_dim["measure_code"], 
        "left"
    ).drop(measure_dim["measure_code"])
    
    duration_dim = spark.read.table("slv_pr_duration_dim")
    df = df.join(
        duration_dim.select("duration_code", col("duration_text").alias("duration_desc")), 
        df["duration_code_parsed"] == duration_dim["duration_code"], 
        "left"
    ).drop(duration_dim["duration_code"])
    
    seasonal_dim = spark.read.table("slv_pr_seasonal_dim")
    df = df.join(
        seasonal_dim.select("seasonal_code", col("seasonal_text").alias("seasonal_desc")), 
        df["seasonal_code_parsed"] == seasonal_dim["seasonal_code"], 
        "left"
    ).drop(seasonal_dim["seasonal_code"])
    
    # Clean up - remove bronze columns we don't need
    df = df.drop("load_timestamp", "sector_code", "class_code", "measure_code", "duration_code", "seasonal")
    
    return df
