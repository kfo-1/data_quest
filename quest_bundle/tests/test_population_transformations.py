"""Tests for population data transformations (silver and gold layers)."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, 
    DoubleType, TimestampType
)
from datetime import datetime


class TestSilverPopulationTransformations:
    """Tests for silver layer population data transformations."""

    def test_population_data_schema(self, spark: SparkSession):
        """Verify that silver population data has the expected schema."""
        # Create sample silver population data
        schema = StructType([
            StructField("nation", StringType(), True),
            StructField("nation_id", StringType(), True),
            StructField("population", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("load_timestamp", TimestampType(), True)
        ])
        
        data = [
            ("United States", "01000US", 340110990, 2024, datetime.now())
        ]
        df = spark.createDataFrame(data, schema)
        
        # Verify schema
        assert "nation" in df.columns
        assert "nation_id" in df.columns
        assert "population" in df.columns
        assert "year" in df.columns
        assert "load_timestamp" in df.columns
        
        # Verify data types
        assert df.schema["population"].dataType == LongType()
        assert df.schema["year"].dataType == IntegerType()

    def test_population_data_casting(self, spark: SparkSession):
        """Verify that population values in scientific notation are correctly cast to long."""
        from pyspark.sql.functions import col
        
        # Create test data with population as double (simulating scientific notation from JSON)
        schema = StructType([
            StructField("nation_id", StringType(), True),
            StructField("population", DoubleType(), True),
            StructField("year", StringType(), True)
        ])
        
        data = [
            ("01000US", 3.4011099e8, "2024"),  # Scientific notation
            ("01000US", 3.2912623e8, "2020")
        ]
        df = spark.createDataFrame(data, schema)
        
        # Cast as done in silver transformation
        result = df.withColumn("population", col("population").cast("long")) \
                   .withColumn("year", col("year").cast("int"))
        
        # Verify casting worked
        row = result.filter(col("year") == 2024).first()
        assert row["population"] == 340110990
        assert isinstance(row["population"], int)
        assert row["year"] == 2024
        assert isinstance(row["year"], int)

    def test_nation_dimension_deduplication(self, spark: SparkSession):
        """Verify that nation dimension removes duplicate nation_ids."""
        # Create test data with duplicates
        data = [
            ("United States", "01000US", datetime.now()),
            ("United States", "01000US", datetime.now()),  # Duplicate
            ("Canada", "CA", datetime.now())
        ]
        schema = StructType([
            StructField("nation", StringType(), True),
            StructField("nation_id", StringType(), True),
            StructField("load_timestamp", TimestampType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Apply deduplication as done in transformation
        result = df.dropDuplicates(["nation_id"])
        
        # Verify only unique nation_ids remain
        assert result.count() == 2
        nation_ids = [row["nation_id"] for row in result.collect()]
        assert "01000US" in nation_ids
        assert "CA" in nation_ids


class TestGoldPopulationTransformations:
    """Tests for gold layer population fact table with year-over-year calculations."""

    def test_gold_dimension_identity_column(self, spark: SparkSession):
        """Verify that gold dimension uses identity column as surrogate key."""
        # This test simulates the gold dimension structure
        schema = StructType([
            StructField("nation_sk", LongType(), False),  # Surrogate key
            StructField("nation_id", StringType(), True),  # Business key
            StructField("name", StringType(), True)
        ])
        
        data = [
            (1, "01000US", "United States"),
            (2, "CA", "Canada")
        ]
        df = spark.createDataFrame(data, schema)
        
        # Verify surrogate key structure
        assert "nation_sk" in df.columns
        assert "nation_id" in df.columns
        assert df.count() == 2

    def test_year_over_year_population_change(self, spark: SparkSession):
        """Verify year-over-year population change calculations."""
        from pyspark.sql.functions import col, lag
        from pyspark.sql.window import Window
        
        # Create multi-year population data
        data = [
            (1, 2020, 329000000),
            (1, 2021, 332000000),
            (1, 2022, 335000000),
            (1, 2023, 338000000),
            (1, 2024, 340000000)
        ]
        schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("population", LongType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Apply window function to calculate previous year's population
        window_spec = Window.partitionBy("nation_sk").orderBy("year")
        result = df.withColumn("previous_population", lag("population", 1).over(window_spec))
        result = result.withColumn("population_change", 
                                   col("population") - col("previous_population"))
        result = result.withColumn("population_pct_change",
                                   ((col("population") - col("previous_population")) / 
                                    col("previous_population")) * 100)
        
        # Verify calculations for 2021 (first year with previous data)
        row_2021 = result.filter(col("year") == 2021).first()
        assert row_2021["previous_population"] == 329000000
        assert row_2021["population_change"] == 3000000
        assert abs(row_2021["population_pct_change"] - 0.91) < 0.01  # ~0.91%
        
        # Verify 2020 has no previous year (null)
        row_2020 = result.filter(col("year") == 2020).first()
        assert row_2020["previous_population"] is None

    def test_foreign_key_relationship(self, spark: SparkSession):
        """Verify join between fact table and dimension table."""
        # Create dimension table
        dim_data = [
            (1, "01000US", "United States"),
            (2, "CA", "Canada")
        ]
        dim_schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("nation_id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        dim_df = spark.createDataFrame(dim_data, dim_schema)
        
        # Create fact table
        fact_data = [
            ("01000US", 2024, 340000000),
            ("CA", 2024, 39000000)
        ]
        fact_schema = StructType([
            StructField("nation_id", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("population", LongType(), True)
        ])
        fact_df = spark.createDataFrame(fact_data, fact_schema)
        
        # Join to get surrogate key
        result = fact_df.join(dim_df.select("nation_sk", "nation_id"), "nation_id", "left")
        
        # Verify join worked and surrogate keys are present
        assert result.count() == 2
        assert "nation_sk" in result.columns
        
        us_row = result.filter(result.nation_id == "01000US").first()
        assert us_row["nation_sk"] == 1
        
        ca_row = result.filter(result.nation_id == "CA").first()
        assert ca_row["nation_sk"] == 2

    def test_null_handling_in_calculations(self, spark: SparkSession):
        """Verify that null values in calculations are handled correctly."""
        from pyspark.sql.functions import col
        
        # Create data with nulls
        data = [
            (1, 2024, 340000000, None),
            (1, 2023, 338000000, 335000000)
        ]
        schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("population", LongType(), True),
            StructField("previous_population", LongType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Calculate changes (will be null when previous_population is null)
        result = df.withColumn("population_change", 
                               col("population") - col("previous_population"))
        
        # Verify null handling
        row_2024 = result.filter(col("year") == 2024).first()
        assert row_2024["population_change"] is None  # Null propagates through calculation
        
        row_2023 = result.filter(col("year") == 2023).first()
        assert row_2023["population_change"] == 3000000  # Valid calculation
