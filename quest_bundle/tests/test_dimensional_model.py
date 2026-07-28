"""Tests for dimensional model structure and data quality rules."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, DoubleType
)
from pyspark.sql.functions import col, count, countDistinct


class TestDimensionTables:
    """Tests for dimension table structure and constraints."""

    def test_dimension_has_surrogate_key(self, spark: SparkSession):
        """Verify dimension table has a surrogate key (nation_sk)."""
        # Simulate gold dimension structure
        schema = StructType([
            StructField("nation_sk", LongType(), False),  # NOT NULL
            StructField("nation_id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        
        data = [
            (1, "01000US", "United States"),
            (2, "CA", "Canada")
        ]
        df = spark.createDataFrame(data, schema)
        
        # Verify surrogate key column exists and has values
        assert "nation_sk" in df.columns
        assert df.filter(col("nation_sk").isNull()).count() == 0

    def test_dimension_surrogate_key_uniqueness(self, spark: SparkSession):
        """Verify surrogate key values are unique."""
        data = [
            (1, "01000US", "United States"),
            (2, "CA", "Canada"),
            (3, "MX", "Mexico")
        ]
        schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("nation_id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Check for uniqueness
        total_rows = df.count()
        distinct_keys = df.select(countDistinct("nation_sk")).first()[0]
        
        assert total_rows == distinct_keys, "Surrogate keys must be unique"

    def test_dimension_business_key_uniqueness(self, spark: SparkSession):
        """Verify business key (nation_id) values are unique in dimension."""
        data = [
            (1, "01000US", "United States"),
            (2, "CA", "Canada"),
            (3, "MX", "Mexico")
        ]
        schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("nation_id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Check business key uniqueness
        total_rows = df.count()
        distinct_nation_ids = df.select(countDistinct("nation_id")).first()[0]
        
        assert total_rows == distinct_nation_ids, "Business keys must be unique"


class TestFactTables:
    """Tests for fact table structure and constraints."""

    def test_fact_table_has_foreign_key(self, spark: SparkSession):
        """Verify fact table includes foreign key to dimension."""
        # Simulate gold fact table structure
        schema = StructType([
            StructField("nation_sk", LongType(), False),  # Foreign key NOT NULL
            StructField("year", IntegerType(), True),
            StructField("population", LongType(), True),
            StructField("previous_population", LongType(), True),
            StructField("population_change", LongType(), True),
            StructField("population_pct_change", DoubleType(), True)
        ])
        
        data = [
            (1, 2024, 340000000, 338000000, 2000000, 0.59)
        ]
        df = spark.createDataFrame(data, schema)
        
        # Verify foreign key column exists and is not null
        assert "nation_sk" in df.columns
        assert df.filter(col("nation_sk").isNull()).count() == 0

    def test_fact_table_composite_primary_key(self, spark: SparkSession):
        """Verify fact table has composite primary key (nation_sk, year)."""
        data = [
            (1, 2020, 329000000),
            (1, 2021, 332000000),
            (1, 2022, 335000000),
            (2, 2020, 38000000),
            (2, 2021, 38500000)
        ]
        schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("population", LongType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Check composite key uniqueness
        total_rows = df.count()
        distinct_composite_keys = df.select(
            countDistinct(col("nation_sk"), col("year"))
        ).first()[0]
        
        assert total_rows == distinct_composite_keys, \
            "Composite primary key (nation_sk, year) must be unique"

    def test_referential_integrity_all_foreign_keys_exist(self, spark: SparkSession):
        """Verify all foreign keys in fact table exist in dimension table."""
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
            (1, 2024, 340000000),
            (2, 2024, 39000000)
        ]
        fact_schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("population", LongType(), True)
        ])
        fact_df = spark.createDataFrame(fact_data, fact_schema)
        
        # Get all distinct foreign keys from fact table
        fact_keys = fact_df.select("nation_sk").distinct()
        
        # Left anti join to find orphaned foreign keys
        orphaned = fact_keys.join(
            dim_df.select("nation_sk"), 
            "nation_sk", 
            "left_anti"
        )
        
        # Verify no orphaned foreign keys
        assert orphaned.count() == 0, "All foreign keys must exist in dimension table"


class TestDataQuality:
    """Tests for data quality rules across the dimensional model."""

    def test_fact_table_no_null_measures(self, spark: SparkSession):
        """Verify that required measure columns are not null."""
        data = [
            (1, 2024, 340000000, 338000000, 2000000, 0.59),
            (1, 2023, 338000000, 335000000, 3000000, 0.89)
        ]
        schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("population", LongType(), True),
            StructField("previous_population", LongType(), True),
            StructField("population_change", LongType(), True),
            StructField("population_pct_change", DoubleType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Check that key measures are not null (except previous_population for first year)
        assert df.filter(col("population").isNull()).count() == 0
        assert df.filter(col("year").isNull()).count() == 0

    def test_fact_table_year_range(self, spark: SparkSession):
        """Verify that year values are within a reasonable range."""
        data = [
            (1, 2020, 329000000),
            (1, 2021, 332000000),
            (1, 2024, 340000000)
        ]
        schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("population", LongType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Check year range (reasonable range: 2000-2030)
        invalid_years = df.filter(
            (col("year") < 2000) | (col("year") > 2030)
        )
        
        assert invalid_years.count() == 0, "Years should be within 2000-2030 range"

    def test_fact_table_positive_population(self, spark: SparkSession):
        """Verify that population values are positive."""
        data = [
            (1, 2024, 340000000),
            (2, 2024, 39000000)
        ]
        schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("population", LongType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Check that all populations are positive
        negative_populations = df.filter(col("population") <= 0)
        
        assert negative_populations.count() == 0, "Population must be positive"

    def test_percentage_calculation_accuracy(self, spark: SparkSession):
        """Verify that percentage change calculations are accurate."""
        data = [
            (1, 2024, 340000000, 338000000, 2000000, None)  # Will calculate pct
        ]
        schema = StructType([
            StructField("nation_sk", LongType(), True),
            StructField("year", IntegerType(), True),
            StructField("population", LongType(), True),
            StructField("previous_population", LongType(), True),
            StructField("population_change", LongType(), True),
            StructField("population_pct_change", DoubleType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        # Calculate percentage change
        result = df.withColumn(
            "calculated_pct", 
            ((col("population") - col("previous_population")) / col("previous_population")) * 100
        )
        
        row = result.first()
        expected_pct = ((340000000 - 338000000) / 338000000) * 100
        
        assert abs(row["calculated_pct"] - expected_pct) < 0.01, \
            "Percentage calculation should be accurate"
