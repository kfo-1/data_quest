"""Tests for utility functions in datausa_load and bls_load."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import sys
import pathlib

# Add src directories to Python path for imports
src_path = pathlib.Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path / "datausa_load"))
sys.path.insert(0, str(src_path / "bls_load"))

from utilities.utils import clean_column_names


class TestCleanColumnNames:
    """Tests for the clean_column_names utility function."""

    def test_clean_simple_column_names(self, spark: SparkSession):
        """Verify that spaces and tabs are replaced with underscores and names are lowercased."""
        # Create test DataFrame with messy column names
        data = [(1, "test", 100)]
        df = spark.createDataFrame(data, ["Column Name", "Another Column", "ID Number"])
        
        # Clean column names
        result = clean_column_names(df)
        
        # Verify cleaned names
        expected_columns = ["column_name", "another_column", "id_number"]
        assert result.columns == expected_columns

    def test_clean_column_names_with_tabs(self, spark: SparkSession):
        """Verify that tabs are removed from column names."""
        data = [(1, 2, 3)]
        df = spark.createDataFrame(data, ["Tab\tColumn", "Space Column", "NormalColumn"])
        
        result = clean_column_names(df)
        
        expected_columns = ["tabcolumn", "space_column", "normalcolumn"]
        assert result.columns == expected_columns

    def test_clean_nested_struct_columns(self, spark: SparkSession):
        """Verify that nested struct field names are also cleaned."""
        # Create DataFrame with nested struct
        schema = StructType([
            StructField("ID", IntegerType()),
            StructField("User Data", StructType([
                StructField("First Name", StringType()),
                StructField("Last Name", StringType())
            ]))
        ])
        
        data = [(1, ("John", "Doe"))]
        df = spark.createDataFrame(data, schema)
        
        result = clean_column_names(df)
        
        # Check top-level columns
        assert result.columns == ["id", "user_data"]
        
        # Check nested struct field names
        nested_fields = [field.name for field in result.schema["user_data"].dataType.fields]
        assert nested_fields == ["first_name", "last_name"]

    def test_clean_array_of_structs(self, spark: SparkSession):
        """Verify that struct fields inside arrays are cleaned."""
        schema = StructType([
            StructField("ID", IntegerType()),
            StructField("Items", ArrayType(StructType([
                StructField("Item Name", StringType()),
                StructField("Item Price", IntegerType())
            ])))
        ])
        
        data = [(1, [("Apple", 100), ("Orange", 150)])]
        df = spark.createDataFrame(data, schema)
        
        result = clean_column_names(df)
        
        # Check that array element struct fields are cleaned
        array_struct_fields = [field.name for field in result.schema["items"].dataType.elementType.fields]
        assert array_struct_fields == ["item_name", "item_price"]

    def test_preserve_data_after_cleaning(self, spark: SparkSession):
        """Verify that data values are preserved after column name cleaning."""
        data = [(1, "Alice", 25), (2, "Bob", 30)]
        df = spark.createDataFrame(data, ["User ID", "User Name", "User Age"])
        
        result = clean_column_names(df)
        result_data = result.collect()
        
        # Verify data integrity
        assert len(result_data) == 2
        assert result_data[0]["user_id"] == 1
        assert result_data[0]["user_name"] == "Alice"
        assert result_data[0]["user_age"] == 25
        assert result_data[1]["user_id"] == 2
        assert result_data[1]["user_name"] == "Bob"
        assert result_data[1]["user_age"] == 30

    def test_leading_trailing_underscores_removed(self, spark: SparkSession):
        """Verify that leading and trailing underscores are stripped."""
        data = [(1, 2, 3)]
        df = spark.createDataFrame(data, [" Leading Space", "Trailing Space ", "  Both  "])
        
        result = clean_column_names(df)
        
        # Should strip leading/trailing underscores that result from spaces
        expected_columns = ["leading_space", "trailing_space", "both"]
        assert result.columns == expected_columns
