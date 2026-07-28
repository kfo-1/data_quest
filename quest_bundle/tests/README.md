# Test Suite Documentation

This directory contains comprehensive unit tests for the quest_bundle DABs project, covering utility functions, data transformations, and dimensional model integrity.

## Test Files

### 1. `test_utils.py`
Tests for utility functions used across the project.

**Coverage:**
* `clean_column_names()` function
* Space and tab handling in column names
* Nested struct field name cleaning
* Array of structs cleaning
* Data preservation after cleaning
* Leading/trailing underscore removal

**Key Tests:**
* `test_clean_simple_column_names` - Verifies basic column name cleaning
* `test_clean_nested_struct_columns` - Tests nested struct field cleaning
* `test_preserve_data_after_cleaning` - Ensures data integrity

### 2. `test_population_transformations.py`
Tests for silver and gold layer population data transformations.

**Coverage:**
* Silver layer schema validation
* Population data type casting (handling scientific notation)
* Nation dimension deduplication
* Gold dimension identity column (surrogate key)
* Year-over-year population calculations
* Foreign key relationships
* Null value handling

**Key Tests:**
* `test_population_data_casting` - Verifies scientific notation → long conversion
* `test_year_over_year_population_change` - Tests LAG window function calculations
* `test_foreign_key_relationship` - Validates dimension-fact joins

### 3. `test_dimensional_model.py`
Tests for dimensional model structure and data quality rules.

**Coverage:**
* Dimension table structure (surrogate keys)
* Fact table structure (composite primary keys)
* Surrogate key uniqueness
* Business key uniqueness
* Referential integrity (foreign key constraints)
* Data quality rules (null checks, range validation)
* Calculation accuracy

**Key Tests:**
* `test_dimension_surrogate_key_uniqueness` - Ensures unique surrogate keys
* `test_fact_table_composite_primary_key` - Validates (nation_sk, year) uniqueness
* `test_referential_integrity_all_foreign_keys_exist` - Checks FK → PK relationships
* `test_percentage_calculation_accuracy` - Verifies year-over-year % calculations

## Running the Tests

### Prerequisites

The project uses pytest with Databricks Connect. Dependencies are managed in `pyproject.toml`.

### Run All Tests

From the project root directory:

```bash
uv run pytest tests/
```

### Run Specific Test File

```bash
uv run pytest tests/test_utils.py
uv run pytest tests/test_population_transformations.py
uv run pytest tests/test_dimensional_model.py
```

### Run Specific Test Class

```bash
uv run pytest tests/test_utils.py::TestCleanColumnNames
uv run pytest tests/test_population_transformations.py::TestGoldPopulationTransformations
uv run pytest tests/test_dimensional_model.py::TestDataQuality
```

### Run Specific Test Function

```bash
uv run pytest tests/test_utils.py::TestCleanColumnNames::test_clean_simple_column_names
uv run pytest tests/test_dimensional_model.py::TestFactTables::test_referential_integrity_all_foreign_keys_exist
```

### Run with Verbose Output

```bash
uv run pytest tests/ -v
```

### Run with Coverage Report

```bash
uv run pytest tests/ --cov=src --cov-report=html
```

## Test Fixtures

The `conftest.py` file provides shared fixtures:

* **`spark`** - Provides a DatabricksSession (Spark session with Databricks Connect)
* **`load_fixture`** - Callable to load JSON or CSV test data from `fixtures/` directory

### Using Fixtures in Tests

```python
def test_example(spark: SparkSession):
    """Example test using the spark fixture."""
    df = spark.createDataFrame([(1, "test")], ["id", "name"])
    assert df.count() == 1

def test_with_fixture_data(load_fixture):
    """Example test loading fixture data."""
    df = load_fixture("sample_data.json")
    assert df.count() > 0
```

## Test Data Organization

Place test fixture files in the `fixtures/` directory:

```
fixtures/
├── sample_population.json
├── sample_nations.csv
└── .gitkeep
```

## What the Tests Verify

### Data Transformation Tests
1. ✅ Schema correctness (column names, data types)
2. ✅ Scientific notation → long integer casting
3. ✅ Window function calculations (LAG for year-over-year)
4. ✅ Deduplication logic
5. ✅ Join operations (dimension-fact relationships)

### Dimensional Model Tests
1. ✅ Surrogate key presence and uniqueness
2. ✅ Business key uniqueness
3. ✅ Composite primary key uniqueness
4. ✅ Foreign key relationships (referential integrity)
5. ✅ NOT NULL constraints

### Data Quality Tests
1. ✅ No null values in required fields
2. ✅ Year values within reasonable range (2000-2030)
3. ✅ Population values are positive
4. ✅ Percentage calculations are accurate
5. ✅ Data preservation through transformations

## CI/CD Integration

These tests can be integrated into your CI/CD pipeline. See `.github/workflows/deploy.yml` for an example.

Example GitHub Actions job:

```yaml
test:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'
    - name: Install dependencies
      run: |
        pip install uv
        uv sync
    - name: Run tests
      run: uv run pytest tests/ -v
      env:
        DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
        DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

## Adding New Tests

When adding new transformations or business logic:

1. **Create test file**: Follow naming convention `test_<module_name>.py`
2. **Organize into classes**: Group related tests in test classes
3. **Use fixtures**: Leverage the `spark` and `load_fixture` fixtures from `conftest.py`
4. **Document**: Add docstrings explaining what each test verifies
5. **Follow patterns**: Use existing tests as templates

### Test Structure Template

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class TestMyTransformation:
    """Tests for my transformation logic."""

    def test_transformation_schema(self, spark: SparkSession):
        """Verify output schema is correct."""
        # Arrange: Create input data
        data = [(1, "test")]
        df = spark.createDataFrame(data, ["id", "name"])
        
        # Act: Apply transformation
        result = my_transformation(df)
        
        # Assert: Verify results
        assert "new_column" in result.columns
        assert result.count() == 1
```

## Troubleshooting

### ImportError: Test dependencies not found

**Solution:** Ensure you're running tests with `uv run pytest` (not plain `pytest`). This ensures the virtual environment with all dependencies is used.

### Connection Error to Databricks

**Solution:** Verify your Databricks Connect configuration:
```bash
databricks auth login --host <your-workspace-url>
```

### Module Not Found Errors

**Solution:** Ensure the `src/` directories are in the Python path. The test files already add them:
```python
src_path = pathlib.Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path / "datausa_load"))
```

## Additional Resources

* [pytest Documentation](https://docs.pytest.org/)
* [PySpark Testing Guide](https://spark.apache.org/docs/latest/api/python/user_guide/testing.html)
* [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect.html)
