from pyspark import pipelines as dp
from pyspark.sql.functions import col, explode, from_json
from utilities.utils import clean_column_names
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType

# Step 1: Temporary view with transformations
@dp.temporary_view()
def population_nation_data_transformed():
    """
    Transform bronze population data:
    - Standardize Year column to integer
    - Create observation_date (end of year)
    - Add derived date columns
    - Clean and standardize text fields
    """
    df = spark.readStream.table("brz_population_data")
    
    df = df.withColumn("data_explode", explode(from_json(col("data"),ArrayType(
            StructType([
                StructField("Nation", StringType(), True),
                StructField("ID Nation", StringType(), True),
                StructField("Year", StringType(), True),
                StructField("Population", LongType(), True)
                ])
        ))))
    df = df.select("data_explode.*", "load_timestamp")

    # Clean column names
    df = clean_column_names(df)

    # Remove duplication
    df = df.dropDuplicates(["id_nation"])

    return df.select("nation", "id_nation","load_timestamp")

# Step 2: Create target streaming table with explicit schema
dp.create_streaming_table(
    name="slv_population_nation_dim",
    comment="Silver dim table - Information on a Nation received",
    schema="""
        id_nation STRING,
        nation STRING,
        load_timestamp TIMESTAMP
    """
)

# Step 3: Auto CDC flow with deduplication on (year, id_nation)
dp.create_auto_cdc_flow(
    target="slv_population_nation_dim",
    source="population_nation_data_transformed",
    keys=["id_nation"],  # Composite key for deduplication
    sequence_by="load_timestamp",  # Latest load wins
    stored_as_scd_type=1,  # Keep only latest value (no history)
    ignore_null_updates=True  # Don't overwrite with nulls
)
