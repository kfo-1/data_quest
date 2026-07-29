from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, explode, col
from utilities.utils import clean_column_names

@dp.table(
    comment="Bronze layer - Raw DataUSA population data from API responses (incremental with Auto Loader)"
)
@dp.expect(
    "data_element_populated",
    "data IS NOT NULL"
)
def brz_population_data():
    # Get catalog and schema from pipeline configuration
    catalog = spark.conf.get("source_catalog")
    schema = spark.conf.get("source_schema")
    
    # Build parameterized volume paths
    data_path = f"/Volumes/{catalog}/{schema}/quest_data/datausa_data"
    schema_path=f"/Volumes/{catalog}/{schema}/quest_data/_schemas/datausa_population/_schemas"

    # Read JSON files incrementally with Auto Loader (cloudFiles)
    # Only processes NEW files since last pipeline run
    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("includeExistingFiles", "true")
            .option("multiline","true")
            .load(data_path)
    )
    
    # Add load_timestamp after reading
    df = (
        df.withColumn("load_timestamp", current_timestamp())
        .withColumn("file_arrival_timestamp", col("_metadata.file_modification_time"))
    )

    # Clean column names
    df = clean_column_names(df)
    return df
