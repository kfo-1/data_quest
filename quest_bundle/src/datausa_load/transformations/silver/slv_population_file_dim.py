# from pyspark import pipelines as dp
# from utilities.utils import clean_column_names

# # Step 1: Temporary view with transformations
# @dp.temporary_view()
# def population_file_data_transformed():
#     """
#     Transform bronze population data:
#     - Standardize Year column to integer
#     - Create observation_date (end of year)
#     - Add derived date columns
#     - Clean and standardize text fields
#     """
#     df = spark.readStream.table("brz_population_data")
    
#     df = df.select("annotations.*", "load_timestamp")

#     # Clean column names
#     df = clean_column_names(df)

#     # Remove duplication
#     df = df.dropDuplicates(["table_id"])

#     return df

# # Step 2: Create target streaming table with explicit schema
# dp.create_streaming_table(
#     name="slv_population_file_dim",
#     comment="Silver dim table - Information on file for data received",
#     schema="""
#         dataset_link STRING,
#         dataset_name STRING,
#         source_description STRING,
#         source_name STRING,
#         subtopic STRING,
#         table_id STRING,
#         topic STRING,
#         load_timestamp TIMESTAMP
#     """
# )

# # Step 3: Auto CDC flow with deduplication on (year, id_nation)
# dp.create_auto_cdc_flow(
#     target="slv_population_file_dim",
#     source="population_file_data_transformed",
#     keys=["table_id"],  # Composite key for deduplication
#     sequence_by="load_timestamp",  # Latest load wins
#     stored_as_scd_type=1,  # Keep only latest value (no history)
#     ignore_null_updates=True  # Don't overwrite with nulls
# )
