# datausa_load

DataUSA API population data extraction and transformation pipeline.

## Pipeline Overview

This pipeline processes US population data from the DataUSA API:

1. **Data Extraction**: `Data_USA_API_Get_Data` notebook fetches data and writes JSON to `/Volumes/{catalog}/{schema}/quest_data/datausa_data/{YYYY/MM/DD}/`
2. **Bronze Layer**: Reads raw JSON files and explodes to individual records
3. **Silver Layer**: Cleans, deduplicates, and standardizes population data

## Folder Structure

- `transformations/bronze/`: Bronze layer tables (raw data from JSON files)
  - `brz_population_data.py` - Raw population records from API responses
  
- `transformations/silver/`: Silver layer tables (cleaned and deduplicated)
  - `slv_population_facts.py` - Deduplicated population facts by year

## Data Flow

```
DataUSA API
  → Data_USA_API_Get_Data notebook
    → /Volumes/.../quest_data/datausa_data/{date}/*.json
      → brz_population_data (Bronze)
        → slv_population_facts (Silver - AUTO CDC)
```

## Tables

### Bronze Layer
* **brz_population_data**: Raw population data extracted from JSON API responses
  - Source: `/Volumes/{catalog}/{schema}/quest_data/datausa_data/*/*/*/*` (date-partitioned)
  - Contains: Year, Nation, ID_Year, ID_Nation, Slug_Nation, Population, load_timestamp

### Silver Layer
* **slv_population_facts**: Cleaned and deduplicated population facts
  - Deduplication key: (year, id_nation)
  - Contains standardized dates and cleaned text fields
  - Uses AUTO CDC (SCD Type 1) - latest record wins

## Getting Started

1. Run the extraction notebook first: `Data_USA_API_Get_Data`
2. Deploy the pipeline: `databricks bundle deploy -t dev`
3. Run the pipeline: `databricks bundle run -t dev datausa_load`

For more information, see https://docs.databricks.com/spark-declarative-pipelines/