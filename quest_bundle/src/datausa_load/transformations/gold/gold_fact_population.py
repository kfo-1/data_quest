from pyspark import pipelines as dp
from pyspark.sql.functions import col, lag
from pyspark.sql.window import Window

@dp.table(
    name="gold_fact_population",
    comment="Gold table - US population with year-over-year changes",
    table_properties={
        "pipelines.primaryKey": "nation_sk, year",
        "delta.constraints.gold_population_nation_fk": "nation_sk IS NOT NULL"
    }
)
def gold_fact_population_transformed():
    """
    Transform silver population facts to gold:
    - Join with nation dimension to get surrogate key
    - Add previous year's population using window function
    - Calculate absolute population change
    - Calculate percentage population change
    """
    df = spark.read.table("slv_population")
    
    # Stream-static join with nation dimension to get surrogate key
    nation_dim = spark.read.table("gold_dim_nation").select("nation_sk", "nation_id")
    df = df.join(nation_dim, "nation_id", "left")
    
    # Define window partitioned by nation_sk, ordered by year
    window_spec = Window.partitionBy("nation_sk").orderBy("year")
    
    # Add previous population and calculate changes using withColumns for better performance
    df = df.withColumns({
        "previous_population": lag("population", 1).over(window_spec),
        "population_change": col("population") - lag("population", 1).over(window_spec),
        "population_pct_change": ((col("population") - lag("population", 1).over(window_spec)) / lag("population", 1).over(window_spec)) * 100
    })
    
    # Select final columns
    return df.select(
        "nation_sk",
        "year", 
        "population",
        "previous_population",
        "population_change",
        "population_pct_change"
    )


