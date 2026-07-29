# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC For every `series_id` in the BLS data, what is the **best year**, meaning the year with the largets sum of `value` across all its quarters?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH year_sum AS (
# MAGIC     SELECT
# MAGIC         f.series_id,
# MAGIC         f.year,
# MAGIC         sum(f.value) as value_sum,
# MAGIC         rank() over (PARTITION BY f.series_id ORDER BY sum(f.value) desc) as rank
# MAGIC     FROM datasets.kbf1984.gold_fact_productivity f
# MAGIC     INNER JOIN datasets.kbf1984.gold_dim_period p ON f.period_sk = p.period_sk AND p.period <> 'Q05'
# MAGIC     GROUP BY f.series_id, f.year
# MAGIC )
# MAGIC SELECT 
# MAGIC     s.series_id,
# MAGIC     c.class_text,
# MAGIC     sec.sector_name,
# MAGIC     m.measure_text,
# MAGIC     d.duration_text,
# MAGIC     y.year,
# MAGIC     y.value_sum
# MAGIC FROM datasets.kbf1984.gold_dim_series s
# MAGIC INNER JOIN datasets.kbf1984.gold_dim_class c ON s.class_code = c.class_code
# MAGIC INNER JOIN datasets.kbf1984.gold_dim_sector sec ON s.sector_code = sec.sector_code
# MAGIC INNER JOIN datasets.kbf1984.gold_dim_duration d ON s.duration_code = d.duration_code
# MAGIC INNER JOIN datasets.kbf1984.gold_dim_measure m ON s.measure_code = m.measure_code 
# MAGIC INNER JOIN year_sum y ON s.series_id = y.series_id
# MAGIC WHERE y.rank = 1

# COMMAND ----------

from pyspark.sql.functions import col, rank, sum
from pyspark.sql.window import Window

# Define window partitioned by series_id
window_spec = Window.partitionBy("series_id").orderBy(col("value_sum").desc())

df_prod = spark.read.table("datasets.kbf1984.gold_fact_productivity")
df_per = spark.read.table("datasets.kbf1984.gold_dim_period").filter("period <> 'Q05'")
df_ser = spark.read.table("datasets.kbf1984.gold_dim_series")
df_class = spark.read.table("datasets.kbf1984.gold_dim_class")
df_measure = spark.read.table("datasets.kbf1984.gold_dim_measure")
df_duration = spark.read.table("datasets.kbf1984.gold_dim_duration")
df_sector = spark.read.table("datasets.kbf1984.gold_dim_sector")

df_year_sum = (df_prod.join(df_per, on=["period_sk"], how="inner")
        .groupBy("series_id", "year")
        .agg(sum("value").alias("value_sum"))
        .withColumn("rank", rank().over(window_spec))
)
df_year_sum = df_year_sum.filter("rank = 1").select("series_id", "year", "value_sum", "rank")

df = df_ser.join(df_year_sum, on=["series_id"], how="inner").filter("rank = 1")
df = (df.join(df_class, on=["class_code"], how="inner")
        .join(df_measure, on=["measure_code"], how="inner")
        .join(df_duration, on=["duration_code"], how="inner")
        .join(df_sector, on=["sector_code"], how="inner")
)
df = df.select("series_id", "class_text", "sector_name", "measure_text", "duration_text", "year", "value_sum")

display(df)