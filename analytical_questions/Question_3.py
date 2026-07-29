# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC For `series_id = PRS30006032` and `period = Q01`, what was the `value` each year, joined with that year's `population` where available?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     f.series_id,
# MAGIC     f.year,
# MAGIC     f.value,
# MAGIC     pop.population
# MAGIC FROM datasets.kbf1984.gold_fact_productivity f
# MAGIC INNER JOIN datasets.kbf1984.gold_dim_period p on f.period_sk = p.period_sk and p.period = 'Q01'
# MAGIC LEFT JOIN datasets.kbf1984.gold_fact_population pop on f.year = pop.year
# MAGIC WHERE trim(f.series_id) = 'PRS30006032'
# MAGIC ORDER BY year desc

# COMMAND ----------

from pyspark.sql.functions import col, trim

df = spark.read.table("datasets.kbf1984.gold_fact_productivity").filter(trim("series_id") == "PRS30006032")
df_p = spark.read.table("datasets.kbf1984.gold_dim_period").filter("period = 'Q01'")
df_pop = spark.read.table("datasets.kbf1984.gold_fact_population")

# Join tables together
df = (df.join(df_p, on="period_sk", how="inner")
      .join(df_pop, on="year", how="left")
      .select(df.series_id, df.year, df.value, df_pop.population)
)

display(df.orderBy(col("year").desc()))