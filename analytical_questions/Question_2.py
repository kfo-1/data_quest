# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC For every `series_id` in the BLS data, what is the **best year**, meaning the year with the largets sum of `value` across all its quarters?

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH year_sum
# MAGIC (
# MAGIC     SELECT
# MAGIC         f.series_id,
# MAGIC         f.year,
# MAGIC         sum(f.value) as value_sum,
# MAGIC         rank() over (PARTITION BY f.series_id ORDER BY sum(f.value)) as rank
# MAGIC     
# MAGIC     FROM datasets.kbf1984.gold_fact_productivity f
# MAGIC     INNER JOIN datasets.kbf1984.gold_dim_period p ON f.period_sk = p.period_sk AND p.period <> 'Q05'
# MAGIC     GROUP BY f.series_id, f.year
# MAGIC )
# MAGIC SELECT
# MAGIC     y.series_id,
# MAGIC     y.year,
# MAGIC     y.value_sum
# MAGIC FROM year_sum y
# MAGIC
# MAGIC WHERE y.rank = 1
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

