# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC What are the mean and standard deviation of the annual US population across 2013-2018 inclusive?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   AVG(population) AS mean_population,
# MAGIC   STDDEV(population) AS stddev_population
# MAGIC FROM datasets.kbf1984.gold_fact_population
# MAGIC WHERE year BETWEEN 2013 AND 2018;

# COMMAND ----------

from pyspark.sql.functions import mean, stddev

df = spark.read.table("datasets.kbf1984.gold_fact_population")

df = df.filter("year between 2013 and 2018").agg(mean("population").alias("mean_population"), stddev("population").alias("stddev_population"))
display(df)