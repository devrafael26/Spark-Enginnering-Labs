# Databricks notebook source
df = spark.range(1_000_000)

# COMMAND ----------

df_filtered = df.filter("id > 500000")

df_selected = df_filtered.select("id")

# COMMAND ----------

df_selected.explain(True)