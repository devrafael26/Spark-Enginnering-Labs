# Databricks notebook source
df_5m = spark.range(5_000_000)
df_5m.explain(True)

# COMMAND ----------

df_40m = spark.range(40_000_000)
df_40m.explain(True)

# COMMAND ----------

df_300m = spark.range(300_000_000)
df_300m.explain(True)