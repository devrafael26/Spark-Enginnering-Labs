# Databricks notebook source
df = spark.range(1_000_000)

# COMMAND ----------

df_filtered = df.filter("id > 500000")

df_selected = df_filtered.select("id")

# COMMAND ----------

# O explain() não é uma Action que executa o processamento dos dados. Ele solicita ao Spark a representação do plano.
df_selected.explain(True)