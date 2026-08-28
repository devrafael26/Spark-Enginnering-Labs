# Databricks notebook source
df = spark.range(1_000_000)

# COMMAND ----------


df_grouped = (
    df
    .withColumn("group_id", df.id % 10)
    .groupBy("group_id")
    .count()
)


# COMMAND ----------

df_grouped.show()

# COMMAND ----------

df_grouped.explain(True)