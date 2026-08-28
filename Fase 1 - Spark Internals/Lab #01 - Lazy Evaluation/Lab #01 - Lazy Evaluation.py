# Databricks notebook source
# AQUI TEMOS AS TRANSFORMÇAÕES

# O Spark construiu o fluxo de operações, mas ainda não houve uma solicitação do resultado.

df = spark.range(1_000_000)

df_filtered = df.filter("id > 500000")

df_selected = df_filtered.select("id")



# COMMAND ----------

# AQUI TEMOS TRANSFORMAÇÃO + AÇÃO

# O show() é uma Action que solicita os dados.
df_selected.show(5)

# A execução da Action show() é registrada pelo Hide Performance, incluindo sua duração. As métricas de Rows Read e Bytes Read podem aparecer zerados, pois os dados foram gerados pelo spark.range().

# COMMAND ----------

# FLUXO

# spark.range()
#       ↓
# dados gerados pelo Spark
#       ↓
# filter()
#       ↓
# select()
#       ↓
# show()
#       ↓
# EXECUÇÃO

# COMMAND ----------

df_selected.explain(True)