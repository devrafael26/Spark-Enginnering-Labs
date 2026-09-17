# Databricks notebook source
# MAGIC %md
# MAGIC ### Experimento A — Primeiro micro-batch
# MAGIC
# MAGIC - A ideia aqui é simples: criar uma tabela Delta de origem, inserir dados nela e depois consumi-la com Structured Streaming.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.lab25_volume;

# COMMAND ----------

from pyspark.sql import functions as F

catalog = "workspace"
schema = "default"

source_table = f"{catalog}.{schema}.lab25_source"
target_table = f"{catalog}.{schema}.lab25_target"

checkpoint_path = "/Volumes/workspace/default/lab25_volume/checkpoints/exp_a"

# COMMAND ----------

# Criando a origem
spark.sql(f"DROP TABLE IF EXISTS {source_table}")
spark.sql(f"DROP TABLE IF EXISTS {target_table}")

df_source = (
    spark.range(0, 1000)
    .withColumn("valor", (F.col("id") * 10))
    .withColumn("created_at", F.current_timestamp())
)

df_source.write.format("delta").mode("overwrite").saveAsTable(source_table)

# COMMAND ----------

df_stream = (
    spark.readStream
    .format("delta")
    .table(source_table)
)

print("É streaming?", df_stream.isStreaming)

# COMMAND ----------

query_a = (
    df_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable(target_table)
)

query_a.awaitTermination()

# COMMAND ----------

spark.table(target_table).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Experimento B — Provocar vários micro-batches
# MAGIC - Vamos colocar vários arquivos Delta na source. Para isso, faremos várias gravações independentes.

# COMMAND ----------

source_b = f"{catalog}.{schema}.lab25_source_b"
target_b = f"{catalog}.{schema}.lab25_target_b"

checkpoint_path_b = "/Volumes/workspace/default/lab25_volume/checkpoints/exp_b"

spark.sql(f"DROP TABLE IF EXISTS {source_b}")
spark.sql(f"DROP TABLE IF EXISTS {target_b}")

# COMMAND ----------

# criando 5 pequenos lotes físicos na tabela de origem

for lote in range(5):

    df_lote = (
        spark.range(lote * 1000, (lote + 1) * 1000)
        .withColumn("lote_origem", F.lit(lote))
    )

    (
        df_lote.write
        .format("delta")
        .mode("append")
        .saveAsTable(source_b)
    )

# COMMAND ----------

# maxFilesPerTrigger=1 limita quantos novos arquivos podem ser consumidos por micro-batch.

df_stream_b = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 1)
    .table(source_b)
)

# COMMAND ----------

query_b = (
    df_stream_b
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path_b)
    .trigger(availableNow=True)
    .toTable(target_b)
)

query_b.awaitTermination()

# COMMAND ----------

dbutils.fs.rm(checkpoint_path_b, True)

# COMMAND ----------

spark.table(target_b).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Experimento C — enxergar os micro-batches
# MAGIC - Agora vamos pegar justamente a query_b e olhar o histórico de progresso.

# COMMAND ----------

query_b.awaitTermination()

# COMMAND ----------

progressos = query_b.recentProgress

print("Quantidade de progressos registrados:", len(progressos))

# COMMAND ----------

for p in progressos:
    print(
        "batchId:", p.batchId,
        "| numInputRows:", p.numInputRows,
        "| inputRowsPerSecond:", p.inputRowsPerSecond,
        "| processedRowsPerSecond:", p.processedRowsPerSecond
    )

# COMMAND ----------

for p in progressos:
    print(
        f"Batch {p.batchId}",
        "| Linhas:", p.numInputRows,
        "| Duração:", p.durationMs
    )

# COMMAND ----------

# checkpoint_c = "/Volumes/workspace/default/lab25_volume/checkpoints/exp_c"

# COMMAND ----------

dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL workspace.default.lab25_source_b;

# COMMAND ----------

from pyspark.sql import functions as F

arquivos_source_b = (
    spark.table("workspace.default.lab25_source_b")
    .select(
        F.col("_metadata.file_name").alias("file_name"),
        F.col("_metadata.file_path").alias("file_path")
    )
    .groupBy("file_name", "file_path")
    .count()
    .orderBy("file_name")
)

display(arquivos_source_b)