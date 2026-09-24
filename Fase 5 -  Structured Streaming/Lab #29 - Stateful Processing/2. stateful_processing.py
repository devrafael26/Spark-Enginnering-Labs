
############ Experimento A — Stateless vs Stateful ############ 

# A ideia aqui é simples: executar primeiro um filter, que não precisa lembrar dados anteriores, e depois uma agregação por cliente, que precisa manter estado.


#  %sql
#  CREATE VOLUME IF NOT EXISTS workspace.default.lab29_volume;


from pyspark.sql import functions as F

sourceA = "workspace.default.lab29_source_a"
target_stateless = "workspace.default.lab29_stateless_a"
target_stateful = "workspace.default.lab29_stateful_a"

checkpoint_stateless = "/Volumes/workspace/default/lab29_volume/checkpoint_a_stateless"
checkpoint_stateful  = "/Volumes/workspace/default/lab29_volume/checkpoint_a_stateful"

spark.sql(f"DROP TABLE IF EXISTS {sourceA}")
spark.sql(f"DROP TABLE IF EXISTS {target_stateless}")
spark.sql(f"DROP TABLE IF EXISTS {target_stateful}")

dbutils.fs.rm(checkpoint_stateless, True)
dbutils.fs.rm(checkpoint_stateful, True)

data = [
    ("A", 100.0),
    ("A", 50.0),
    ("B", 200.0),
    ("C", 80.0)
]

df = spark.createDataFrame(
    data,
    ["customer_id", "value"]
)

df.write.format("delta").mode("overwrite").saveAsTable(sourceA)

display(spark.table(sourceA))



# Operação Stateless

df_stream_stateless = (
    spark.readStream
    .format("delta")
    .table(sourceA)
)

df_filtered = (
    df_stream_stateless
    .filter(F.col("value") >= 100)
)

query_stateless = (
    df_filtered.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_stateless)
    .trigger(availableNow=True)
    .toTable(target_stateless)
)

query_stateless.awaitTermination()

display(spark.table(target_stateless))



# olhamos se existiu operador de estado
# A expectativa é que stateOperators esteja vazio, porque o filtro não precisa manter estado entre micro-batches.

for p in query_stateless.recentProgress:
    print("batchId:", p.batchId)
    print("inputRows:", p.numInputRows)
    print("stateOperators:", p.stateOperators)
    print("-" * 50)



# Operação Stateful

df_stream_stateful = (
    spark.readStream
    .format("delta")
    .table(sourceA)
)

df_agg = (
    df_stream_stateful
    .groupBy("customer_id")
    .agg(
        F.sum("value").alias("total_value")
    )
)

query_stateful = (
    df_agg.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", checkpoint_stateful)
    .trigger(availableNow=True)
    .toTable(target_stateful)
)

query_stateful.awaitTermination()

display(
    spark.table(target_stateful)
    .orderBy("customer_id")
)



for p in query_stateful.recentProgress:
    print("batchId:", p.batchId)
    print("inputRows:", p.numInputRows)

    for s in p.stateOperators:
        print("operatorName:", s.operatorName)
        print("numRowsTotal:", s.numRowsTotal)
        print("numRowsUpdated:", s.numRowsUpdated)
        print("memoryUsedBytes:", s.memoryUsedBytes)

    print("-" * 50)



# Validação de numInputRows -> query de controle sem filtro


checkpoint_control = "/Volumes/workspace/default/lab29_volume/checkpoint_a_control"
target_control = "workspace.default.lab29_control_a"

spark.sql(f"DROP TABLE IF EXISTS {target_control}")
dbutils.fs.rm(checkpoint_control, True)

df_stream_control = (
    spark.readStream
    .format("delta")
    .table(sourceA)
)

query_control = (
    df_stream_control.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_control)
    .trigger(availableNow=True)
    .toTable(target_control)
)

query_control.awaitTermination()



display(spark.table(target_control))



for p in query_control.recentProgress:
    print("batchId:", p.batchId)
    print("query numInputRows:", p.numInputRows)

    for source in p.sources:
        print("source numInputRows:", source.numInputRows)




############  Experimento B — Ver o State evoluindo ############ 


sourceB = "workspace.default.lab29_source_b"
targetB = "workspace.default.lab29_stateful_b"

checkpointB = "/Volumes/workspace/default/lab29_volume/checkpoint_b"

spark.sql(f"DROP TABLE IF EXISTS {sourceB}")
spark.sql(f"DROP TABLE IF EXISTS {targetB}")

dbutils.fs.rm(checkpointB, True)


batch1 = [
    ("A", 100.0),
    ("B", 50.0)
]

df_batch1 = spark.createDataFrame(
    batch1,
    ["customer_id", "value"]
)

df_batch1.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(sourceB)



df_stream_b = (
    spark.readStream
    .format("delta")
    .table(sourceB)
)

# operação stateful -> agregações streaming
df_state_b = (
    df_stream_b
    .groupBy("customer_id")
    .agg(
        F.sum("value").alias("total_value")
    )
)


# Sobre .outputMode
# Append: emite somente resultados que não vão mais mudar.
# Update: emite resultados alterados naquele trigger.
# Complete: emite toda a tabela resultante da agregação.

query_b1 = (
    df_state_b.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", checkpointB)
    .trigger(availableNow=True)
    .toTable(targetB)
)

query_b1.awaitTermination()



display(
    spark.table(targetB)
    .orderBy("customer_id")
)



# Inserir novos dados

batch2 = [
    ("A", 30.0),
    ("C", 70.0)
]

df_batch2 = spark.createDataFrame(
    batch2,
    ["customer_id", "value"]
)

df_batch2.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(sourceB)



query_b2 = (
    df_state_b.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", checkpointB)
    .trigger(availableNow=True)
    .toTable(targetB)
)

query_b2.awaitTermination()



display(
    spark.table(targetB)
    .orderBy("customer_id")
)




# OBS.: De onde vieram os 100 anteriores do cliente A? 
# Não vieram do segundo micro-batch.
# O operador stateful manteve a informação necessária para continuar a agregação incremental. É justamente essa atualização incremental de estado que caracteriza uma query stateful segundo a documentação da Databricks.




############  Experimento C — Métricas do State ############ 



# Agora vamos inspecionar o que o Spark registrou.

progress = query_b2.recentProgress

for p in progress:

    print("=" * 70)
    print("batchId:", p.batchId)
    print("numInputRows:", p.numInputRows)

    if len(p.stateOperators) == 0:
        print("Nenhum operador stateful encontrado.")

    for s in p.stateOperators:

        print("operatorName:", s.operatorName)
        print("numRowsTotal:", s.numRowsTotal)
        print("numRowsUpdated:", s.numRowsUpdated)
        print("numRowsRemoved:", s.numRowsRemoved)
        print("memoryUsedBytes:", s.memoryUsedBytes)
        print(
            "numRowsDroppedByWatermark:",
            s.numRowsDroppedByWatermark
        )
        print(
            "numShufflePartitions:",
            s.numShufflePartitions
        )
        print(
            "numStateStoreInstances:",
            s.numStateStoreInstances
        )




############  Experimento D — Watermark removendo State antigo ############ 



from datetime import datetime
from pyspark.sql import functions as F

sourceD = "workspace.default.lab29_source_d"
targetD = "workspace.default.lab29_stateful_d"

checkpointD = "/Volumes/workspace/default/lab29_volume/checkpoint_d"

spark.sql(f"DROP TABLE IF EXISTS {sourceD}")
spark.sql(f"DROP TABLE IF EXISTS {targetD}")

dbutils.fs.rm(checkpointD, True)



data_d1 = [
    ("A", 100.0, datetime(2026, 9, 23, 10, 1, 0)),
    ("A", 50.0,  datetime(2026, 9, 23, 10, 3, 0)),
    ("B", 80.0,  datetime(2026, 9, 23, 10, 7, 0))
]

df_d1 = spark.createDataFrame(
    data_d1,
    ["customer_id", "value", "event_time"]
)

df_d1.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(sourceD)



# Agregação com Watermark

df_stream_d = (
    spark.readStream
    .format("delta")
    .table(sourceD)
)

df_windowed = (
    df_stream_d
    .withWatermark(
        "event_time",
        "10 minutes"
    )
    .groupBy(
        F.window(
            F.col("event_time"),
            "5 minutes"
        ),
        F.col("customer_id")
    )
    .agg(
        F.sum("value").alias("total_value")
    )
)



query_d1 = (
    df_windowed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointD)
    .trigger(availableNow=True)
    .toTable(targetD)
)

query_d1.awaitTermination()



display(
    spark.table(targetD)
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "customer_id",
        "total_value"
    )
    .orderBy("window_start", "customer_id")
)



for p in query_d1.recentProgress:

    print("batchId:", p.batchId)

    if p.eventTime:
        print("eventTime:", p.eventTime)

    for s in p.stateOperators:
        print("numRowsTotal:", s.numRowsTotal)
        print("numRowsUpdated:", s.numRowsUpdated)
        print("numRowsRemoved:", s.numRowsRemoved)
        print("memoryUsedBytes:", s.memoryUsedBytes)

    print("-" * 60)



# Fazer o tempo avançar -> adicionamos um evento bem mais à frente

data_d2 = [
    ("C", 200.0, datetime(2026, 9, 23, 10, 30, 0))
]

df_d2 = spark.createDataFrame(
    data_d2,
    ["customer_id", "value", "event_time"]
)

df_d2.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(sourceD)




query_d2 = (
    df_windowed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointD)
    .trigger(availableNow=True)
    .toTable(targetD)
)

query_d2.awaitTermination()



for p in query_d2.recentProgress:

    print("=" * 70)

    print("batchId:", p.batchId)
    print("numInputRows:", p.numInputRows)

    if p.eventTime:
        print("eventTime:", p.eventTime)

    for s in p.stateOperators:
        print("numRowsTotal:", s.numRowsTotal)
        print("numRowsUpdated:", s.numRowsUpdated)
        print("numRowsRemoved:", s.numRowsRemoved)
        print(
            "numRowsDroppedByWatermark:",
            s.numRowsDroppedByWatermark
        )
        print("memoryUsedBytes:", s.memoryUsedBytes)




# | Operação streaming   |     Stateful? | Por quê?                             |
# | -------------------- | ------------: | ------------------------------------ |
# | `SELECT col1, col2`  |           Não | Processa a linha atual               |
# | `WHERE value > 100`  |           Não | Não precisa lembrar o passado        |
# | `CASE WHEN...`       |           Não | Cálculo por linha                    |
# | `CAST()`             |           Não | Transformação por linha              |
# | `GROUP BY + SUM`     |           Sim | Precisa manter agregado anterior     |
# | `GROUP BY + COUNT`   |           Sim | Precisa manter contador              |
# | `AVG` por chave      |           Sim | Precisa manter informação acumulada  |
# | `DISTINCT`           |           Sim | Precisa lembrar valores já vistos    |
# | `dropDuplicates`     |           Sim | Precisa lembrar chaves já vistas     |
# | Stream × Stream Join |           Sim | Um lado pode chegar antes do outro   |
# | Stream × Static Join | Não, em geral | A tabela estática já está disponível |
# | Window aggregation   |           Sim | Precisa manter estado da janela      |
