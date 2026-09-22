

#### Experimento A — Event Time + Window, sem Watermark

# O event_time realmente determina em qual janela o registro será agregado?


#  %sql
#  CREATE VOLUME IF NOT EXISTS workspace.default.lab27_checkpoints;



from pyspark.sql import functions as F

spark.sql("DROP TABLE IF EXISTS lab27_source_a")

dados_a = [
    (1, "2026-09-18 10:01:00"),
    (2, "2026-09-18 10:02:00"),
    (3, "2026-09-18 10:04:00"),
    (4, "2026-09-18 10:06:00"),
    (5, "2026-09-18 10:08:00"),
    (6, "2026-09-18 10:11:00"),
]

df_a = (
    spark.createDataFrame(
        dados_a,
        ["event_id", "event_time"]
    )
    .withColumn(
        "event_time",
        F.to_timestamp("event_time")
    )
)

df_a.write.mode("overwrite").saveAsTable("lab27_source_a")



spark.table("lab27_source_a").orderBy("event_time").show(truncate=False)



# Ler como Streaming
stream_a = (
    spark.readStream
        .table("lab27_source_a")
)



# criamos as janelas de 5 minutos

window_a = (
    stream_a
        .groupBy(
            F.window("event_time", "5 minutes")
        )
        .count()
)



query_a = (
    window_a.writeStream
        .format("memory")
        .queryName("lab27_result_a")
        .outputMode("complete")
        .option(
            "checkpointLocation",
            "/Volumes/workspace/default/lab27_checkpoints/lab27_exp_a"
        )
        .trigger(availableNow=True)
        .start()
)

query_a.awaitTermination()



spark.sql("""
SELECT
    window.start AS window_start,
    window.end   AS window_end,
    count
FROM lab27_result_a
ORDER BY window_start
""").show(truncate=False)



####### Experimento B — Window + Watermark


spark.sql("DROP TABLE IF EXISTS lab27_source_b")


### LOTE 1 ###

batch_1 = [
    (1, "2026-09-18 10:01:00"),
    (2, "2026-09-18 10:03:00"),
    (3, "2026-09-18 10:04:00"),
]

df_b1 = (
    spark.createDataFrame(
        batch_1,
        ["event_id", "event_time"]
    )
    .withColumn(
        "event_time",
        F.to_timestamp("event_time")
    )
)

df_b1.coalesce(1).write.mode("overwrite").saveAsTable("lab27_source_b")



### LOTE 2 ###

batch_2 = [
    (4, "2026-09-18 10:06:00"),
    (5, "2026-09-18 10:08:00"),
    (6, "2026-09-18 10:09:00"),
]

df_b2 = (
    spark.createDataFrame(
        batch_2,
        ["event_id", "event_time"]
    )
    .withColumn(
        "event_time",
        F.to_timestamp("event_time")
    )
)

df_b2.coalesce(1).write.mode("append").saveAsTable("lab27_source_b")



### LOTE 3 ###

batch_3 = [
    (7, "2026-09-18 10:12:00"),
    (8, "2026-09-18 10:14:00"),
]

df_b3 = (
    spark.createDataFrame(
        batch_3,
        ["event_id", "event_time"]
    )
    .withColumn(
        "event_time",
        F.to_timestamp("event_time")
    )
)

df_b3.coalesce(1).write.mode("append").saveAsTable("lab27_source_b")



### LOTE 4 ###  

batch_4 = [
    (9,  "2026-09-18 10:16:00"),
    (10, "2026-09-18 10:18:00"),
]

df_b4 = (
    spark.createDataFrame(
        batch_4,
        ["event_id", "event_time"]
    )
    .withColumn(
        "event_time",
        F.to_timestamp("event_time")
    )
)

df_b4.coalesce(1).write.mode("append").saveAsTable("lab27_source_b")



# Streaming com Watermark
# stream_b = DE ONDE vêm os dados
# .option("maxFilesPerTrigger", 1) -> para cada micro-batch, considere no máximo 1 novo arquivo físico da fonte Delta

stream_b = (
    spark.readStream
        .option("maxFilesPerTrigger", 1)
        .table("lab27_source_b")
)



# watermark_b = O QUE fazer com os dados

watermark_b = (
    stream_b
        .withWatermark(
            "event_time",
            "10 minutes"
        )
        .groupBy(
            F.window(
                "event_time",
                "5 minutes"
            )
        )
        .count()
)



# query_b = EXECUTAR isso
# Esse .outputMode("update") faz sentido aqui porque estamos trabalhando com agregação stateful + watermark.
# outputMode("update") a Databricks documenta três modos principais para agregações stateful: append, update e complete.
# AvailableNow quer dizer -> quando essa streaming query começa, processa todo o backlog atualmente disponível que ainda não foi processado por essa query e depois encerra.

query_b = (
    watermark_b.writeStream
        .format("memory")
        .queryName("lab27_result_b")
        .outputMode("update")  
        .option(
            "checkpointLocation",
            "/Volumes/workspace/default/lab27_checkpoints/lab27_exp_b"
        )
        .trigger(availableNow=True)
        .start()
)

query_a.awaitTermination()


#  Pondo stream_b, watermark_b e query_b em um bloco só.

# query_b = (
#     spark.readStream
#         .option("maxFilesPerTrigger", 1)
#         .table("lab27_source_b")
#         .withWatermark("event_time", "10 minutes")
#         .groupBy(
#             F.window("event_time", "5 minutes")
#         )
#         .count()
#         .writeStream
#         .format("memory")
#         .queryName("lab27_result_b")
#         .outputMode("update")
#         .option(
#             "checkpointLocation",
#             "/Volumes/workspace/default/lab27_checkpoints/lab27_exp_b"
#         )
#         .trigger(availableNow=True)
#         .start()
# )


######################## Mapa mental do fluxo se fosse como a query_b acima ########################

# BATCH 1
# BATCH 2
# BATCH 3
# BATCH 4
#       ↓
# Delta table
# lab27_source_b
#       ↓
# readStream
#       ↓
# maxFilesPerTrigger = 1
#       ↓
# withWatermark(event_time, 10 min)
#       ↓
# window(event_time, 5 min)
#       ↓
# groupBy
#       ↓
# count
#       ↓
# writeStream
#       ↓
# memory sink
# lab27_result_b
#       ↓
# AvailableNow
#       ↓
# start()
#       ↓
# SELECT
#       ↓
# visualização


######################### Mapas mentais do fluxo #########################

# query_b
# =
# EXECUTA o processamento
# +
# define ONDE sai o resultado
# +
# COMO a query roda
# +
# ONDE fica o checkpoint

# ===============================================================

# Fonte Delta

# arquivo 1 ─┐
# arquivo 2  │
# arquivo 3  │  tudo disponível
# arquivo 4 ─┘
#       ↓
# AvailableNow:
# "quero terminar todo esse backlog"
#       ↓
# maxFilesPerTrigger = 1:
# "mas no máximo 1 arquivo por micro-batch"
#       ↓
# micro-batch 0 → até um arquivo 1
# micro-batch 1 → até um arquivo 1
# micro-batch 2 → até um arquivo 1
# micro-batch 3 → até um arquivo 1
#       ↓
# acabou o backlog
#       ↓
# query encerra

# ======================================================

# BATCH 1
# BATCH 2
# BATCH 3
# BATCH 4
#       ↓
# Delta table
# lab27_source_b
#       ↓
# readStream
#       ↓
# maxFilesPerTrigger = 1
#       ↓
# stream_b
#       ↓
# withWatermark(event_time, 10 min)
#       ↓
# window(event_time, 5 min)
#       ↓
# groupBy
#       ↓
# count
#       ↓
# watermark_b
#       ↓
# writeStream.start()
#       ↓
# memory sink
# lab27_result_b
#       ↓
# SELECT
#       ↓
# visualização



spark.sql("""
SELECT
    window.start AS window_start,
    window.end   AS window_end,
    count
FROM lab27_result_b
ORDER BY window_start
""").show(truncate=False)



###### Experimento C — Observar o Watermark avançando


# Aqui nós não precisamos criar outra query.
# Vamos aproveitar a query_b, porque ela processou os arquivos Delta em micro-batches.
# A opção `.option("maxFilesPerTrigger", 1)` foi colocada justamente para incentivar o AvailableNow a dividir os dados disponíveis em mais de um micro-batch. AvailableNow pode processar os dados disponíveis incrementalmente antes de encerrar.



for p in query_b.recentProgress:

    event_time = p.get("eventTime", {})

    print("=" * 60)

    print("Batch ID:")
    print(p.get("batchId"))

    print("\nInput Rows:")
    print(p.get("numInputRows"))

    print("\nEvent Time mínimo:")
    print(event_time.get("min"))

    print("\nEvent Time máximo:")
    print(event_time.get("max"))

    print("\nEvent Time médio:")
    print(event_time.get("avg"))

    print("\nWatermark:")
    print(event_time.get("watermark"))



for p in query_b.recentProgress:

    print("=" * 60)
    print("Batch ID:", p.get("batchId"))

    state_operators = p.get("stateOperators", [])

    if not state_operators:
        print("Nenhum state operator reportado.")
        continue

    for i, state in enumerate(state_operators):

        print(f"\nState Operator {i}")

        print(
            "numRowsTotal:",
            state.get("numRowsTotal")
        )

        print(
            "numRowsUpdated:",
            state.get("numRowsUpdated")
        )

        print(
            "numRowsRemoved:",
            state.get("numRowsRemoved")
        )

        print(
            "memoryUsedBytes:",
            state.get("memoryUsedBytes")
        )



# EXPERIMENTO A
# Event time define o quê?
#         ↓
# A janela à qual o evento pertence.


# EXPERIMENTO B
# Watermark acrescenta o quê?
#         ↓
# Um limite temporal sobre o estado
# mantido pela operação stateful.


# EXPERIMENTO C
# Consigo enxergar isso acontecendo?
#         ↓
# eventTime.max
# watermark
# stateOperators
# numRowsTotal
# numRowsRemoved