
#  %sql
#  CREATE VOLUME IF NOT EXISTS workspace.default.lab28_checkpoints;

from pyspark.sql import functions as F
import uuid

# 1. CONFIGURAÇÕES

catalog = "workspace"
schema = "default"

volume_path = "/Volumes/workspace/default/lab28_checkpoints"

# ID único para evitar reutilização acidental de checkpoint
run_id = uuid.uuid4().hex[:8]


# 2. FUNÇÃO AUXILIAR — ADICIONAR EVENTOS

def append_events(table_name, rows):

    df = (
        spark.createDataFrame(
            rows,
            "event_id INT, event_time STRING"
        )
        .select(
            "event_id",
            F.to_timestamp("event_time").alias("event_time")
        )
    )

    (
        df
        .coalesce(1)
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )


# 3. FUNÇÃO AUXILIAR — OBSERVAR PROGRESSO

def show_progress(query):

    print("\n=== STREAMING PROGRESS ===")

    for p in query.recentProgress:

        watermark = None

        if p.eventTime:
            watermark = p.eventTime.get("watermark")

        dropped = 0

        for op in p.stateOperators:
            dropped += op.numRowsDroppedByWatermark

        print(
            f"batchId={p.batchId} | "
            f"inputRows={p.numInputRows} | "
            f"watermark={watermark} | "
            f"droppedByWatermark={dropped}"
        )



############## Experimento A — Baseline: dados chegando em ordem ############## 


# Aqui não existe late data.

# PREPARAÇÃO

sourceA = f"{catalog}.{schema}.lab28_source_a_v2"
targetA = f"{catalog}.{schema}.lab28_target_a_v2"

checkpointA = f"{volume_path}/a_v2_{run_id}"


spark.sql(f"DROP TABLE IF EXISTS {sourceA}")
spark.sql(f"DROP TABLE IF EXISTS {targetA}")

# cria tabela fonte vazia
spark.sql(f"""
CREATE TABLE {sourceA} (
    event_id INT,
    event_time TIMESTAMP
)
USING DELTA
""")



# 7. INSERÇÃO DOS EVENTOS

append_events(
    sourceA,
    [
        (1, "2026-09-21 10:01:00"),
        (2, "2026-09-21 10:03:00"),
        (3, "2026-09-21 10:20:00")
    ]
)


# 8. VERIFICAÇÃO DA FONTE

display(
    spark.table(sourceA)
    .orderBy("event_time")
)


# 9. CRIAÇÃO DO STREAM


df_stream_a = (
    spark.readStream
    .table(sourceA)
)


# 10. WATERMARK + WINDOW + AGREGAÇÃO


df_agg_a = (
    df_stream_a
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        F.window("event_time", "5 minutes")
    )
    .agg(
        F.count("*").alias("qtd_eventos")
    )
)


# 11. EXECUÇÃO DO STREAM


queryA = (
    df_agg_a.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointA)
    .trigger(availableNow=True)
    .toTable(targetA)
)

queryA.awaitTermination()


# 12. OBSERVABILIDADE

show_progress(queryA)


# 13. RESULTADO FINAL

display(
    spark.table(targetA)
    .orderBy("window.start")
)



# Observação técnica do Exp. A:
# Com AvailableNow, o Spark processou os eventos disponíveis, depois executou um batch sem novas linhas, no qual o watermark avançou para 10:10; isso permitiu finalizar a janela 10:00–10:05 e gravar o resultado com 2 eventos.




##############  Experimento B — Late data ainda dentro da tolerância ############## 

# Objetivo: fazer o Spark primeiro enxergar 10:30, o que leva o watermark conceitualmente para 10:20, e depois inserir um evento com event_time = 10:24.
# Ele é atrasado em relação ao máximo já observado, mas ainda está à frente do watermark.


sourceB = f"{catalog}.{schema}.lab28_source_b"
targetB = f"{catalog}.{schema}.lab28_target_b"

checkpointB = f"{volume_path}/b_{run_id}"

spark.sql(f"DROP TABLE IF EXISTS {sourceB}")
spark.sql(f"DROP TABLE IF EXISTS {targetB}")

spark.sql(f"""
CREATE TABLE {sourceB} (
    event_id INT,
    event_time TIMESTAMP
)
USING DELTA
""")


append_events(
    sourceB,
    [
        (1, "2026-09-21 10:30:00")
    ]
)

display(
    spark.table(sourceB)
    .orderBy("event_time")
)



# Criando o stream

df_stream_b = (
    spark.readStream
    .table(sourceB)
)

df_agg_b = (
    df_stream_b
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        F.window("event_time", "5 minutes")
    )
    .agg(
        F.count("*").alias("qtd_eventos")
    )
)


queryB1 = (
    df_agg_b.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointB)
    .trigger(availableNow=True)
    .toTable(targetB)
)

queryB1.awaitTermination()

show_progress(queryB1)

# OBS. O inputRows=1 é o evento 10:30. O watermark ainda aparece como 1970 porque aquele era o watermark usado nesse batch.


# inserimos o late event:

append_events(
    sourceB,
    [
        (2, "2026-09-21 10:24:00")
    ]
)

display(
    spark.table(sourceB)
    .orderBy("event_time")
)


# Roda novamente o mesmo checkpoint
queryB2 = (
    df_agg_b.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointB)
    .trigger(availableNow=True)
    .toTable(targetB)
)

queryB2.awaitTermination()

show_progress(queryB2)

# OBS. o inputRows=1 é o evento de 10:24.


# Avançando o event time para fechar a janela 10:20–10:25

append_events(
    sourceB,
    [
        (3, "2026-09-21 10:40:00")
    ]
)


queryB3 = (
    df_agg_b.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointB)
    .trigger(availableNow=True)
    .toTable(targetB)
)

queryB3.awaitTermination()

show_progress(queryB3)

# OBS.: O inputRows=1 é o evento de 10:40.


display(
    spark.table(targetB)
    .orderBy("window.start")
)

# OBS.: qtd_eventos = 1 é o evento de 10:24.


# O evento 10:24 chegou depois de o Spark já ter observado 10:30, portanto era late data. Mesmo assim, como o watermark vigente era 10:20, ele ainda estava dentro da tolerância e foi processado normalmente.




##############  Experimento C — Late data anterior ao watermark ############## 


sourceC = f"{catalog}.{schema}.lab28_source_c"
targetC = f"{catalog}.{schema}.lab28_target_c"

checkpointC = f"{volume_path}/c_{run_id}"

spark.sql(f"DROP TABLE IF EXISTS {sourceC}")
spark.sql(f"DROP TABLE IF EXISTS {targetC}")

spark.sql(f"""
CREATE TABLE {sourceC} (
    event_id INT,
    event_time TIMESTAMP
)
USING DELTA
""")


append_events(
    sourceC,
    [
        (1, "2026-09-21 10:30:00")
    ]
)

display(
    spark.table(sourceC)
    .orderBy("event_time")
)


df_stream_c = (
    spark.readStream
    .table(sourceC)
)

df_agg_c = (
    df_stream_c
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        F.window("event_time", "5 minutes")
    )
    .agg(
        F.count("*").alias("qtd_eventos")
    )
)


queryC1 = (
    df_agg_c.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointC)
    .trigger(availableNow=True)
    .toTable(targetC)
)

queryC1.awaitTermination()

show_progress(queryC1)


append_events(
    sourceC,
    [
        (2, "2026-09-21 10:08:00")
    ]
)

display(
    spark.table(sourceC)
    .orderBy("event_time")
)


queryC2 = (
    df_agg_c.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointC)
    .trigger(availableNow=True)
    .toTable(targetC)
)

queryC2.awaitTermination()

show_progress(queryC2)


append_events(
    sourceC,
    [
        (3, "2026-09-21 10:40:00")
    ]
)


queryC3 = (
    df_agg_c.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpointC)
    .trigger(availableNow=True)
    .toTable(targetC)
)

queryC3.awaitTermination()

show_progress(queryC3)

# OBS.: O inputRows=1 é o evento de 10:40.


display(
    spark.table(targetC)
    .orderBy("window.start")
)


# Conclusão Experimento C
# o evento 10:08 foi processado pela query, porém numRowsDroppedByWatermark = 1 evidenciou seu descarte pelo operador stateful, pois o watermark vigente era 10:20. Posteriormente, o evento 10:40 foi aceito e fez o watermark avançar para 10:30; entretanto, como pertence à janela 10:40–10:45, ainda não finalizada pelo watermark, nenhum resultado foi emitido para o target no modo append.


# Diferença central entre B e C fica bem limpa:

# B:
# watermark ≈ 10:20
# late event = 10:24

# 10:24 > 10:20
# → atrasado, mas ainda tolerado


# C:
# watermark ≈ 10:20
# late event = 10:08

# 10:08 < 10:20
# → tarde demais
# → candidato a descarte
