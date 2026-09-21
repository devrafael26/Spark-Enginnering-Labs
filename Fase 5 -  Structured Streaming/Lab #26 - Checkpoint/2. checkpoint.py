
#  %sql
#  CREATE VOLUME IF NOT EXISTS workspace.default.lab26_checkpoint;


CATALOG = "workspace"
SCHEMA = "default"

VOLUME = "lab26_checkpoint"

INPUT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/input"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/checkpoint"
CHECKPOINT_NEW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/checkpoint_new"

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.lab26_checkpoint_result"
TARGET_TABLE_NEW = f"{CATALOG}.{SCHEMA}.lab26_checkpoint_result_new"



##############  Experimento A — Primeira execução com checkpoint ############## 


# A ideia aqui é criar 30 registros, divididos em 3 arquivos, e executar a primeira leitura streaming.


# Limpeza do ambiente
dbutils.fs.rm(INPUT_PATH, True)
dbutils.fs.rm(CHECKPOINT_PATH, True)
dbutils.fs.rm(CHECKPOINT_NEW_PATH, True)

spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_NEW}")


from pyspark.sql import functions as F

# criando um df lógico com 30 linhas
df_inicial = (
    spark.range(1, 31)
    .withColumn("grupo", F.lit("carga_inicial"))
)

display(df_inicial)



# Gravação física em 3 arquivos
(
    df_inicial
    .repartition(3)
    .write
    .mode("overwrite")
    .parquet(INPUT_PATH)
)



display(dbutils.fs.ls(INPUT_PATH))



# Criando leitura streaming no diretório de arquivos parquet

stream_df = (
    spark.readStream
    .schema(df_inicial.schema)
    .format("parquet")
    .load(INPUT_PATH)
)



# availableNow=True -> significa processe todos os registros atualmente disponíveis que ainda nao foram processados e quando terminar, encerre a query.

query = (
    stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query.awaitTermination()



spark.sql(f"""
SELECT *
FROM {TARGET_TABLE}
ORDER BY id
""").show(50, truncate=False)



spark.sql(f"""
SELECT COUNT(*) AS total_registros
FROM {TARGET_TABLE}
""").show()



############## Experimento B — Inspecionar o checkpoint ############## 

# Agora não alteramos nada.


display(dbutils.fs.ls(CHECKPOINT_PATH))


display(dbutils.fs.ls(f"{CHECKPOINT_PATH}/offsets"))

# offsets/ registra o progresso da source em cada micro-batch. É justamente uma das informações que permite ao Spark saber até onde aquela query já processou dados. A Databricks documenta que os offsets processados em cada micro-batch ficam registrados no checkpoint para que a query possa retomar do ponto correto.
# https://docs.databricks.com/aws/en/structured-streaming/checkpoints


display(dbutils.fs.ls(f"{CHECKPOINT_PATH}/commits"))

# # commits/ registra quais micro-batches foram efetivamente concluídos/commitados no sink. Então, de forma simplificada:
# offsets
#    ↓
# "até onde eu li"

# commits
#    ↓
# "o que eu concluí com sucesso"


dbutils.fs.head(f"{CHECKPOINT_PATH}/metadata")

# o ID único da streaming query. Esse UUID alfanumérico separado por hífens não é um número de batch nem um offset. É a identidade da query associada àquele checkpoint.



# Micro-batch 0
#       │
#       ├── offsets/0
#       │      informação de progresso daquele batch
#       │
#       └── commits/0
#              registro de que aquele batch foi concluído



# Obs. do Experimento B: não altera a streaming query. Ele apenas inspeciona o checkpoint produzido pelo Experimento A para observar como o Spark persistiu informações de progresso e identificação da query.



############# Experimento C — Novos dados + mesmo checkpoint #############


# Vamos adicionar mais 20 registros, sem apagar os 30 anteriores.


# Criar nova carga
df_novo = (
    spark.range(31, 51)
    .withColumn("grupo", F.lit("segunda_carga"))
)

display(df_novo)


# Adicionar novos arquivos ao mesmo diretório
(
    df_novo
    .repartition(2)
    .write
    .mode("append")
    .parquet(INPUT_PATH)
)


spark.read.parquet(INPUT_PATH).count()


# Reiniciar a stream usando o MESMO checkpoint
# Cria um novo objeto Streaming DataFrame apontando para a mesma fonte
# Mas esse objeto sozinho ainda não sabe do progresso anterior. Quem conecta essa nova execução ao histórico anterior é a linha depois: .option("checkpointLocation", CHECKPOINT_PATH)

stream_df_2 = (
    spark.readStream
    .schema(df_inicial.schema)
    .format("parquet")
    .load(INPUT_PATH)
)


query_2 = (
    stream_df_2.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query_2.awaitTermination()


spark.sql(f"""
SELECT COUNT(*) AS total_registros
FROM {TARGET_TABLE}
""").show()


spark.sql(f"""
SELECT
    grupo,
    COUNT(*) AS registros
FROM {TARGET_TABLE}
GROUP BY grupo
ORDER BY grupo
""").show()



# OBS do Experimento C: 
# Reiniciar a stream significa iniciar uma nova execução da query; não significa zerar o checkpoint. Usando o mesmo checkpoint, o Spark recupera o progresso persistido anteriormente.



############## Experimento D — Novo checkpoint ##############

# Agora mantemos exatamente os mesmos 50 registros no source, mas damos ao Spark um checkpoint vazio.
# Para não misturar os resultados, vamos usar também outra tabela destino.


# Criar novamente a leitura

stream_df_new_checkpoint = (
    spark.readStream
    .schema(df_inicial.schema)
    .format("parquet")
    .load(INPUT_PATH)
)


# Executar com outro checkpoint

query_new_checkpoint = (
    stream_df_new_checkpoint.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_NEW_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE_NEW)
)

query_new_checkpoint.awaitTermination()


spark.sql(f"""
SELECT COUNT(*) AS total_registros
FROM {TARGET_TABLE_NEW}
""").show()


# A Databricks documenta explicitamente que alterar para um novo checkpoint ou remover os arquivos de checkpoint faz a próxima execução começar sem aquele progresso anterior.
#  https://docs.databricks.com/aws/en/structured-streaming/checkpoints

# A documentação da Databricks descreve exatamente essa diferença: o checkpoint é o que fornece a identidade da stream e acompanha os registros já processados. Com AvailableNow, a query processa todos os registros que ainda estão não processados do ponto de vista daquele checkpoint.


# EXPERIMENTO C
# --------------------------------
# Source:          50 registros
# Checkpoint:      antigo
# Já registrados:  30
# Faltavam:         20

# Processados nessa execução: 20
# Target final: 50


# EXPERIMENTO D
# --------------------------------
# Source:          50 registros
# Checkpoint:      novo
# Já registrados:   0

# Processados nessa execução: 50
# Target nova: 50


# OBS Experimento D:
# O checkpoint não contém os dados da source. Ele contém o progresso da query. Ao usar um checkpoint novo, o Spark perde o conhecimento do que já havia sido processado e passa a tratar os dados ainda disponíveis na fonte como não processados para aquela nova query.




############## Experimento E — id vs runId ##############


# Aqui o objetivo é capturar as informações da query enquanto temos acesso ao objeto StreamingQuery.

# Adicionando mais 10 registros

df_terceira_carga = (
    spark.range(51, 61)
    .withColumn("grupo", F.lit("terceira_carga"))
)

(
    df_terceira_carga
    .write
    .mode("append")
    .parquet(INPUT_PATH)
)


# Iniciar nova execução

stream_df_3 = (
    spark.readStream
    .schema(df_inicial.schema)
    .format("parquet")
    .load(INPUT_PATH)
)


query_3 = (
    stream_df_3.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)


print("Query ID :", query_3.id)
print("Run ID   :", query_3.runId)

query_3.awaitTermination()


spark.sql(f"""
SELECT COUNT(*) AS total
FROM {TARGET_TABLE}
""").show()


# Adicionamos mais 10

df_quarta_carga = (
    spark.range(61, 71)
    .withColumn("grupo", F.lit("quarta_carga"))
)

(
    df_quarta_carga
    .write
    .mode("append")
    .parquet(INPUT_PATH)
)


stream_df_4 = (
    spark.readStream
    .schema(df_inicial.schema)
    .format("parquet")
    .load(INPUT_PATH)
)


query_4 = (
    stream_df_4.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

print("Query ID :", query_4.id)
print("Run ID   :", query_4.runId)

query_4.awaitTermination()


# query.id
#    ↓
# "quem é essa streaming query?"

# runId
#    ↓
# "qual execução dela é essa?"


# OBS Experimento E:
# Reutilizar o mesmo checkpoint preserva a identidade lógica da streaming query (query.id), enquanto cada nova execução possui um identificador próprio (runId).


# Fluxo completo do laboratório

# EXPERIMENTO A
# 30 registros
#    ↓
# Streaming
#    ↓
# Checkpoint
#    ↓
# Target = 30


# EXPERIMENTO B
# Checkpoint
#    ↓
# inspeção física
# offsets / commits / metadata / ...


# EXPERIMENTO C
# +20 registros
#    ↓
# MESMO checkpoint
#    ↓
# somente dados ainda não processados
#    ↓
# Target = 50


# EXPERIMENTO D
# mesmos 50 registros
#    ↓
# NOVO checkpoint
#    ↓
# nova identidade/progresso
#    ↓
# nova Target = 50


# EXPERIMENTO E
# mesmo checkpoint
#    ↓
# restart
#    ↓
# Query ID permanece relacionado à mesma query
# Run ID identifica a nova execução



# Arquitetura do Lab


# UNITY CATALOG
# │
# ├── workspace
# │   └── default
# │       │
# │       ├── Volume: lab26_checkpoint
# │       │   │
# │       │   ├── input/
# │       │   │   └── arquivos Parquet da source
# │       │   │
# │       │   ├── checkpoint/
# │       │   │   ├── metadata
# │       │   │   ├── offsets/
# │       │   │   ├── commits/
# │       │   │   └── sources/
# │       │   │
# │       │   └── checkpoint_new/
# │       │
# │       ├── Tabela: lab26_checkpoint_result
# │       │   └── tabela Delta gerenciada
# │       │
# │       └── Tabela: lab26_checkpoint_result_new
# │           └── outra tabela Delta gerenciada