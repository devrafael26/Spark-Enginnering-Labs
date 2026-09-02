########### Experimento A — Recomputação sem cache ##########

# A ideia aqui é criar um DataFrame com uma transformação que gere Shuffle e depois reutilizar
# esse mesmo resultado em duas Actions.

from pyspark.sql import functions as F

df = (
    spark.range(5_000_000)
    .withColumn("group_id", F.col("id") % 100)
)

df_resultado = (
    df
    .groupBy("group_id")
    .agg(
        F.count("*").alias("quantidade"),
        F.sum("id").alias("soma_id")
    )
)

# Action 1
df_resultado.count()

# Action 2
df_resultado.orderBy(F.desc("quantidade")).show(10)

df_resultado.explain(True)


# Observado no experimento A
# Duas Actions diferentes foram realizadas sobre o mesmo DataFrame sem persistência.
# Em ambas, o Query Text apresentou novamente o processamento desde o Range, 
# incluindo agregação parcial, Shuffle e agregação final. A Action show(), 
# por utilizar ordenação decrescente e retornar apenas as primeiras linhas, 
# apresentou adicionalmente o operador Top K.



############### Experimento B — Cache e Persist no Serverless ###############


# B1 - cache()
# No Databricks Serverless atual, a documentação diz que df.cache() não é suportado e que a chamada deve resultar em exceção.

df_cache = (
    spark.range(5_000_000)
    .withColumn("group_id", F.col("id") % 100)
    .groupBy("group_id")
    .count()
)

df_cache.cache()



# B2 — persist()

from pyspark import StorageLevel

df_persist = (
    spark.range(5_000_000)
    .withColumn("group_id", F.col("id") % 100)
    .groupBy("group_id")
    .count()
)

df_persist.persist(StorageLevel.MEMORY_AND_DISK)

# A Databricks inclusive orienta workloads migrados para Serverless a remover as chamadas df.cache() e df.persist().


### Observado no experimento B ###

# No Experimento B, foram testadas as operações `cache()` e `persist()` sobre o DataFrame.
# Em ambos os casos, o Databricks Serverless retornou erro informando que a operação de persistência não é suportada nesse tipo de compute.
# Dessa forma, embora `cache()` e `persist()` façam parte dos mecanismos de persistência do Apache Spark,
# essas operações não podem ser utilizadas neste ambiente Serverless.
# O resultado observado no laboratório confirma a limitação do ambiente e direciona o uso
# de outras estratégias de materialização, como tabelas Delta, quando houver necessidade de reutilizar resultados intermediários.




############# Experimento C — Materialização intermediária em Delta #############

from pyspark.sql import functions as F

df = (
    spark.range(5_000_000)
    .withColumn("group_id", F.col("id") % 100)
)

df_resultado = (
    df
    .groupBy("group_id")
    .agg(
        F.count("*").alias("quantidade"),
        F.sum("id").alias("soma_id")
    )
)
# Action 1
df_resultado.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lab13_cache_persist_resultado")


df_delta = spark.table("lab13_cache_persist_resultado")


# Action 2

df_delta.count()
# Observado: o count() executou, mas o Databricks mostrou Query profile is not available.
# Segundo a documentaçõa da Databricks, consultas atendidas pelo query cache não disponibilizam Query Profile.
# link doc: https://docs.databricks.com/aws/pt/sql/user/queries/query-profile


# Action 3

df_delta.orderBy(F.desc("quantidade")).show(5)

df_delta.explain(True)



### Observado no experimento C ###

# O processamento que anteriormente partia do Range e passava pelas etapas de agregação e
# Shuffle foi materializado em uma tabela Delta. Nas consultas posteriores, o Query Profile 
# e o Physical Plan passaram a apresentar a leitura por meio de Scan Table/PhotonScan,
# sem repetir o processamento utilizado para construir o resultado original.