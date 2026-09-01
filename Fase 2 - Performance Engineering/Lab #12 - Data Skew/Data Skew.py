
# Experimento A → dados equilibrados + groupBy

# Aqui vamos criar 5 milhões de registros distribuídos entre 100 chaves de maneira praticamente uniforme.

# Experimento A — Distribuição equilibrada

from pyspark.sql import functions as F

df_balanced = (
    spark.range(5_000_000)
    .withColumn("key_id", F.col("id") % 100)
)

df_balanced.groupBy("key_id").count().orderBy("key_id").show(5)

df_balanced_agg = (
    df_balanced
    .groupBy("key_id")
    .agg(
        F.count("*").alias("total")
    )
)

df_balanced_agg.show()

df_balanced_agg.explain(True)



# Experimento B — Data Skew proposital

# Agora usamos praticamente o mesmo DataFrame, mas manipulamos a distribuição da chave.
# Vamos fazer 80% dos registros receberem key_id = 0.
# Os outros 20% continuam distribuídos entre as demais chaves.


df_skew = (
    spark.range(5_000_000)
    .withColumn(
        "key_id",
        F.when(
            F.col("id") < 4_000_000,
            F.lit(0)
        ).otherwise(
            (F.col("id") % 99) + 1
        )
    )
)


df_skew.groupBy("key_id").count().orderBy(F.desc("count")).show(100)

# Agora executamos a mesma transformação do experimento anterior:

df_skew_agg = (
    df_skew
    .groupBy("key_id")
    .agg(
        F.count("*").alias("total")
    )
)

df_skew_agg.show(5)

df_skew_agg.explain(True)


# Experimento C — Skew em Join

# Agora vamos aproveitar o df_skew e criar outro DataFrame para fazer o join.
# Mas aqui tem uma preocupação: não queremos que o Spark simplesmente transforme isso em Broadcast Hash Join, porque aí estaríamos repetindo o Lab 11 e não conseguiríamos estudar direito um Shuffle Join.
# Vamos criar os dois lados com volume relevante.

df_left = (
    spark.range(5_000_000)
    .withColumn(
        "key_id",
        F.when(
            F.col("id") < 4_000_000,
            F.lit(0)
        ).otherwise(
            (F.col("id") % 99) + 1
        )
    )
    .withColumnRenamed("id", "left_id")
)

df_keys = (
    spark.range(100)
    .withColumnRenamed("id", "key_id")
    .withColumn(
        "description",
        F.concat(
            F.lit("key_"),
            F.col("key_id")
        )
    )
)

df_skew_join = (
    df_left
    .hint("merge")
    .join(
        df_keys.hint("merge"),
        "key_id",
        "inner"
    )
)

df_skew_join.count()

df_skew_join.explain(True)