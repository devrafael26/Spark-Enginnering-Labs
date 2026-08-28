#CRIANDO DADOS

df = spark.range(1_000_000)


#CRIANDO O GRUPO
# Transfromação

df_grouped = (
    df
    .withColumn("group_id", df.id % 10)
)



# Transformação

df_result = (
    df_grouped
    .groupBy("group_id")
    .count()
)


# Ação

df_result.show()
