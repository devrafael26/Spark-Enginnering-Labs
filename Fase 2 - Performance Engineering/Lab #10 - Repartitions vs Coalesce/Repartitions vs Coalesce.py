df = spark.range(1_000_000)

df_repartition = df.repartition(16)
df_repartition.explain(True)


df_repartition_4 = df.repartition(4)
df_repartition_4.explain(True)


df_coalesce_4 = df.coalesce(4)
df_coalesce_4.explain(True)


df_coalesce_16 = df.coalesce(16)
df_coalesce_16.explain(True)