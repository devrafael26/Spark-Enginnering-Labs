df = spark.range(1_000_000)

df_grouped = (
    df
    .withColumn("group_id", df.id % 10)
    .groupBy("group_id")
    .count()
)

df_grouped.show()

df_grouped.explain(True)
