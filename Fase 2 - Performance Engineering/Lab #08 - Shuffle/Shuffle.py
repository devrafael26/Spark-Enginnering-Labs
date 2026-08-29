from pyspark.sql import functions as F

df_orders = (
    spark.range(5_000_000)
    .withColumn("customer_id", (F.col("id") % 100_000).cast("long"))
    .withColumn("category_id", (F.col("id") % 20).cast("int"))
    .withColumn("amount", (F.rand() * 1000).cast("double"))
)

df_result5m = (
    df_orders
    .groupBy("category_id")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("amount").alias("total_amount")
    )
)

df_result5m.show()

df_result5m.explain(True)


# Alterando o range para 10 milhões de registros

from pyspark.sql import functions as F

df_orders10m = (
    spark.range(10_000_000)
    .withColumn("customer_id", (F.col("id") % 100_000).cast("long"))
    .withColumn("category_id", (F.col("id") % 20).cast("int"))
    .withColumn("amount", (F.rand() * 1000).cast("double"))
)

df_result10m = (
    df_orders10m
    .groupBy("category_id")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("amount").alias("total_amount")
    )
)

df_result10m.explain(True)