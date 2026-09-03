# EXPERIMENTO A

from pyspark.sql import functions as F

df = (
    spark.range(5_000_000)
    .withColumn("key_id", F.col("id") % 100)
)

df_agg = (
    df
    .groupBy("key_id")
    .count()
)


df_agg.explain(True)

df_agg.collect()

df_agg.explain(True)




# EXPERIMENTO B

from pyspark.sql import functions as F

# Lado grande do join
df_sales = (
    spark.range(10_000_000)
    .withColumnRenamed("id", "sale_id")
    .withColumn(
        "item_id",
        F.col("sale_id") % 5_000_000
    )
)

# Outro lado inicialmente grande
df_items = (
    spark.range(5_000_000)
    .withColumnRenamed("id", "item_id")
    .withColumn(
        "price_group",
        F.col("item_id") % 1000
    )
)

# O filtro deixa esse lado muito pequeno:
# aproximadamente 5.000 registros dos 5 milhões
df_items_filtered = (
    df_items
    .filter(F.col("price_group") == 0)
)

# Join SEM hint de broadcast e SEM hint MERGE
df_join = (
    df_sales
    .join(
        df_items_filtered,
        on="item_id",
        how="inner"
    )
)

# 1. Plano antes da execução
df_join.explain(True)

resultado = df_join.count()
print(resultado)