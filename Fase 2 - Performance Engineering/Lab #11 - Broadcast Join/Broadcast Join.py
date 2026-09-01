# EXPERIMENTO A
from pyspark.sql import functions as F

# DataFrame grande: 5 milhões de pedidos
df_orders = (
    spark.range(5_000_000)
    .withColumnRenamed("id", "order_id")
    .withColumn(
        "product_id",
        (F.col("order_id") % 1_000).cast("long")
    )
    .withColumn(
        "quantity",
        ((F.col("order_id") % 5) + 1).cast("int")
    )
)

# DataFrame pequeno: 1.000 produtos
df_products = (
    spark.range(1_000)
    .withColumnRenamed("id", "product_id")
    .withColumn(
        "product_name",
        F.concat(
            F.lit("Product_"),
            F.col("product_id")
        )
    )
)

df_orders.show(5)
df_products.show(5)

# O % 1_000 foi colocado justamente para garantir que todos os pedidos tenham um product_id correspondente na nossa tabela de produtos.


df_join = df_orders.join(
    df_products,
    on="product_id",
    how="inner"
)

df_join.explain(True)


# EXPERIMENTO B

from pyspark.sql import functions as F

df_join_broadcast = df_orders.join(
    F.broadcast(df_products),
    on="product_id",
    how="inner"
)

df_join_broadcast.explain(True)