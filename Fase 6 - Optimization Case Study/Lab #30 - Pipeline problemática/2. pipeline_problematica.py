
from pyspark.sql import functions as F
from time import perf_counter

catalog = "workspace"
schema = "default"

orders_table = f"{catalog}.{schema}.lab30_orders"
customers_table = f"{catalog}.{schema}.lab30_customers"
products_table = f"{catalog}.{schema}.lab30_products"
result_table = f"{catalog}.{schema}.lab30_result_baseline"

spark.sql(f"DROP TABLE IF EXISTS {result_table}")
spark.sql(f"DROP TABLE IF EXISTS {orders_table}")
spark.sql(f"DROP TABLE IF EXISTS {customers_table}")
spark.sql(f"DROP TABLE IF EXISTS {products_table}")



# Criar customers aproximadamente com: 80% ACTIVE e 20% INACTIVE

customers = (
    spark.range(1_000_000)
    .select(
        F.col("id").alias("customer_id"),

        F.concat(
            F.lit("customer_"),
            F.col("id")
        ).alias("customer_name"),

        (
            F.col("id") % 5
        ).cast("int").alias("region_id"),

        F.when(
            (F.col("id") % 10) < 8,
            F.lit("ACTIVE")
        )
        .otherwise(
            F.lit("INACTIVE")
        )
        .alias("customer_status")
    )
)

customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(customers_table)



# Criar produtos com: 50 mil produtos, 100 categorias e preços variados.

products = (
    spark.range(50_000)
    .select(
        F.col("id").alias("product_id"),

        F.concat(
            F.lit("product_"),
            F.col("id")
        ).alias("product_name"),

        (
            F.col("id") % 100
        ).cast("int").alias("category_id"),

        (
            F.lit(10.0) +
            (F.col("id") % 500).cast("double")
        ).alias("base_price")
    )
)

products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(products_table)



# Criar orders com 30mi

orders_base = (
    spark.range(30_000_000)
    .withColumnRenamed("id", "order_id")
)



# Criar colunas

orders = (
    orders_base

    # skew proposital:
    # aproximadamente 40% das linhas ficam concentradas
    # em apenas 100 customer_ids
    .withColumn(
        "customer_id",
        F.when(
            (F.col("order_id") % 10) < 4,
            F.col("order_id") % 100
        )
        .otherwise(
            F.col("order_id") % 1_000_000
        )
        .cast("long")
    )

    .withColumn(
        "product_id",
        (F.col("order_id") % 50_000).cast("long")
    )

    .withColumn(
        "quantity",
        ((F.col("order_id") % 5) + 1).cast("int")
    )

    .withColumn(
        "unit_price",
        (
            F.lit(10.0) +
            (F.col("order_id") % 500).cast("double")
        )
    )

    .withColumn(
        "order_status",
        F.when(
            (F.col("order_id") % 10) < 7,
            F.lit("COMPLETED")
        )
        .when(
            (F.col("order_id") % 10) < 9,
            F.lit("PENDING")
        )
        .otherwise(
            F.lit("CANCELLED")
        )
    )

    .withColumn(
        "order_date",
        F.date_add(
            F.lit("2026-01-01").cast("date"),
            (F.col("order_id") % 365).cast("int")
        )
    )

    .withColumn(
        "amount",
        F.col("quantity") * F.col("unit_price")
    )
)



orders.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(orders_table)



print(
    "Orders:",
    spark.table(orders_table).count()
)

print(
    "Customers:",
    spark.table(customers_table).count()
)

print(
    "Products:",
    spark.table(products_table).count()
)



# Validar que realmente criamos skew

(
    spark.table(orders_table)
    .groupBy("customer_id")
    .count()
    .orderBy(F.desc("count"))
    .show(5, truncate=False)
)



customer_distribution = (
    spark.table(orders_table)
    .groupBy("customer_id")
    .count()
)

customer_distribution \
    .groupBy("count") \
    .count() \
    .orderBy(F.desc("count")) \
    .show(truncate=False)



## Pipeline problemática


orders_df = spark.table(orders_table)

customers_df = spark.table(customers_table)

products_df = spark.table(products_table)



# Queremos calcular: receita, quantidade vendida, número de pedidos e ticket médio por região e categoria para pedidos concluídos, clientes ativos e pedidos do segundo semestre.

pipeline_baseline = (
    orders_df

    # JOIN 1
    .join(
        customers_df,
        on="customer_id",
        how="inner"
    )

    # JOIN 2
    .join(
        products_df,
        on="product_id",
        how="inner"
    )

    # FILTROS PROPOSITALMENTE TARDIOS
    .filter(
        (F.col("order_status") == "COMPLETED") &
        (F.col("customer_status") == "ACTIVE") &
        (F.col("order_date") >= F.lit("2026-07-01"))
    )

    # AGREGAÇÃO APÓS OS JOINS
    .groupBy(
        "region_id",
        "category_id"
    )

    .agg(
        F.sum("amount").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
        F.count("*").alias("total_orders"),
        F.avg("amount").alias("avg_order_value")
    )
)


pipeline_baseline.explain(True)



## Action + benchmark baseline


start = perf_counter()

pipeline_baseline.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(result_table)

elapsed = perf_counter() - start

print(f"Tempo da pipeline baseline: {elapsed:.3f} s")


## Validar saída


result_df = spark.table(result_table)

result_count = result_df.count()

print(
    "Linhas da tabela final:",
    result_count
)


result_df.orderBy(
    "region_id",
    "category_id"
).show(10, truncate=False)



# Fluxo completo

# lab30_orders
# 30.000.000
#       │
#       │ customer_id
#       ▼
# lab30_customers
# 1.000.000
#       │
#       │
#       ├──────────────┐
#       │              │
#       ▼              │
#      JOIN            │
#       │              │
#       │ product_id   │
#       ▼              │
# lab30_products       │
# 50.000               │
#       │              │
#       ▼              │
#      JOIN            │
#       │
#       ▼
#  filtros
#  COMPLETED
#  ACTIVE
#  >= 2026-07-01
#       │
#       ▼
#  groupBy
#  region_id
#  category_id
#       │
#       ▼
#  SUM(amount)
#  SUM(quantity)
#  COUNT(*)
#  AVG(amount)
#       │
#       ▼
# lab30_result_baseline