######## criação do df ########

from pyspark.sql import functions as F

df_lab20 = (
    spark.range(30_000_000)
    .withColumn(
        "customer_id",
        F.pmod(F.hash(F.col("id")), F.lit(100_000))
    )
    .withColumn(
        "product_id",
        F.pmod(F.hash(F.col("id") * 13), F.lit(10_000))
    )
    .withColumn(
        "order_date",
        F.date_add(
            F.lit("2025-01-01"),
            F.pmod(F.hash(F.col("id") * 7), F.lit(365))
        )
    )
    .withColumn(
        "amount",
        (F.pmod(F.hash(F.col("id") * 17), F.lit(10_000)) / 100).cast("double")
    )
    .select(
        F.col("id").alias("order_id"),
        "customer_id",
        "product_id",
        "order_date",
        "amount"
    )
)

df_lab20.count()



######## Experimento A — Tabela base ########


spark.sql("""
DROP TABLE IF EXISTS workspace.default.lab20_base
""")

(
    df_lab20
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.lab20_base")
)


spark.sql("""
SELECT COUNT(*) AS total_rows
FROM workspace.default.lab20_base
""").show()


spark.sql("""
SELECT *
FROM workspace.default.lab20_base
WHERE customer_id = 12345
""").show()



######## Experimento B — ZORDER ########



spark.sql("""
DROP TABLE IF EXISTS workspace.default.lab20_zorder
""")

(
    df_lab20
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.lab20_zorder")
)


# Antes do ZORDER

spark.sql("""
DESCRIBE DETAIL workspace.default.lab20_zorder
""").show(truncate=False)


spark.sql("""
OPTIMIZE workspace.default.lab20_zorder
ZORDER BY (customer_id)
""").show(truncate=False)


spark.sql("""
SELECT *
FROM workspace.default.lab20_zorder
WHERE customer_id = 12345
""").show()




######## Experimento C — Liquid Clustering ########



spark.sql("""
DROP TABLE IF EXISTS workspace.default.lab20_liquid
""")

(
    df_lab20
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.lab20_liquid")
)


spark.sql("""
ALTER TABLE workspace.default.lab20_liquid
CLUSTER BY (customer_id)
""")


spark.sql("""
DESCRIBE DETAIL workspace.default.lab20_liquid
""").show(truncate=False)


spark.sql("""
OPTIMIZE workspace.default.lab20_liquid
""").show(truncate=False)


spark.sql("""
SELECT *
FROM workspace.default.lab20_liquid
WHERE customer_id = 12345
""").show()




######## Experimento D — Alterando a clustering key ########


spark.sql("""
DESCRIBE DETAIL workspace.default.lab20_liquid
""").show(truncate=False)


spark.sql("""
ALTER TABLE workspace.default.lab20_liquid
CLUSTER BY (product_id)
""")


spark.sql("""
DESCRIBE DETAIL workspace.default.lab20_liquid
""").show(truncate=False)


spark.sql("""
OPTIMIZE workspace.default.lab20_liquid
""").show(truncate=False)


spark.sql("""
SELECT *
FROM workspace.default.lab20_liquid
WHERE product_id = 1234
""").show()




######## Experimento E — Forçando o histórico para a nova chave ########


spark.sql("""
OPTIMIZE workspace.default.lab20_liquid FULL
""").show(truncate=False)


spark.sql("""
DESCRIBE DETAIL workspace.default.lab20_liquid
""").show(truncate=False)


spark.sql("""
SELECT *
FROM workspace.default.lab20_liquid
WHERE product_id = 1234
""").show()