############# Experimento A #############

from pyspark.sql import functions as F

table_name = "lab15_transaction_log"

# Limpeza para permitir repetir o laboratório
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Dados iniciais
df = (
    spark.range(100_000)
    .withColumn("customer_id", F.col("id") % 1000)
    .withColumn("amount", (F.col("id") % 500 + 10).cast("double"))
    .withColumn("status", F.lit("ACTIVE"))
)

# Criação da tabela Delta
(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)


spark.sql(f"""
    INSERT INTO {table_name}
    VALUES
        (100000, 1000, 250.0, 'ACTIVE'),
        (100001, 1001, 300.0, 'ACTIVE'),
        (100002, 1002, 450.0, 'ACTIVE')
""")


spark.sql(f"""
    UPDATE {table_name}
    SET status = 'INACTIVE'
    WHERE customer_id < 10
""")


spark.sql(f"""
    DELETE FROM {table_name}
    WHERE id < 100
""")


display(
    spark.sql(f"""
        DESCRIBE HISTORY {table_name}
    """)
)



############# Experimento B - Parte 1 #############


# localizar a tabela e tentar observar o _delta_log

display(
    spark.sql("""
        DESCRIBE DETAIL lab15_transaction_log
    """)
)


detail = spark.sql("""
    DESCRIBE DETAIL lab15_transaction_log
""")

location = detail.select("location").first()["location"]

print(location)


display(
    dbutils.fs.ls(f"{location}/_delta_log")
)




############# Experimento B - Parte 2 #############


#  %sql
#  CREATE VOLUME IF NOT EXISTS workspace.default.lab15_volume;

spark.sql("""
    CREATE VOLUME IF NOT EXISTS workspace.default.lab15_volume
""")


path = "/Volumes/workspace/default/lab15_volume/transaction_log"


from pyspark.sql import functions as F

df = (
    spark.range(100_000)
    .withColumn("customer_id", F.col("id") % 1000)
    .withColumn("amount", (F.col("id") % 500 + 10).cast("double"))
    .withColumn("status", F.lit("ACTIVE"))
)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(path)
)


spark.sql(f"""
    INSERT INTO delta.`{path}`
    VALUES
        (100000, 1000, 250.0, 'ACTIVE'),
        (100001, 1001, 300.0, 'ACTIVE'),
        (100002, 1002, 450.0, 'ACTIVE')
""")


spark.sql(f"""
    UPDATE delta.`{path}`
    SET status = 'INACTIVE'
    WHERE customer_id < 10
""")


spark.sql(f"""
    DELETE FROM delta.`{path}`
    WHERE id < 100
""")


display(
    spark.sql(f"""
        DESCRIBE HISTORY delta.`{path}`
    """)
)


display(
    dbutils.fs.ls(f"{path}/_delta_log")
)