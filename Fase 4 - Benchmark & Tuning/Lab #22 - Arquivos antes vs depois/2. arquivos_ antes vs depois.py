
######### Experimento A — criação do cenário “antes” #########


from pyspark.sql import functions as F

# Criar uma tabela com vários arquivos

table_name = "workspace.default.lab22_files"

spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# 1. Criar os dados

df = (
    spark.range(30_000_000)
    .withColumn(
        "customer_id",
        (F.col("id") % 1_000_000).cast("long")
    )
    .withColumn(
        "category_id",
        (F.col("id") % 100).cast("int")
    )
    .withColumn(
        "amount",
        (F.col("id") % 1000).cast("double")
    )
)

print("Quantidade de linhas:")
print(df.count())

# 2. Aumentar propositalmente o paralelismo da escrita

df_files = df.repartition(100)

# 3. Gravar a tabela Delta


(
    df_files
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)

print("Tabela criada:")
print(table_name)



# Inspecionar o estado inicial da tabela

detail_before = spark.sql("""
    DESCRIBE DETAIL workspace.default.lab22_files
""")

display(
    detail_before.select(
        "format",
        "numFiles",
        "sizeInBytes"
    )
)



# Métricas do estado inicial

row = detail_before.select(
    "numFiles",
    "sizeInBytes"
).first()

num_files_before = row["numFiles"]
size_bytes_before = row["sizeInBytes"]

print(f"Número de arquivos: {num_files_before}")
print(f"Tamanho total: {size_bytes_before:,} bytes")
print(f"Tamanho total: {size_bytes_before / (1024**2):.2f} MB")

if num_files_before > 0:
    avg_file_mb = (
        size_bytes_before / num_files_before / (1024**2)
    )

    print(f"Tamanho médio aproximado por arquivo: {avg_file_mb:.2f} MB")




######### Experimento B — benchmark ANTES #########


import time

table_name = "workspace.default.lab22_files"

# Benchmark ANTES do OPTIMIZE

query = f"""
SELECT
    category_id,
    SUM(amount) AS total_amount,
    COUNT(*) AS total_rows
FROM {table_name}
GROUP BY category_id
"""

for rodada in range(1, 4):

    inicio = time.perf_counter()

    result = spark.sql(query).collect()

    fim = time.perf_counter()

    tempo = fim - inicio

    print(f"Rodada {rodada}: {tempo:.3f} s")



result_before = spark.sql("""
SELECT
    category_id,
    SUM(amount) AS total_amount,
    COUNT(*) AS total_rows
FROM workspace.default.lab22_files
GROUP BY category_id
""")

display(result_before)




######### Experimento C — OPTIMIZE #########


optimize_result = spark.sql("""
OPTIMIZE workspace.default.lab22_files
""")

display(optimize_result)


detail_after = spark.sql("""
DESCRIBE DETAIL workspace.default.lab22_files
""")

display(
    detail_after.select(
        "numFiles",
        "sizeInBytes"
    )
)



row = detail_after.select(
    "numFiles",
    "sizeInBytes"
).first()

num_files_after = row["numFiles"]
size_bytes_after = row["sizeInBytes"]

print(f"Número de arquivos: {num_files_after}")
print(f"Tamanho total: {size_bytes_after / (1024**2):.2f} MB")

if num_files_after > 0:
    avg_file_mb = (
        size_bytes_after / num_files_after / (1024**2)
    )

    print(f"Tamanho médio aproximado por arquivo: {avg_file_mb:.2f} MB")



# Quantidade de linhas depois do OPTIMIZE

spark.sql("""
SELECT COUNT(*) AS total_rows
FROM workspace.default.lab22_files
""").show()


spark.sql("""
DESCRIBE DETAIL workspace.default.lab22_files
""").select(
    "numFiles",
    "sizeInBytes"
).show(truncate=False)



# Query Profile DEPOIS do OPTIMIZE

result_after = spark.sql("""
SELECT
    category_id,
    SUM(amount) AS total_amount,
    COUNT(*) AS total_rows
FROM workspace.default.lab22_files
GROUP BY category_id
""")

display(result_after)



import time

query_after = """
SELECT
    category_id,
    SUM(amount) AS total_amount,
    COUNT(*) AS total_rows
FROM workspace.default.lab22_files
GROUP BY category_id
"""

for rodada in range(1, 4):

    inicio = time.perf_counter()

    spark.sql(query_after).collect()

    fim = time.perf_counter()

    print(f"Rodada {rodada}: {fim - inicio:.3f} s")