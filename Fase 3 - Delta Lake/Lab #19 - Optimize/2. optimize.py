
#### EXPERIMENTO ######
 
#  A — Criar tabela Delta
#          ↓
#  B — produzir vários arquivos pequenos
#          ↓
#  C — DESCRIBE DETAIL
#      numFiles / sizeInBytes
#          ↓
#  D — registrar quantidade de linhas
#          ↓
#  E — OPTIMIZE
#          ↓
#  F — observar filesAdded / filesRemoved
#          ↓
#  G — DESCRIBE DETAIL novamente
#          ↓
#  H — comparar numFiles
#          ↓
#  I — validar que COUNT(*) não mudou
#          ↓
#  J — executar OPTIMIZE novamente
#          ↓
#  observar idempotência





# Experimento A - Criar a tabela e provocar vários arquivos pequenos
# A ideia aqui é criar a tabela e depois fazer vários appends pequenos de propósito.



from pyspark.sql import functions as F

table_name = "workspace.default.lab19_optimize"

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

df_inicial = (
    spark.range(0, 100_000)
    .withColumn("category_id", (F.col("id") % 10))
    .withColumn("value", F.col("id") * 10)
)

(
    df_inicial.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)



for i in range(10):
    
    inicio = 100_000 + (i * 10_000)
    fim = inicio + 10_000

    df_append = (
        spark.range(inicio, fim)
        .withColumn("category_id", (F.col("id") % 10))
        .withColumn("value", F.col("id") * 10)
    )

    (
        df_append.write
        .format("delta")
        .mode("append")
        .saveAsTable(table_name)
    )



# Experimento B — Observar o estado da tabela antes do OPTIMIZE


spark.sql(f"""
DESCRIBE DETAIL {table_name}
""").display()


spark.sql(f"""
SELECT COUNT(*) AS total_registros
FROM {table_name}
""").display()




# Experimento C — Executar o OPTIMIZE


spark.sql(f"""
OPTIMIZE {table_name}
""").display()



# Experimento D — Comparar o estado depois do OPTIMIZE


spark.sql(f"""
DESCRIBE DETAIL {table_name}
""").display()



spark.sql(f"""
SELECT COUNT(*) AS total_registros
FROM {table_name}
""").display()



# Experimento E — Executar OPTIMIZE novamente


spark.sql(f"""
OPTIMIZE {table_name}
""").display()


spark.sql(f"""
DESCRIBE DETAIL {table_name}
""").display()