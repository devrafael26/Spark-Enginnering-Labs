from pyspark.sql import functions as F
import time

# Dataset base
df = (
    spark.range(100_000_000)
    .withColumn("group_id", F.col("id") % 1000)
    .withColumn("value", F.col("id") * 0.01)
)

df.explain(True)



########## Experimento A — Poucas partições: repartition(2) ##########

df_a = df.repartition(2)

result_a = (
    df_a
    .groupBy("group_id")
    .agg(
        F.sum("value").alias("total_value")
    )
)

# Plano
result_a.explain(True)

# Benchmark
inicio = time.perf_counter()

rows_a = result_a.count()

tempo_a = time.perf_counter() - inicio

print("=== EXPERIMENTO A ===")
print(f"Partitions solicitadas: 2")
print(f"Linhas resultantes: {rows_a:,}")
print(f"Tempo: {tempo_a:.3f} s")




##########Experimento B — Particionamento intermediário: repartition(16) ##########

df_b = df.repartition(16)

result_b = (
    df_b
    .groupBy("group_id")
    .agg(
        F.sum("value").alias("total_value")
    )
)

# Plano
result_b.explain(True)

# Benchmark
inicio = time.perf_counter()

rows_b = result_b.count()

tempo_b = time.perf_counter() - inicio

print("=== EXPERIMENTO B ===")
print(f"Partitions solicitadas: 16")
print(f"Linhas resultantes: {rows_b:,}")
print(f"Tempo: {tempo_b:.3f} s")




########## Experimento C — Muitas partições: repartition(500) ##########

df_c = df.repartition(500)

result_c = (
    df_c
    .groupBy("group_id")
    .agg(
        F.sum("value").alias("total_value")
    )
)

# Plano
result_c.explain(True)

# Benchmark
inicio = time.perf_counter()

rows_c = result_c.count()

tempo_c = time.perf_counter() - inicio

print("=== EXPERIMENTO C ===")
print(f"Partitions solicitadas: 500")
print(f"Linhas resultantes: {rows_c:,}")
print(f"Tempo: {tempo_c:.3f} s")


########## Experimento D — Sem repartition() ##########

result_d = (
    df
    .groupBy("group_id")
    .agg(
        F.sum("value").alias("total_value")
    )
)

# Plano
result_d.explain(True)

# Benchmark
inicio = time.perf_counter()

rows_d = result_d.count()

tempo_d = time.perf_counter() - inicio

print("=== EXPERIMENTO D ===")
print("Partitions solicitadas: nenhuma intervenção manual")
print(f"Linhas resultantes: {rows_d:,}")
print(f"Tempo: {tempo_d:.3f} s")


########## Comparação final ##########

print("\n=== RESULTADO DO BENCHMARK ===\n")

print("Experimento A — repartition(2)")
print(f"Tempo: {tempo_a:.3f} s")

print("\nExperimento B — repartition(16)")
print(f"Tempo: {tempo_b:.3f} s")

print("\nExperimento C — repartition(500)")
print(f"Tempo: {tempo_c:.3f} s")

print("\nExperimento D — Automático")
print(f"Tempo: {tempo_d:.3f} s")


resultados = {
    "A — repartition(2)": tempo_a,
    "B — repartition(16)": tempo_b,
    "C — repartition(500)": tempo_c,
    "D — automático": tempo_d
}

mais_rapido = min(resultados, key=resultados.get)

print("\n=== MELHOR RESULTADO ===")
print(f"Estratégia mais rápida: {mais_rapido}")
print(f"Tempo: {resultados[mais_rapido]:.3f} s")