
spark.sql("SHOW TABLES IN workspace.default").show(50,truncate=False)


# Validação das tabelas herdadas do Lab 32


baseline_table = "workspace.default.lab32_orders_large"
optimized_table = "workspace.default.lab32_orders_large_clustered"

print("BASELINE")
spark.sql(f"""
SELECT COUNT(*) AS total_rows
FROM {baseline_table}
""").show()

print("OTIMIZADA")
spark.sql(f"""
SELECT COUNT(*) AS total_rows
FROM {optimized_table}
""").show()

spark.sql(f"DESCRIBE DETAIL {baseline_table}").show(truncate=False)
spark.sql(f"DESCRIBE DETAIL {optimized_table}").show(truncate=False)




########### Experimento A — Baseline com uma consulta filtrando order_date ###########



from time import perf_counter

baseline_table = "workspace.default.lab32_orders_large"

start = perf_counter()

baseline_result = spark.sql(f"""
SELECT *
FROM {baseline_table}
WHERE order_date >= DATE '2026-08-01'
""")

baseline_count = baseline_result.count()

elapsed = perf_counter() - start

print(f"Linhas retornadas: {baseline_count:,}")
print(f"Tempo: {elapsed:.3f} s")



# Rows read:       64.444.438
# Files read:      32
# Files pruned:    0
# Bytes read:      28,04 MB
# Bytes pruned:    0
# Scan time:       678 ms
# Query wall-clock: 610 ms

# BASELINE
# 32 arquivos existentes
# ↓
# 32 arquivos lidos
# ↓
# 0 arquivos ignorados
# ↓
# 28,04 MB lidos
# ↓
# 64,44M linhas



optimized_table = "workspace.default.lab32_orders_large_clustered"

start = perf_counter()

optimized_result = spark.sql(f"""
SELECT *
FROM {optimized_table}
WHERE order_date >= DATE '2026-08-01'
""")

optimized_count = optimized_result.count()

elapsed = perf_counter() - start

print(f"Linhas retornadas: {optimized_count:,}")
print(f"Tempo: {elapsed:.3f} s")



# Rows read:       64.444.438
# Files read:      2
# Files pruned:    6
# Bytes read:      3,35 MB
# Bytes pruned:    180,92 MB
# Scan time:       156 ms
# Query wall-clock: 595 ms

# CLUSTERED
# 8 arquivos existentes
# ↓
# 6 arquivos descartados
# ↓
# somente 2 arquivos lidos
# ↓
# 3,35 MB lidos
# ↓
# 64,44M linhas




## Benchmark



from time import perf_counter
import statistics

baseline_table = "workspace.default.lab32_orders_large"
optimized_table = "workspace.default.lab32_orders_large_clustered"

query_baseline = f"""
SELECT *
FROM {baseline_table}
WHERE order_date >= DATE '2026-08-01'
"""

query_optimized = f"""
SELECT *
FROM {optimized_table}
WHERE order_date >= DATE '2026-08-01'
"""


def executar_benchmark(query):
    inicio = perf_counter()

    total = spark.sql(query).count()

    tempo = perf_counter() - inicio

    return total, tempo


baseline_times = []
optimized_times = []

print("=== BENCHMARK LAB 33 ===\n")

for rodada in range(1, 8):

    # Alterna a ordem para reduzir o efeito
    # de sempre executar a mesma consulta primeiro.
    if rodada % 2 != 0:

        baseline_count, baseline_time = executar_benchmark(query_baseline)
        optimized_count, optimized_time = executar_benchmark(query_optimized)

    else:

        optimized_count, optimized_time = executar_benchmark(query_optimized)
        baseline_count, baseline_time = executar_benchmark(query_baseline)

    baseline_times.append(baseline_time)
    optimized_times.append(optimized_time)

    print(f"RODADA {rodada}")
    print(f"Baseline:  {baseline_time:.3f} s")
    print(f"Otimizada: {optimized_time:.3f} s")
    print(f"Linhas baseline:  {baseline_count:,}")
    print(f"Linhas otimizada: {optimized_count:,}")
    print("-" * 40)


print("\n=== RESUMO ===")

print("\nBASELINE")
print(f"Mínimo:  {min(baseline_times):.3f} s")
print(f"Máximo:  {max(baseline_times):.3f} s")
print(f"Média:   {statistics.mean(baseline_times):.3f} s")
print(f"Mediana: {statistics.median(baseline_times):.3f} s")

print("\nOTIMIZADA")
print(f"Mínimo:  {min(optimized_times):.3f} s")
print(f"Máximo:  {max(optimized_times):.3f} s")
print(f"Média:   {statistics.mean(optimized_times):.3f} s")
print(f"Mediana: {statistics.median(optimized_times):.3f} s")


media_baseline = statistics.mean(baseline_times)
media_optimized = statistics.mean(optimized_times)

melhoria_media = (
    (media_baseline - media_optimized)
    / media_baseline
) * 100

mediana_baseline = statistics.median(baseline_times)
mediana_optimized = statistics.median(optimized_times)

melhoria_mediana = (
    (mediana_baseline - mediana_optimized)
    / mediana_baseline
) * 100


print("\n=== COMPARAÇÃO ===")

print(
    f"Variação pela média: "
    f"{melhoria_media:.2f}%"
)

print(
    f"Variação pela mediana: "
    f"{melhoria_mediana:.2f}%"
)