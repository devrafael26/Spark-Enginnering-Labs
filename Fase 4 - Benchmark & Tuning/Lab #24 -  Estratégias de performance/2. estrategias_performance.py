######## Experimento A — Baseline ########

from pyspark.sql import functions as F

# 1. ORDERS

df_orders = (
    spark.range(30_000_000)
    .withColumn(
        "product_id",
        (F.col("id") % 1_000_000).cast("long")
    )
    .withColumn(
        "amount",
        (F.col("id") % 500) + 1
    )
)


# 2. PRODUCTS


df_products = (
    spark.range(1_000_000)
    .withColumnRenamed("id", "product_id")
    .withColumn(
        "category_id",
        (F.col("product_id") % 100).cast("long")
    )
)



# 3. BASELINE
#
# Primeiro fazemos o JOIN.
# Depois aplicamos um filtro bastante seletivo.


df_baseline = (
    df_orders
    .join(
        df_products,
        on="product_id",
        how="inner"
    )
    .filter(
        F.col("amount") >= 450
    )
    .groupBy("category_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("total_orders")
    )
)



# 4. PLANO


df_baseline.explain(True)




## Lógica escrita

# 30 milhões de orders
#         +
# 1 milhão de products
#         ↓
#        JOIN
#         ↓
#       FILTER
#  amount >= 450
#         ↓
#     GROUP BY




# EXPERIMENTO A — BASELINE

import time
inicio_a = time.perf_counter()

df_baseline.show()

fim_a = time.perf_counter()

tempo_a = fim_a - inicio_a



######## Experimento B — estratégia de performance ########


# Redução dos dados antes do JOIN
# 1. FILTRAR ORDERS PRIMEIRO


df_orders_filtered = (
    df_orders
    .filter(
        F.col("amount") >= 450
    )
)

# 2. JOIN APÓS A REDUÇÃO DOS DADOS

df_optimized = (
    df_orders_filtered
    .join(
        df_products,
        on="product_id",
        how="inner"
    )
    .groupBy("category_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("total_orders")
    )
)


# 3. PLANO

df_optimized.explain(True)




# Lógica escrita

# 30 milhões de orders
#         ↓
#       FILTER
#  amount >= 450
#         ↓
#       ~3 milhões
#         +
#       products
#         ↓
#        JOIN
#         ↓
#     GROUP BY



# FILTRO ANTES DO JOIN

import time

inicio_b = time.perf_counter()

df_optimized.show()

fim_b = time.perf_counter()

tempo_b = fim_b - inicio_b




# Benchmark A vs B
# comparar as execuções alternando a ordem para verificar se a diferença observada depende da versão A/B ou da posição da execução na sequência.



import time

# COMPARAÇÃO

print("\n")
print("=" * 60)
print("RESULTADO DO BENCHMARK")
print("=" * 60)

print(f"\nExperimento A — Baseline:")
print(f"Tempo: {tempo_a:.3f} s")

print(f"\nExperimento B — Filtro antes do Join:")
print(f"Tempo: {tempo_b:.3f} s")


# DIFERENÇA PERCENTUAL

if tempo_a < tempo_b:

    diferenca = ((tempo_b - tempo_a) / tempo_b) * 100

    print(
        f"\nExperimento A foi {diferenca:.2f}% "
        f"mais rápido que o Experimento B."
    )

    print("\nEstratégia mais rápida: Experimento A — Baseline")
    print(f"Tempo: {tempo_a:.3f} s")


elif tempo_b < tempo_a:

    diferenca = ((tempo_a - tempo_b) / tempo_a) * 100

    print(
        f"\nExperimento B foi {diferenca:.2f}% "
        f"mais rápido que o Experimento A."
    )

    print("\nEstratégia mais rápida: Experimento B — Filtro antes do Join")
    print(f"Tempo: {tempo_b:.3f} s")


else:

    print("\nOs dois experimentos apresentaram o mesmo tempo.")

print("\n" + "=" * 60)



# RODADA 1
# RESULTADO DO BENCHMARK


# Experimento A — Baseline:
# Tempo: 0.896 s

# Experimento B — Filtro antes do Join:
# Tempo: 0.887 s

# Experimento B foi 1.00% mais rápido que o Experimento A.

# Estratégia mais rápida: Experimento B — Filtro antes do Join
# Tempo: 0.887 s





# RODADA 2
# RESULTADO DO BENCHMARK

# Experimento A — Baseline:
# Tempo: 0.979 s

# Experimento B — Filtro antes do Join:
# Tempo: 0.846 s

# Experimento B foi 13.68% mais rápido que o Experimento A.

# Estratégia mais rápida: Experimento B — Filtro antes do Join
# Tempo: 0.846 s





# RODADA 3
# RESULTADO DO BENCHMARK

# Experimento A — Baseline:
# Tempo: 0.724 s

# Experimento B — Filtro antes do Join:
# Tempo: 0.849 s

# Experimento A foi 14.69% mais rápido que o Experimento B.

# Estratégia mais rápida: Experimento A — Baseline
# Tempo: 0.724 s





# RODADA 4
# RESULTADO DO BENCHMARK

# Experimento A — Baseline:
# Tempo: 0.757 s

# Experimento B — Filtro antes do Join:
# Tempo: 0.900 s

# Experimento A foi 15.89% mais rápido que o Experimento B.

# Estratégia mais rápida: Experimento A — Baseline
# Tempo: 0.757 s





# RODADA 5
# RESULTADO DO BENCHMARK

# Experimento A — Baseline:
# Tempo: 0.717 s

# Experimento B — Filtro antes do Join:
# Tempo: 0.789 s

# Experimento A foi 9.12% mais rápido que o Experimento B.

# Estratégia mais rápida: Experimento A — Baseline
# Tempo: 0.717 s





# RODADA 6
# RESULTADO DO BENCHMARK


# Experimento A — Baseline:
# Tempo: 0.711 s

# Experimento B — Filtro antes do Join:
# Tempo: 0.707 s

# Experimento B foi 0.45% mais rápido que o Experimento A.

# Estratégia mais rápida: Experimento B — Filtro antes do Join
# Tempo: 0.707 s





# RODADA 7
# RESULTADO DO BENCHMARK


# Experimento A — Baseline:
# Tempo: 0.843 s

# Experimento B — Filtro antes do Join:
# Tempo: 0.761 s

# Experimento B foi 9.70% mais rápido que o Experimento A.

# Estratégia mais rápida: Experimento B — Filtro antes do Join
# Tempo: 0.761 s




########################### PARTE 2 DO LAB ##########################



######## Experimento C — Baseline: Join antes da agregação ########

from pyspark.sql import functions as F
import time


df_experimento_c = (
    df_orders
    .join(
        df_products,
        on="product_id",
        how="inner"
    )
    .groupBy("category_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("total_orders")
    )
)



# PLANO

df_experimento_c.explain(True)


# ACTION + TEMPO

inicio_c = time.perf_counter()

df_experimento_c.show()

fim_c = time.perf_counter()

tempo_c = fim_c - inicio_c

print(f"\nTempo Experimento C: {tempo_c:.3f} s")


## Lógica de escrita

# orders
# 30 milhões
#     │
#     │
#     ├──────── products
#     │          1 milhão
#     │
#     ▼
#    JOIN
#     │
#     ▼
# GROUP BY category_id
#     │
#     ▼
# resultado




######## Experimento D — Estratégia: pré-agregação antes do Join ########


# 1. PRÉ-AGREGAÇÃO DE ORDERS POR PRODUCT_ID


df_orders_agg = (
    df_orders
    .groupBy("product_id")
    .agg(
        F.sum("amount").alias("product_total_amount"),
        F.count("*").alias("product_total_orders")
    )
)


# 2. JOIN APÓS A REDUÇÃO DE CARDINALIDADE


df_experimento_d = (
    df_orders_agg
    .join(
        df_products,
        on="product_id",
        how="inner"
    )
    .groupBy("category_id")
    .agg(
        F.sum("product_total_amount").alias("total_amount"),
        F.sum("product_total_orders").alias("total_orders")
    )
)



# PLANO

df_experimento_d.explain(True)


# ACTION + TEMPO

inicio_d = time.perf_counter()

df_experimento_d.show()

fim_d = time.perf_counter()

tempo_d = fim_d - inicio_d

print(f"\nTempo Experimento D: {tempo_d:.3f} s")


# Lógica de escrita

# orders
# 30 milhões
#     │
#     ▼
# GROUP BY product_id
#     │
#     │ 30M → aproximadamente/exatamente 1M grupos
#     │
#     ▼
# orders agregados
# ~1 milhão
#     │
#     ├──────── products
#     │          1 milhão
#     │
#     ▼
#    JOIN
#     │
#     ▼
# GROUP BY category_id
#     │
#     ▼
# resultado


# LAB 24 — BENCHMARK C vs D


print("\n")
print("=" * 60)
print("RESULTADO DO BENCHMARK — EXPERIMENTOS C vs D")
print("=" * 60)

print("\nExperimento C — Join antes da agregação:")
print(f"Tempo: {tempo_c:.3f} s")

print("\nExperimento D — Pré-agregação antes do Join:")
print(f"Tempo: {tempo_d:.3f} s")


if tempo_c < tempo_d:

    diferenca = ((tempo_d - tempo_c) / tempo_d) * 100

    print(
        f"\nExperimento C foi {diferenca:.2f}% "
        f"mais rápido que o Experimento D."
    )

    print("\nEstratégia mais rápida: Experimento C — Baseline")
    print(f"Tempo: {tempo_c:.3f} s")


elif tempo_d < tempo_c:

    diferenca = ((tempo_c - tempo_d) / tempo_c) * 100

    print(
        f"\nExperimento D foi {diferenca:.2f}% "
        f"mais rápido que o Experimento C."
    )

    print("\nEstratégia mais rápida: Experimento D — Pré-agregação")
    print(f"Tempo: {tempo_d:.3f} s")


else:

    print("\nOs dois experimentos apresentaram o mesmo tempo.")

print("\n" + "=" * 60)



# RODADA 1 - ordem C e D

# Experimento C — Join antes da agregação:
# Tempo: 0.844 s

# Experimento D — Pré-agregação antes do Join:
# Tempo: 1.423 s

# Experimento C foi 40.72% mais rápido que o Experimento D.

# Estratégia mais rápida: Experimento C — Baseline
# Tempo: 0.844 s


# RODADA 2 - AQUECIMENTO feita HOJE ordem C -> D 

# Experimento C — Join antes da agregação:
# Tempo: 2.193 s

# Experimento D — Pré-agregação antes do Join:
# Tempo: 2.162 s

# Experimento D foi 1.40% mais rápido que o Experimento C.

# Estratégia mais rápida: Experimento D — Pré-agregação
# Tempo: 2.162 s



# RODADA 3 - ORDEM D -> C

# Experimento C — Join antes da agregação:
# Tempo: 1.323 s

# Experimento D — Pré-agregação antes do Join:
# Tempo: 1.749 s

# Experimento C foi 24.34% mais rápido que o Experimento D.

# Estratégia mais rápida: Experimento C — Baseline
# Tempo: 1.323 s


# RODADA 4 - ORDEM C -> D

# Experimento C — Join antes da agregação:
# Tempo: 1.361 s

# Experimento D — Pré-agregação antes do Join:
# Tempo: 1.711 s

# Experimento C foi 20.45% mais rápido que o Experimento D.

# Estratégia mais rápida: Experimento C — Baseline
# Tempo: 1.361 s


# RODADA 5 - ORDEM D -> C

# Experimento C — Join antes da agregação:
# Tempo: 1.137 s

# Experimento D — Pré-agregação antes do Join:
# Tempo: 1.508 s

# Experimento C foi 24.60% mais rápido que o Experimento D.

# Estratégia mais rápida: Experimento C — Baseline
# Tempo: 1.137 s


## RODADA 6 - ORDEM C -> D

# Experimento C — Join antes da agregação:
# Tempo: 1.080 s

# Experimento D — Pré-agregação antes do Join:
# Tempo: 1.571 s

# Experimento C foi 31.28% mais rápido que o Experimento D.

# Estratégia mais rápida: Experimento C — Baseline
# Tempo: 1.080 s


#RODADA 7 - ORDEM D -> C

# Experimento C — Join antes da agregação:
# Tempo: 1.024 s

# Experimento D — Pré-agregação antes do Join:
# Tempo: 1.529 s

# Experimento C foi 33.01% mais rápido que o Experimento D.

# Estratégia mais rápida: Experimento C — Baseline
# Tempo: 1.024 s


# RODADA 8 - ORDEM C -> D

# Experimento C — Join antes da agregação:
# Tempo: 0.910 s

# Experimento D — Pré-agregação antes do Join:
# Tempo: 1.554 s

# Experimento C foi 41.43% mais rápido que o Experimento D.

# Estratégia mais rápida: Experimento C — Baseline
# Tempo: 0.910 s

