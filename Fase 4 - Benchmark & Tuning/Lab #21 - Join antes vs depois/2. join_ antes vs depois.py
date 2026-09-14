
from pyspark.sql import functions as F
from time import perf_counter

# Fato: 20 milhões de registros
df_orders = (
    spark.range(20_000_000)
    .select(
        F.col("id").alias("order_id"),
        (F.col("id") % 100_000).alias("product_id"),
        ((F.col("id") % 5) + 1).alias("quantity")
    )
)

# Dimensão: 100 mil produtos
df_products = (
    spark.range(100_000)
    .select(
        F.col("id").alias("product_id"),
        (F.col("id") % 100).alias("category_id"),
        ((F.col("id") % 500) + 10).cast("double").alias("price")
    )
)


######### Experimento A — Baseline real #########


df_join_baseline = (
    df_orders
    .join(
        df_products,
        on="product_id",
        how="inner"
    )
)

df_join_baseline.explain(True)



inicio = perf_counter()

resultado_baseline = df_join_baseline.count()

fim = perf_counter()

tempo_baseline = fim - inicio

print("Quantidade de linhas:", resultado_baseline)
print(f"Tempo Baseline: {tempo_baseline:.3f} s")


######### Experimento B — Baseline controlada com Merge #########



df_join_merge = (
    df_orders
    .join(
        df_products.hint("merge"),
        on="product_id",
        how="inner"
    )
)

df_join_merge.explain(True)

df_join_merge.explain(mode="cost")



inicio = perf_counter()

resultado_merge = df_join_merge.count()

fim = perf_counter()

tempo_merge = fim - inicio

print("Quantidade de linhas:", resultado_merge)
print(f"Tempo Merge Join: {tempo_merge:.3f} s")



######### Experimento C — Broadcast #########
 

df_join_broadcast = (
    df_orders
    .join(
        F.broadcast(df_products),
        on="product_id",
        how="inner"
    )
)

df_join_broadcast.explain(True)


df_join_broadcast.explain(mode="cost")


inicio = perf_counter()

resultado_broadcast = df_join_broadcast.count()

fim = perf_counter()

tempo_broadcast = fim - inicio

print("Quantidade de linhas:", resultado_broadcast)
print(f"Tempo Broadcast Join: {tempo_broadcast:.3f} s")




######### Experimento D — Comparação #########



print("=== RESULTADO DO BENCHMARK ===")

print("\nBaseline automática:")
print(f"  Linhas: {resultado_baseline:,}")
print(f"  Tempo:  {tempo_baseline:.3f} s")

print("\nMerge Join:")
print(f"  Linhas: {resultado_merge:,}")
print(f"  Tempo:  {tempo_merge:.3f} s")

print("\nBroadcast Join explícito:")
print(f"  Linhas: {resultado_broadcast:,}")
print(f"  Tempo:  {tempo_broadcast:.3f} s")


tempos = {
    "Baseline automática": tempo_baseline,
    "Merge Join": tempo_merge,
    "Broadcast Join explícito": tempo_broadcast
}

mais_rapido = min(tempos, key=tempos.get)
tempo_mais_rapido = tempos[mais_rapido]

print("\n=== COMPARAÇÃO ===")

for estrategia, tempo in tempos.items():
    if estrategia != mais_rapido:
        diferenca = tempo - tempo_mais_rapido
        percentual = (diferenca / tempo) * 100

        print(
            f"{mais_rapido} foi {percentual:.2f}% mais rápido "
            f"que {estrategia} nesta execução."
        )

print(f"\nEstratégia mais rápida: {mais_rapido}")
print(f"Tempo: {tempo_mais_rapido:.3f} s")



# === RESULTADO DO BENCHMARK === --> RODADA 7

# Baseline automática:
#   Linhas: 20,000,000
#   Tempo:  0.527 s

# Merge Join:
#   Linhas: 20,000,000
#   Tempo:  0.987 s

# Broadcast Join explícito:
#   Linhas: 20,000,000
#   Tempo:  0.427 s

# === COMPARAÇÃO ===
# Broadcast Join explícito foi 19.01% mais rápido que Baseline automática nesta execução.
# Broadcast Join explícito foi 56.73% mais rápido que Merge Join nesta execução.

# Estratégia mais rápida: Broadcast Join explícito
# Tempo: 0.427 s



# === RESULTADO DO BENCHMARK === --> RODADA 6

# Baseline automática:
#   Linhas: 20,000,000
#   Tempo:  0.518 s

# Merge Join:
#   Linhas: 20,000,000
#   Tempo:  1.277 s

# Broadcast Join explícito:
#   Linhas: 20,000,000
#   Tempo:  0.531 s

# === COMPARAÇÃO ===
# Baseline automática foi 59.43% mais rápido que Merge Join nesta execução.
# Baseline automática foi 2.50% mais rápido que Broadcast Join explícito nesta execução.

# Estratégia mais rápida: Baseline automática
# Tempo: 0.518 s



# === RESULTADO DO BENCHMARK === --> RODADA 5

# Baseline automática:
#   Linhas: 20,000,000
#   Tempo:  0.532 s

# Merge Join:
#   Linhas: 20,000,000
#   Tempo:  1.337 s

# Broadcast Join explícito:
#   Linhas: 20,000,000
#   Tempo:  0.600 s

# === COMPARAÇÃO ===
# Baseline automática foi 60.19% mais rápido que Merge Join nesta execução.
# Baseline automática foi 11.35% mais rápido que Broadcast Join explícito nesta execução.

# Estratégia mais rápida: Baseline automática
# Tempo: 0.532 s



# === RESULTADO DO BENCHMARK === --> RODADA 4

# Baseline automática:
#   Linhas: 20,000,000
#   Tempo:  0.570 s

# Merge Join:
#   Linhas: 20,000,000
#   Tempo:  1.166 s

# Broadcast Join explícito:
#   Linhas: 20,000,000
#   Tempo:  0.541 s

# === COMPARAÇÃO ===
# Broadcast Join explícito foi 5.14% mais rápido que Baseline automática nesta execução.
# Broadcast Join explícito foi 53.65% mais rápido que Merge Join nesta execução.

# Estratégia mais rápida: Broadcast Join explícito
# Tempo: 0.541 s



# === RESULTADO DO BENCHMARK === --> RODADA 3

# Baseline automática:
#   Linhas: 20,000,000
#   Tempo:  0.570 s

# Merge Join:
#   Linhas: 20,000,000
#   Tempo:  1.166 s

# Broadcast Join explícito:
#   Linhas: 20,000,000
#   Tempo:  0.541 s

# === COMPARAÇÃO ===
# Broadcast Join explícito foi 5.14% mais rápido que Baseline automática nesta execução.
# Broadcast Join explícito foi 53.65% mais rápido que Merge Join nesta execução.

# Estratégia mais rápida: Broadcast Join explícito
# Tempo: 0.541 s



# === RESULTADO DO BENCHMARK === --> RODADA 2

# Baseline automática:
#   Linhas: 20,000,000
#   Tempo:  0.682 s

# Merge Join:
#   Linhas: 20,000,000
#   Tempo:  1.470 s

# Broadcast Join explícito:
#   Linhas: 20,000,000
#   Tempo:  0.585 s

# === COMPARAÇÃO ===
# Broadcast Join explícito foi 14.26% mais rápido que Baseline automática nesta execução.
# Broadcast Join explícito foi 60.24% mais rápido que Merge Join nesta execução.

# Estratégia mais rápida: Broadcast Join explícito
# Tempo: 0.585 s



# === RESULTADO DO BENCHMARK === --> RODADA 1

# Baseline automática:
#   Linhas: 20,000,000
#   Tempo:  0.677 s

# Merge Join:
#   Linhas: 20,000,000
#   Tempo:  2.446 s

# Broadcast Join explícito:
#   Linhas: 20,000,000
#   Tempo:  1.128 s

# === COMPARAÇÃO ===
# Baseline automática foi 72.34% mais rápido que Merge Join nesta execução.
# Baseline automática foi 40.03% mais rápido que Broadcast Join explícito nesta execução.

# Estratégia mais rápida: Baseline automática
# Tempo: 0.677 s
