## *Spark Engineering Lab 08 — Shuffle*

## Categoria: Performance Engineering

## Objetivo
Demonstrar por que determinadas operações exigem a redistribuição dos dados entre partições e observar o Shuffle gerado pelo Spark durante a execução.

## Pergunta
Por que o Spark precisa realizar Shuffle durante determinadas operações e como podemos identificar esse comportamento na execução?

## Experimento
DataFrame → `groupBy()` → aggregation → `explain(True)`

## Dados
Como os dados foram criados?

Com `spark.range(5_000_000)` e `spark.range(10_000_000)`.

## Transformações
Quais operações foram realizadas?

Aplicação de `withColumn()` utilizando `F.col()`, seguida de `groupBy()` e `count()` como função de agregação.

## Action
Qual operação disparou a execução?

Não se aplica. O experimento foi analisado por meio do `explain(True)`.

## Comando de análise
`df_result5m.explain(True)` e `df_result10m.explain(True)`

O plano físico foi analisado para identificar a presença de operadores relacionados ao Shuffle, especialmente `Shuffle Exchange`, `Shuffle Map Stage` e `Hash Partitioning`.

## Código

```python
from pyspark.sql import functions as F

## 5 milhões de registros
df_orders = (
    spark.range(5_000_000)
    .withColumn("customer_id", (F.col("id") % 100_000).cast("long"))
    .withColumn("category_id", (F.col("id") % 20).cast("int"))
    .withColumn("amount", (F.rand() * 1000).cast("double"))
)

df_result5m = (
    df_orders
    .groupBy("category_id")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("amount").alias("total_amount")
    )
)

df_result5m.explain(True)

## 10 milhões de registros
df_orders10m = (
    spark.range(10_000_000)
    .withColumn("customer_id", (F.col("id") % 100_000).cast("long"))
    .withColumn("category_id", (F.col("id") % 20).cast("int"))
    .withColumn("amount", (F.rand() * 1000).cast("double"))
)

df_result10m = (
    df_orders10m
    .groupBy("category_id")
    .agg(
        F.count("*").alias("total_orders"),
        F.sum("amount").alias("total_amount")
    )
)

df_result10m.explain(True)
```

## Conclusão
Ao observar o plano físico na saída do `df_result5m.explain(True)`, podemos observar que a agregação por `groupBy()` exige redistribuição dos dados entre as partições, pois registros pertencentes à mesma chave de agrupamento podem estar distribuídos em partições diferentes e precisam ser reorganizados para que possam ser processados juntos. No plano físico, essa redistribuição pode ser identificada pela presença de Exchange associado a `hashpartitioning(...)`.

O `Hash Partitioning` indica que os dados são redistribuídos com base na chave utilizada na agregação, neste caso `category_id`, organizando os dados em 38 partições para o `Range` de 5 milhões de registros e permitindo que registros pertencentes ao mesmo grupo sejam processados juntos.

Ao variar o volume de dados de entrada, também foi observada uma alteração no número de partições definido para o Shuffle. Com 5 milhões de registros, o plano apresentou `hashpartitioning(category_id, 38)`, enquanto com 10 milhões apresentou `hashpartitioning(category_id, 76)`. No ambiente utilizado, `spark.sql.shuffle.partitions` está configurado como `auto`, permitindo que o Auto Optimized Shuffle determine automaticamente esse número com base no plano da consulta e no tamanho dos dados de entrada.

O experimento demonstra que o Shuffle não é uma operação necessariamente ruim ou que deve ser evitada, mas um mecanismo necessário em determinadas operações distribuídas. Em engenharia de performance, o objetivo é identificar quando ele ocorre, compreender seu custo e avaliar formas de reduzir seu impacto quando possível.

- Physical Plan do `df_result5m`: 5 milhões de registros

```text
AdaptiveSparkPlan isFinalPlan=false
+- == Initial Plan ==
   PhotonResultStage
   +- PhotonColumnarToRow
      +- PhotonGroupingAgg(limit=None, keys=[category_id##11184], functions=[finalmerge_count(merge count##11269L) AS count(1)##11266L, finalmerge_sum(merge sum##11271) AS sum(amount)##11267], output=[category_id##11184, total_orders##11233L, total_amount##11234])
         +- PhotonShuffleExchangeSource
            +- PhotonShuffleMapStage ENSURE_REQUIREMENTS, [id=##7511]
               +- PhotonShuffleExchangeSink hashpartitioning(category_id##11184, 38)
                  +- PhotonGroupingAgg(limit=None, keys=[category_id##11184], functions=[partial_count(1) AS count##11269L, partial_sum(amount##11186) AS sum##11271], output=[category_id##11184, count##11269L, sum##11271])
                     +- PhotonProject [cast((id##11180L % 20) as int) AS category_id##11184, (rand(4366637453063449799) * 1000.0) AS amount##11186]
                        +- PhotonRange Range (0, 5000000, step=1, splits=8)
```

- Physical Plan do `df_result10m`: 10 milhões de registros

```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Initial Plan ==
   PhotonResultStage
   +- PhotonColumnarToRow
      +- PhotonGroupingAgg(limit=None, keys=[category_id##11376], functions=[finalmerge_count(merge count##11388L) AS count(1)##11385L, finalmerge_sum(merge sum##11390) AS sum(amount)##11386], output=[category_id##11376, total_orders##11380L, total_amount##11381])
         +- PhotonShuffleExchangeSource false
            +- PhotonShuffleMapStage ENSURE_REQUIREMENTS, [id=##7839]
               +- PhotonShuffleExchangeSink hashpartitioning(category_id##11376, 76)
                  +- PhotonGroupingAgg(limit=None, keys=[category_id##11376], functions=[partial_count(1) AS count##11388L, partial_sum(amount##11378) AS sum##11390], output=[category_id##11376, count##11388L, sum##11390])
                     +- PhotonProject [cast((id##11372L % 20) as int) AS category_id##11376, (rand(695320079806222855) * 1000.0) AS amount##11378]
                        +- PhotonRange Range (0, 10000000, step=1, splits=8)
```

## Observações

Operações que frequentemente podem provocar Shuffle incluem:

- `groupBy()`
- `join()`
- `distinct()`
- `orderBy()`
- `repartition()`
- algumas agregações/window operations
