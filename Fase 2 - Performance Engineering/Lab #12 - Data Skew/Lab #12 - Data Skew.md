## *Spark Engineering Lab 12 - Data Skew*

## Categoria: Performance Engineering

## Objetivo

Demonstrar como uma distribuição desigual dos dados entre as chaves pode gerar um cenário de Data Skew durante operações que exigem Shuffle, observando seu comportamento em agregações e joins.

## Pergunta

Como uma distribuição desigual dos dados entre as chaves pode provocar Data Skew e impactar o processamento no Spark?

## Experimento

Experimento A:

DataFrame com distribuição equilibrada  
→ `groupBy(key_id)`  
→ `count()`  
→ `show()`  
→ `explain(True)`

Experimento B:

DataFrame com distribuição skewed  
→ concentração de 4 milhões de registros em `key_id = 0`  
→ `groupBy(key_id)`  
→ `count()`  
→ `show()`  
→ `explain(True)`

Experimento C:

DataFrame skewed  
→ `MERGE` hint  
→ `join()`  
→ `count()`  
→ Query Profile  
→ `explain(True)`

## Dados

Foram utilizados dados artificiais criados com `spark.range()`.

No Experimento A foram gerados 5 milhões de registros distribuídos de forma equilibrada entre 100 valores de `key_id`.

No Experimento B foram gerados 5 milhões de registros, sendo 4 milhões concentrados em `key_id = 0` e o restante distribuído entre as demais chaves.

No Experimento C foi utilizado um DataFrame com 5 milhões de registros e a mesma distribuição skewed do Experimento B, realizando o join com um segundo DataFrame contendo 100 chaves.

## Transformações

Foram realizadas:

- criação da coluna `key_id`;
- distribuição equilibrada dos dados utilizando `id % 100`;
- criação proposital de uma distribuição skewed utilizando `when()` e `lit()`;
- `groupBy()` por `key_id`;
- agregação com `count()`;
- `join()` utilizando `key_id`;
- aplicação do hint `MERGE` para direcionar a execução para `SortMergeJoin`.

## Action

Foram utilizadas as Actions:

- `show()` nos experimentos de agregação;
- `count()` no experimento com join.

## Comando de análise

Foram utilizados:

- `explain(True)` para análise dos planos lógico e físico;
- Query Profile para observar `Rows`, `Time Spent` e `Memory Peak`.

No plano físico foram observados principalmente:

- `PhotonShuffleExchangeSink`;
- `hashpartitioning(key_id, 38)`;
- `SortMergeJoin`;
- `AdaptiveSparkPlan`.

Também foi verificado se o AQE apresentava evidência de tratamento de Skew Join por meio de `isSkew=true`.

## Código

```python
from pyspark.sql import functions as F

# Experimento A — Distribuição equilibrada

df_balanced = (
    spark.range(5_000_000)
    .withColumn("key_id", F.col("id") % 100)
)

df_balanced.groupBy("key_id").count().orderBy("key_id").show(100)

df_balanced_agg = (
    df_balanced
    .groupBy("key_id")
    .agg(
        F.count("*").alias("total")
    )
)

df_balanced_agg.show()

df_balanced_agg.explain(True)


# Experimento B — Distribuição com Data Skew

df_skew = (
    spark.range(5_000_000)
    .withColumn(
        "key_id",
        F.when(
            F.col("id") < 4_000_000,
            F.lit(0)
        ).otherwise(
            (F.col("id") % 99) + 1
        )
    )
)

df_skew.groupBy("key_id") \
    .count() \
    .orderBy(
        F.desc("count"),
        F.asc("key_id")
    ) \
    .show(100)

df_skew_agg = (
    df_skew
    .groupBy("key_id")
    .agg(
        F.count("*").alias("total")
    )
)

df_skew_agg.show()

df_skew_agg.explain(True)


# Experimento C — Data Skew em Join

df_left = (
    spark.range(5_000_000)
    .withColumn(
        "key_id",
        F.when(
            F.col("id") < 4_000_000,
            F.lit(0)
        ).otherwise(
            (F.col("id") % 99) + 1
        )
    )
    .withColumnRenamed("id", "left_id")
)

df_keys = (
    spark.range(100)
    .withColumnRenamed("id", "key_id")
    .withColumn(
        "description",
        F.concat(
            F.lit("key_"),
            F.col("key_id")
        )
    )
)

df_skew_join = (
    df_left
    .hint("merge")
    .join(
        df_keys.hint("merge"),
        "key_id",
        "inner"
    )
)

df_skew_join.count()

df_skew_join.explain(True)
```

## Conclusão

Nos experimentos realizados, foi criada propositalmente uma distribuição de dados desbalanceada, concentrando 4 milhões dos 5 milhões de registros em uma única chave. O plano físico mostrou que essa chave foi utilizada no particionamento do Shuffle e, no experimento com join, foi utilizado um `SortMergeJoin`.

Apesar disso, não foi observada evidência de que o Spark/AQE tenha identificado uma partition como skewed, pois o plano não apresentou `isSkew=true` nem indicação de tratamento específico de Skew Join.

Portanto, o laboratório demonstrou a criação de dados skewed e o cenário que pode levar ao Data Skew, mas não demonstrou a detecção e o tratamento automático desse problema pelo AQE.

#### Explain - Experimento C
```text
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Initial Plan ==
   Project [key_id#11425L, left_id#11426L, description#11436]
   +- SortMergeJoin [key_id#11425L], [key_id#11434L], Inner
      :- ColumnarToRow
      :  +- PhotonResultStage
      :     +- PhotonSort [key_id#11425L ASC NULLS FIRST]
      :        +- PhotonShuffleExchangeSource false
      :           +- PhotonShuffleMapStage ENSURE_REQUIREMENTS, [id=#8854]
      :              +- PhotonShuffleExchangeSink hashpartitioning(key_id#11425L, 38)
      :                 +- PhotonProject [id#11423L AS left_id#11426L, CASE WHEN (id#11423L < 4000000) THEN 0 ELSE ((id#11423L % 99) + 1) END AS key_id#11425L]
      :                    +- PhotonRange Range (0, 5000000, step=1, splits=8)
      +- ColumnarToRow
         +- PhotonResultStage
            +- PhotonSort [key_id#11434L ASC NULLS FIRST]
               +- PhotonShuffleExchangeSource false
                  +- PhotonShuffleMapStage ENSURE_REQUIREMENTS, [id=#8863]
                     +- PhotonShuffleExchangeSink hashpartitioning(key_id#11434L, 38)
                        +- PhotonProject [id#11433L AS key_id#11434L, concat(key_, cast(id#11433L as string)) AS description#11436]
                           +- PhotonRange Range (0, 100, step=1, splits=8)
```




