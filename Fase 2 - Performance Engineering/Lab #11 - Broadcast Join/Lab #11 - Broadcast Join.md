## *Spark Engineering Lab 11 - Broadcast Join*

## Categoria: Performance Engineering

## Objetivo

Demonstrar como o Broadcast Join pode ser utilizado quando um dos lados de um join é pequeno, observando como essa estratégia altera o plano de execução e pode evitar a redistribuição da relação maior.

## Pergunta

Quando o Spark pode utilizar Broadcast Join e como essa estratégia altera a execução de um join entre conjuntos de dados de tamanhos diferentes?

## Experimento

```text
DataFrame grande
      +
DataFrame pequeno
      ↓
     Join
      ↓
explain(True)

        x

DataFrame grande
      +
broadcast(DataFrame pequeno)
      ↓
     Join
      ↓
explain(True)
```

## Dados

Como os dados foram criados?

Os dados foram criados com `spark.range()`:

`df_orders`

5.000.000 registros

`df_products`

1.000 registros

A coluna `product_id` foi criada nos pedidos por meio da expressão:

`order_id % 1_000`

produzindo valores entre 0 e 999, correspondentes aos 1.000 produtos existentes em `df_products`.

## Transformações

Foram realizadas:

`withColumnRenamed()`, `withColumn()`, `join()` e `broadcast()`

No primeiro experimento, o Join foi realizado sem informar explicitamente uma estratégia de Broadcast, deixando o Spark definir a estratégia de execução.

No segundo experimento, `broadcast(df_products)` foi utilizado para indicar explicitamente `df_products` como a relação a ser transmitida.

## Action

Não se aplica.

## Comando de análise

`explain(True)`

O plano físico foi utilizado para observar principalmente os operadores:

`PhotonBroadcastHashJoin` indica que o Spark escolheu Broadcast Hash Join como estratégia física para realizar o Join.

`BuildRight` indica que o lado direito do Join foi escolhido como relação de construção do Broadcast.

`PhotonShuffleMapStage EXECUTOR_BROADCAST` também aparece no ramo correspondente ao `df_products`, indicando sua preparação para utilização no Broadcast Join.


## Código

```python
from pyspark.sql import functions as F

# DataFrame grande: 5 milhões de pedidos
df_orders = (
    spark.range(5_000_000)
    .withColumnRenamed("id", "order_id")
    .withColumn(
        "product_id",
        (F.col("order_id") % 1_000).cast("long")
    )
    .withColumn(
        "quantity",
        ((F.col("order_id") % 5) + 1).cast("int")
    )
)

# DataFrame pequeno: 1.000 produtos
df_products = (
    spark.range(1_000)
    .withColumnRenamed("id", "product_id")
    .withColumn(
        "product_name",
        F.concat(
            F.lit("Product_"),
            F.col("product_id")
        )
    )
)

# Experimento A — Spark define a estratégia
df_join = df_orders.join(
    df_products,
    on="product_id",
    how="inner"
)

df_join.explain(True)

# Experimento B — Broadcast indicado explicitamente
df_join_broadcast = df_orders.join(
    F.broadcast(df_products),
    on="product_id",
    how="inner"
)

df_join_broadcast.explain(True)
```

## Conclusão

No Experimento A, o Spark escolheu sozinho uma estratégia de broadcast e, como o join é um equi-join por product_id, o operador físico escolhido foi PhotonBroadcastHashJoin.

No Experimento B, quando usamos broadcast(df_products), estamos colocando um hint de estratégia. Esse hint prioriza o Broadcast para aquele lado do join. Como continua sendo o mesmo tipo de join, o resultado físico também é PhotonBroadcastHashJoin. A documentação do Spark explica justamente que o hint BROADCAST prioriza um broadcast join e que, em um equi-join, isso normalmente resulta em Broadcast Hash Join; outros tipos de join podem levar, por exemplo, a Broadcast Nested Loop Join.

Ao utilizar `broadcast(df_products)` no segundo experimento, indicamos explicitamente ao Spark que `df_products` deveria ser considerado como o lado de Broadcast. Esse hint pôde ser observado no plano lógico, inicialmente como `UnresolvedHint broadcast`, depois como `ResolvedHint (strategy=broadcast)` e, no plano lógico otimizado, como `rightHint=(strategy=broadcast)`.

No plano físico, o hint não aparece mais explicitamente. Seu efeito é refletido na escolha de `PhotonBroadcastHashJoin` com `BuildRight`, indicando que o lado direito do Join, `df_products`, foi utilizado como relação de Broadcast. 


#### Explains
```text
Experimento A
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Initial Plan ==
   PhotonResultStage
   +- PhotonColumnarToRow
      +- PhotonProject [product_id#11241L, order_id#11239L, quantity#11243, product_name#11248]
         +- PhotonBroadcastHashJoin [product_id#11241L], [product_id#11246L], Inner, BuildRight, false, true, false
            :- PhotonProject [id#11238L AS order_id#11239L, (id#11238L % 1000) AS product_id#11241L, cast(((id#11238L % 5) + 1) as int) AS quantity#11243]
            :  +- PhotonRange Range (0, 5000000, step=1, splits=8)
            +- PhotonShuffleExchangeSource false
               +- PhotonShuffleMapStage EXECUTOR_BROADCAST, [id=#7447]
                  +- PhotonShuffleExchangeSink SinglePartition
                     +- PhotonProject [id#11245L AS product_id#11246L, concat(Product_, cast(id#11245L as string)) AS product_name#11248]
                        +- PhotonRange Range (0, 1000, step=1, splits=8)

== Photon Explanation ==
The query is fully supported by Photon.

----------------------------------------------------------------------------------------------------

# Experimento B
== Parsed Logical Plan ==
'Join UsingJoin(Inner, [product_id])
:- Project [order_id#11239L, product_id#11241L, cast(((order_id#11239L % cast(5 as bigint)) + cast(1 as bigint)) as int) AS quantity#11243]
:  +- Project [order_id#11239L, cast((order_id#11239L % cast(1000 as bigint)) as bigint) AS product_id#11241L]
:     +- Project [id#11238L AS order_id#11239L]
:        +- Range (0, 5000000, step=1, splits=Some(8))
+- UnresolvedHint broadcast
   +- Project [product_id#11246L, concat(Product_, cast(product_id#11246L as string)) AS product_name#11248]
      +- Project [id#11245L AS product_id#11246L]
         +- Range (0, 1000, step=1, splits=Some(8))

== Analyzed Logical Plan ==
product_id: bigint, order_id: bigint, quantity: int, product_name: string
Project [product_id#11241L, order_id#11239L, quantity#11243, product_name#11248]
+- Join Inner, (product_id#11241L = product_id#11246L)
   :- Project [order_id#11239L, product_id#11241L, cast(((order_id#11239L % cast(5 as bigint)) + cast(1 as bigint)) as int) AS quantity#11243]
   :  +- Project [order_id#11239L, cast((order_id#11239L % cast(1000 as bigint)) as bigint) AS product_id#11241L]
   :     +- Project [id#11238L AS order_id#11239L]
   :        +- Range (0, 5000000, step=1, splits=Some(8))
   +- ResolvedHint (strategy=broadcast)
      +- Project [product_id#11246L, concat(Product_, cast(product_id#11246L as string)) AS product_name#11248]
         +- Project [id#11245L AS product_id#11246L]
            +- Range (0, 1000, step=1, splits=Some(8))

== Optimized Logical Plan ==
Project [product_id#11241L, order_id#11239L, quantity#11243, product_name#11248]
+- Join Inner, (product_id#11241L = product_id#11246L), rightHint=(strategy=broadcast), joinId=20
   :- Project [id#11238L AS order_id#11239L, (id#11238L % 1000) AS product_id#11241L, cast(((id#11238L % 5) + 1) as int) AS quantity#11243]
   :  +- Range (0, 5000000, step=1, splits=Some(8))
   +- Project [id#11245L AS product_id#11246L, concat(Product_, cast(id#11245L as string)) AS product_name#11248]
      +- Range (0, 1000, step=1, splits=Some(8))

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Initial Plan ==
   PhotonResultStage
   +- PhotonColumnarToRow
      +- PhotonProject [product_id#11241L, order_id#11239L, quantity#11243, product_name#11248]
         +- PhotonBroadcastHashJoin [product_id#11241L], [product_id#11246L], Inner, BuildRight, false, true, false
            :- PhotonProject [id#11238L AS order_id#11239L, (id#11238L % 1000) AS product_id#11241L, cast(((id#11238L % 5) + 1) as int) AS quantity#11243]
            :  +- PhotonRange Range (0, 5000000, step=1, splits=8)
            +- PhotonShuffleExchangeSource false
               +- PhotonShuffleMapStage EXECUTOR_BROADCAST, [id=#7548]
                  +- PhotonShuffleExchangeSink SinglePartition
                     +- PhotonProject [id#11245L AS product_id#11246L, concat(Product_, cast(id#11245L as string)) AS product_name#11248]
                        +- PhotonRange Range (0, 1000, step=1, splits=8)


```

### Observações adicionais

No Broadcast Join, o lado grande permanece distribuído em partições, enquanto o lado pequeno é replicado integralmente para os executores, permitindo que o join seja realizado localmente sem redistribuir o lado grande pela chave do join.

