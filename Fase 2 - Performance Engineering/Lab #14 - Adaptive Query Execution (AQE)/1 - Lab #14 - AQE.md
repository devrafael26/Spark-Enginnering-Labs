## *Spark Engineering Lab ##14 — Adaptive Query Execution (AQE)*

## Categoria

Performance Engineering

## Objetivo

Entender como o Adaptive Query Execution (AQE) pode adaptar a execução de uma consulta utilizando estatísticas obtidas em runtime, observando duas formas de atuação:

- ajuste dinâmico do número de partições após um Shuffle;
- mudança dinâmica da estratégia física de Join.

## Pergunta

Como o AQE utiliza informações obtidas durante a execução para adaptar o processamento de uma consulta no Spark?

## Experimento

### Experimento A — Coalescing de partições pós-Shuffle

```text
spark.range(5.000.000)
        ↓
criação da coluna key_id
        ↓
id % 100
        ↓
groupBy("key_id")
        ↓
count()
        ↓
explain(True)
        ↓
collect()
        ↓
Query Profile
        ↓
análise do Shuffle
```

O experimento foi estruturado para provocar uma operação de Shuffle por meio de uma agregação utilizando `groupBy`.

Primeiro foi analisado o plano físico antes da execução. Em seguida, a Action `collect()` foi utilizada para efetivamente executar a consulta e permitir que o AQE trabalhasse com estatísticas reais da execução.

Após a execução, o Query Profile foi utilizado para comparar o número de partições planejadas para o Shuffle com o número de partições efetivamente utilizadas na leitura pós-Shuffle.

### Experimento B — Mudança dinâmica da estratégia de Join

```text
df_sales
10.000.000 registros
        +
df_items
5.000.000 registros
        ↓
filtro em df_items
price_group == 0
        ↓
~5.000 registros no lado direito
        ↓
Inner Join por item_id
        ↓
explain(True)
        ↓
PhotonShuffledHashJoin
        ↓
count()
        ↓
Query Profile
        ↓
Photon Broadcast Hash
```

O experimento foi criado com dois DataFrames inicialmente grandes.

No lado direito do Join foi aplicado um filtro que reduziu significativamente a quantidade de registros antes da execução do Join.

Nenhum hint de `broadcast` ou `merge` foi utilizado, deixando a escolha da estratégia física para o Spark e para o AQE.

O objetivo foi comparar a estratégia inicialmente planejada no Physical Plan com a estratégia efetivamente utilizada durante a execução e apresentada no Query Profile.

## Dados

### Experimento A

Os dados foram criados com:

```python
spark.range(5_000_000)
```

Foram gerados 5 milhões de registros contendo inicialmente a coluna `id`.

Em seguida foi criada a coluna `key_id`:

```python
.withColumn("key_id", F.col("id") % 100)
```

A expressão `id % 100` produz 100 valores possíveis para `key_id`:

```text
0, 1, 2, ..., 99
```

Dessa forma, os 5 milhões de registros foram distribuídos entre 100 chaves.

Como a distribuição é uniforme neste experimento, cada chave possui:

```text
50.000 registros
```

O resultado final da agregação contém 100 linhas, uma para cada valor de `key_id`.

### Experimento B

Foi criado o DataFrame `df_sales`:

```python
df_sales = (
    spark.range(10_000_000)
    .withColumnRenamed("id", "sale_id")
    .withColumn(
        "item_id",
        F.col("sale_id") % 5_000_000
    )
)
```

Foram gerados 10 milhões de registros.

Também foi criado o DataFrame `df_items`:

```python
df_items = (
    spark.range(5_000_000)
    .withColumnRenamed("id", "item_id")
    .withColumn(
        "price_group",
        F.col("item_id") % 1000
    )
)
```

Foram gerados 5 milhões de registros.

A expressão `item_id % 1000` produz 1.000 valores possíveis para `price_group`.

Em seguida foi aplicado:

```python
.filter(F.col("price_group") == 0)
```

reduzindo o lado direito do Join para aproximadamente 5 mil registros.

No Query Profile foram observados aproximadamente 10 mil registros no lado esquerdo e 5 mil registros no lado direito na entrada do operador de Join.

## Transformações

### Experimento A

Foram realizadas as seguintes transformações:

```python
.withColumn("key_id", F.col("id") % 100)
```

Criação da coluna utilizada como chave de agrupamento.

```python
.groupBy("key_id")
.count()
```

Agregação dos registros por `key_id`.

O plano físico mostrou uma agregação parcial antes do Shuffle e uma agregação final depois dele.

Fluxo observado:

```text
PhotonRange
        ↓
PhotonProject
        ↓
PhotonGroupingAgg
partial_count
        ↓
PhotonShuffleExchange
        ↓
PhotonGroupingAgg
finalmerge_count
```

A agregação parcial reduz significativamente a quantidade de dados que precisa ser movimentada pelo Shuffle.

Apesar da origem possuir 5 milhões de registros, os registros são parcialmente agregados por `key_id` antes da redistribuição.

### Experimento B

No lado esquerdo foi criada a coluna `item_id`:

```python
.withColumn(
    "item_id",
    F.col("sale_id") % 5_000_000
)
```

No lado direito foi criada a coluna `price_group`:

```python
.withColumn(
    "price_group",
    F.col("item_id") % 1000
)
```

e aplicado o filtro:

```python
.filter(F.col("price_group") == 0)
```

Depois foi realizado o Join:

```python
df_sales.join(
    df_items_filtered,
    on="item_id",
    how="inner"
)
```

Nenhum hint foi utilizado.

No Optimized Logical Plan, foi observado um Filter também no ramo esquerdo do Join, embora o filtro tenha sido definido originalmente apenas no lado direito. Isso mostra que o Catalyst inferiu uma condição equivalente a partir da igualdade da chave item_id e a aplicou ao lado esquerdo durante a otimização.

## Action

### Experimento A

Foi utilizada a Action:

```python
df_agg.collect()
```

O `collect()` força a execução do plano e retorna todas as linhas resultantes do DataFrame para o driver.

Neste experimento, o resultado contém apenas 100 linhas, correspondentes aos 100 valores possíveis de `key_id`.

A Action foi utilizada principalmente para efetivamente executar a consulta e permitir que o AQE utilizasse estatísticas reais produzidas durante o processamento.

### Experimento B

Foi utilizada a Action:

```python
resultado = df_join.count()
```

O `count()` força a execução do Join e retorna a quantidade total de registros resultantes.

A execução permitiu comparar a estratégia inicialmente apresentada no Physical Plan com o algoritmo efetivamente apresentado no Query Profile.

## Comando de análise

Foram utilizados:

```python
df_agg.explain(True)
```

e o:

```text
Query Profile
```

#### Consulta das configurações do AQE

Também foram testados:

```python
spark.conf.get("spark.sql.adaptive.enabled")
```

e:

```python
spark.conf.get(
    "spark.sql.adaptive.coalescePartitions.enabled"
)
```

No ambiente Databricks Serverless utilizado no laboratório, as configurações não estavam disponíveis para consulta e retornaram erro informando que a configuração não estava disponível.

Dessa forma, a análise da atuação do AQE foi realizada a partir do plano físico e das métricas reais apresentadas no Query Profile.

#### Plano físico antes da execução

O plano apresentou:

```text
AdaptiveSparkPlan isFinalPlan=false
```

e:

```text
PhotonShuffleExchangeSink
hashpartitioning(key_id, 38)
```

O trecho:

```text
hashpartitioning(key_id, 38)
```

indica que o Shuffle foi inicialmente planejado utilizando 38 partições de destino.

Também foi observado:

```text
PhotonRange
Range (0, 5000000, step=1, splits=8)
```

mostrando que o `Range` foi criado com 8 splits nesta execução.

#### Plano após a Action

Após executar:

```python
df_agg.collect()
```

foi chamado novamente:

```python
df_agg.explain(True)
```

Entretanto, o plano continuou apresentando:

```text
AdaptiveSparkPlan isFinalPlan=false
```

e:

```text
== Initial Plan ==
```

mantendo também:

```text
hashpartitioning(key_id, 38)
```

Portanto, no ambiente Databricks Serverless com Photon utilizado no laboratório, o `explain()` não apresentou o plano final adaptado após a execução.

O campo:

```text
isFinalPlan=false
```

não significa que o AQE estava desabilitado.

Ele indica que o `AdaptiveSparkPlan` exibido naquele momento não estava sendo apresentado como plano final da execução.

#### Query Profile

Após a execução do `collect()`, foi analisado o operador de Shuffle no Query Profile.

Foi observado:

```text
Shuffle Read Number of Partitions = 1
```

Enquanto o plano inicial apresentava:

```text
hashpartitioning(key_id, 38)
```

o Query Profile mostrou que a leitura pós-Shuffle ocorreu utilizando apenas:

```text
1 partição
```

O comportamento observado foi:

```text
Plano inicial

Shuffle
hashpartitioning(key_id, 38)

        ↓

execução

        ↓

AQE

        ↓

Query Profile

Shuffle Read Number of Partitions = 1
```


### Experimento B — Plano físico inicial

O plano físico apresentou:

```text
AdaptiveSparkPlan isFinalPlan=false
```

e a estratégia inicialmente escolhida para o Join foi:

```text
PhotonShuffledHashJoin
BuildRight
```

O plano mostrou Shuffle nos dois lados do Join.

Lado esquerdo:

```text
PhotonShuffleExchangeSink
hashpartitioning(item_id, 114)
```

Lado direito:

```text
PhotonShuffleExchangeSink
hashpartitioning(item_id, 114)
```

Portanto, os dois lados foram redistribuídos pela chave `item_id`, inicialmente em 114 partições.

A estrutura simplificada do plano é:

```text
PhotonShuffledHashJoin
├── lado esquerdo
│   └── Shuffle
│       └── hashpartitioning(item_id, 114)
│
└── lado direito
    └── Shuffle
        └── hashpartitioning(item_id, 114)
```

O `BuildRight` não representa a ordem de execução. Ele indica que, no `ShuffledHashJoin`, o lado direito foi escolhido para construção da estrutura hash.

### Experimento B — Query Profile

Após executar:

```python
resultado = df_join.count()
```

o operador `Inner Join` apresentou no Query Profile:

```text
Join algorithm
Photon Broadcast Hash
```

e:

```text
Build side
Right
```

Portanto, houve diferença entre a estratégia apresentada no plano físico inicial e a estratégia efetivamente apresentada durante a execução.

O comportamento observado foi:

```text
Plano inicial

PhotonShuffledHashJoin
BuildRight

        ↓

execução

        ↓

AQE utiliza estatísticas
obtidas em runtime

        ↓

Query Profile

Photon Broadcast Hash
Build side = Right
```

A documentação oficial da Databricks exemplifica a mudança dinâmica de `SortMergeJoin` para `BroadcastHashJoin` como uma das atuações do AQE.

Neste experimento realizado em Databricks Serverless com Photon, porém, o plano inicial apresentou `PhotonShuffledHashJoin`, enquanto o Query Profile apresentou `Photon Broadcast Hash`.

Dessa forma, o comportamento experimental foi registrado separadamente do exemplo específico apresentado pela documentação.

## Código

```python
from pyspark.sql import functions as F


# ============================================================
# Experimento A — Coalescing de partições pós-Shuffle
# ============================================================

df = (
    spark.range(5_000_000)
    .withColumn(
        "key_id",
        F.col("id") % 100
    )
)

df_agg = (
    df
    .groupBy("key_id")
    .count()
)

# Plano antes da execução
df_agg.explain(True)

# Action
df_agg.collect()

# Plano após a execução
df_agg.explain(True)


# ============================================================
# Experimento B — Mudança dinâmica da estratégia de Join
# ============================================================

df_sales = (
    spark.range(10_000_000)
    .withColumnRenamed("id", "sale_id")
    .withColumn(
        "item_id",
        F.col("sale_id") % 5_000_000
    )
)

df_items = (
    spark.range(5_000_000)
    .withColumnRenamed("id", "item_id")
    .withColumn(
        "price_group",
        F.col("item_id") % 1000
    )
)

df_items_filtered = (
    df_items
    .filter(F.col("price_group") == 0)
)

df_join = (
    df_sales
    .join(
        df_items_filtered,
        on="item_id",
        how="inner"
    )
)

# Plano físico inicial
df_join.explain(True)

# Action
resultado = df_join.count()

print(resultado)
```

## Conclusão

Os experimentos demonstraram duas formas diferentes de atuação do Adaptive Query Execution durante a execução das consultas.

### Experimento A — Coalescing pós-Shuffle

O plano físico inicial mostrou:

```text
AdaptiveSparkPlan isFinalPlan=false
```

e o Shuffle foi planejado utilizando:

```text
hashpartitioning(key_id, 38)
```

Após a execução da Action `collect()`, o Query Profile mostrou:

```text
Shuffle Read Number of Partitions = 1
```

Isso indica que, apesar de o Shuffle ter sido inicialmente planejado com 38 partições, a etapa seguinte realizou a leitura utilizando apenas uma partição.

O comportamento observado é compatível com o **post-shuffle partition coalescing do AQE**, no qual o Spark utiliza estatísticas obtidas durante a execução para combinar partições pequenas de Shuffle e evitar o processamento de várias partições com pouco volume de dados.

Neste experimento, apesar da fonte possuir 5 milhões de registros, uma agregação parcial ocorreu antes do Shuffle. Como existem apenas 100 valores possíveis de `key_id`, o volume de dados intermediários movimentado após essa agregação foi significativamente menor que o volume original.

Com base nas estatísticas reais dessa saída de Shuffle, o AQE considerou desnecessário manter as 38 partições inicialmente planejadas e a leitura pós-Shuffle ocorreu utilizando apenas uma partição.

O `explain()` não apresentou o plano final adaptado no ambiente Databricks Serverless com Photon utilizado no laboratório, permanecendo com:

```text
AdaptiveSparkPlan isFinalPlan=false
```

e:

```text
== Initial Plan ==
```

Por isso, a adaptação efetivamente ocorrida durante a execução foi identificada por meio do Query Profile.

### Experimento B — Mudança dinâmica da estratégia de Join

O plano físico inicial apresentou:

```text
PhotonShuffledHashJoin
BuildRight
```

com Shuffle nos dois lados utilizando:

```text
hashpartitioning(item_id, 114)
```

Após a execução da Action `count()`, o Query Profile apresentou no operador `Inner Join`:

```text
Join algorithm
Photon Broadcast Hash
```

e:

```text
Build side
Right
```

Portanto, a estratégia física observada durante a execução foi diferente da estratégia inicialmente apresentada no plano.

O lado direito, reduzido pelo filtro para aproximadamente 5 mil registros, permaneceu como lado de construção e passou a ser utilizado em uma estratégia de Broadcast Hash Join.

A documentação oficial da Databricks exemplifica como atuação do AQE a mudança dinâmica de `SortMergeJoin` para `BroadcastHashJoin` quando estatísticas obtidas em runtime mostram que um dos lados é pequeno o suficiente para Broadcast.

No ambiente Databricks Serverless com Photon utilizado neste laboratório, o comportamento observado foi:

```text
PhotonShuffledHashJoin
        ↓
Photon Broadcast Hash
```

Dessa forma, o laboratório demonstra experimentalmente uma mudança da estratégia física de Join durante a execução para Broadcast Hash Join, mantendo separado o comportamento observado do exemplo específico `SortMergeJoin → BroadcastHashJoin` apresentado pela documentação oficial.

### Conclusão geral

Os dois experimentos mostram o princípio central do AQE:

```text
planejamento inicial
        ↓
execução
        ↓
estatísticas reais de runtime
        ↓
AQE
        ↓
adaptação da execução
```

No Experimento A, a adaptação ocorreu sobre o número de partições utilizadas após o Shuffle:

```text
38 partições planejadas
        ↓
1 partição de leitura
```

No Experimento B, a adaptação ocorreu sobre a estratégia física do Join:

```text
PhotonShuffledHashJoin
        ↓
Photon Broadcast Hash
```

Assim, o laboratório demonstrou que o AQE pode utilizar informações disponíveis durante a execução para rever decisões do plano físico e adequar o processamento às características reais dos dados.

#### Observações adicionais

* `AdaptiveSparkPlan isFinalPlan=false` não significa que o AQE está desabilitado.
* O `AdaptiveSparkPlan` indica que a consulta está sendo executada dentro do mecanismo de Adaptive Query Execution.
* O campo `isFinalPlan` indica se o plano apresentado corresponde ou não ao plano final da execução.
* O AQE utiliza estatísticas obtidas em runtime para adaptar determinadas decisões do plano físico.
* Uma das otimizações realizadas pelo AQE é o coalescing de partições após Shuffle.
* Outra atuação do AQE é a mudança dinâmica da estratégia física de Join.
* No Experimento A, foram planejadas 38 partições de Shuffle, enquanto a leitura pós-Shuffle utilizou apenas 1.
* O número de partições pós-Shuffle não precisa ser igual ao número inicialmente planejado no `hashpartitioning`.
* O AQE não realizou um novo `repartition(1)`. O mecanismo agrupou as partições de Shuffle para que a etapa seguinte pudesse processá-las através de uma quantidade menor de partições de leitura.
* A decisão de coalescing considera o volume real de dados produzido pelo Shuffle, e não apenas a quantidade de registros existentes originalmente no DataFrame.
* No Experimento B, o plano inicial apresentou `PhotonShuffledHashJoin`.
* O `PhotonShuffledHashJoin` possuía Shuffle nos dois lados do Join pela chave `item_id`.
* Os dois lados apresentaram `hashpartitioning(item_id, 114)` no plano inicial.
* `BuildRight` indica que o lado direito foi escolhido para construção da estrutura hash; não representa a ordem cronológica de execução dos operadores.
* O plano físico é apresentado como uma árvore, com o operador pai acima dos seus ramos de entrada.
* Após a Action `count()`, o Query Profile apresentou `Photon Broadcast Hash` como algoritmo do `Inner Join`.
* O Query Profile também apresentou `Build side = Right`.
* O lado direito havia sido reduzido pelo filtro para aproximadamente 5 mil registros.
* A documentação oficial da Databricks exemplifica a adaptação de Join do AQE através da conversão `SortMergeJoin → BroadcastHashJoin`.
* No experimento realizado com Photon foi observado `PhotonShuffledHashJoin → Photon Broadcast Hash`.
* Essa diferença foi registrada como comportamento observado no experimento, sem assumir que a documentação descreve explicitamente essa conversão específica.
* No ambiente Serverless utilizado, as configurações `spark.sql.adaptive.enabled` e `spark.sql.adaptive.coalescePartitions.enabled` não estavam disponíveis para consulta através de `spark.conf.get()`.
* No ambiente utilizado, o `explain()` permaneceu apresentando `AdaptiveSparkPlan isFinalPlan=false` e `Initial Plan`, mesmo após a Action no Experimento A.
* Por isso, o Query Profile foi essencial para observar as adaptações efetivamente realizadas pelo AQE nos dois experimentos.
