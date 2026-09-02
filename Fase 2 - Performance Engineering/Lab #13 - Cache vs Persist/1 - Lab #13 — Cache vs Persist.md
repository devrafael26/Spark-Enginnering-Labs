# Spark Engineering Lab #13 — Cache vs Persist

## Categoria

Performance Engineering

## Objetivo

Entender como Cache e Persist podem ser utilizados no Apache Spark para evitar recomputações de um mesmo conjunto de transformações quando um DataFrame é reutilizado, além de observar as limitações desses mecanismos no Databricks Serverless e a utilização de uma tabela Delta como alternativa para materialização de resultados intermediários.

## Pergunta

Como Cache e Persist evitam recomputações no Spark e como a reutilização de resultados pode ser tratada no Databricks Serverless?

## Experimento

### Experimento A — Recomputação sem persistência

`DataFrame → criação de group_id → agregação → Action count() → Action show() → Query Profile → explain(True)`

O mesmo DataFrame foi utilizado em duas Actions diferentes sem aplicação de Cache ou Persist, permitindo observar se o processamento necessário para produzir o resultado seria executado novamente.

### Experimento B — Cache e Persist no Databricks Serverless

`DataFrame → cache() → erro`

`DataFrame → persist() → erro`

Foram testadas as operações `cache()` e `persist()` para verificar seu comportamento no ambiente Databricks Serverless.

Nos dois casos, o ambiente retornou erro informando que a operação de persistência não é suportada em Serverless Compute.

### Experimento C — Materialização do resultado em Delta

`DataFrame → criação de group_id → agregação → write Delta → leitura da tabela → count() → show() → Query Profile → explain(True)`

O resultado intermediário foi materializado em uma tabela Delta. Após a gravação, novas Actions foram executadas a partir da tabela criada, permitindo comparar esse comportamento com a recomputação observada no Experimento A.

## Dados

Os dados foram criados artificialmente utilizando:

```python
spark.range(5_000_000)
```

O `Range` gerou 5 milhões de registros contendo inicialmente a coluna `id`.

A coluna `group_id` foi criada utilizando:

```python
F.col("id") % 100
```

Dessa forma, os registros foram distribuídos entre 100 valores possíveis de `group_id`, de `0` a `99`.

## Transformações

Foram realizadas as seguintes transformações:

* criação da coluna `group_id` com `withColumn()`;
* agrupamento dos registros por `group_id`;
* contagem dos registros de cada grupo;
* soma dos valores da coluna `id`;
* ordenação decrescente pela coluna `quantidade` no resultado exibido com `show()`.

O DataFrame principal foi construído da seguinte forma:

```python
df = (
    spark.range(5_000_000)
    .withColumn("group_id", F.col("id") % 100)
)

df_resultado = (
    df
    .groupBy("group_id")
    .agg(
        F.count("*").alias("quantidade"),
        F.sum("id").alias("soma_id")
    )
)
```

## Action

### Experimento A

Foram utilizadas duas Actions sobre o mesmo DataFrame:

```python
df_resultado.count()
```

e:

```python
df_resultado.orderBy(F.desc("quantidade")).show()
```

As duas Actions geraram execuções distintas.

Na Action `count()`, o Query Profile apresentou operadores como:

```text
Range
↓
Grouping Aggregate
↓
Shuffle
↓
Grouping Aggregate
↓
Aggregate
↓
Columnar to Row
↓
Result Query Stage
```

Na Action `show()`, o processamento também iniciou novamente no `Range`:

```text
Range
↓
Grouping Aggregate
↓
Shuffle
↓
Grouping Aggregate
↓
Top K
↓
Columnar to Row
↓
Result Query Stage
```

O operador `Top K` apareceu devido à combinação da ordenação por `quantidade DESC` com a quantidade limitada de registros retornados pelo `show()`.

O fato de as duas execuções iniciarem novamente no `Range` mostrou que reutilizar o mesmo objeto DataFrame não significa que o resultado da primeira execução tenha sido automaticamente armazenado.

O lineage foi reutilizado, mas o resultado computado não foi persistido.

### Experimento B

Foram testadas:

```python
df_cache.cache()
```

e:

```python
df_persist.persist(StorageLevel.MEMORY_AND_DISK)
```

As duas operações retornaram erro no Databricks Serverless, informando que a persistência de tabela não é suportada nesse tipo de compute.

O resultado observado confirma que, embora Cache e Persist façam parte dos mecanismos de persistência do Apache Spark, essas APIs não podem ser utilizadas nesse ambiente Serverless.

### Experimento C

A primeira Action foi a materialização do resultado em uma tabela Delta:

```python
df_resultado.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lab13_cache_persist_resultado")
```

Essa Action executou o lineage necessário para produzir `df_resultado` e gravou o resultado.

Depois, foi criado um novo DataFrame a partir da tabela:

```python
df_delta = spark.table("lab13_cache_persist_resultado")
```

Essa operação apenas definiu um novo DataFrame apontando para a tabela e não constituiu uma Action.

Em seguida foram executadas:

```python
df_delta.count()
```

e:

```python
df_delta.orderBy(F.desc("quantidade")).show()
```

No `count()`, a execução foi concluída, porém o Databricks apresentou:

```text
Query profile is not available
```

A documentação do Databricks informa que consultas atendidas pelo Query Cache podem não disponibilizar Query Profile. No experimento, foi observada apenas a indisponibilidade do perfil, portanto não foi possível determinar exclusivamente por essa mensagem qual mecanismo causou esse comportamento.

Na Action `show()`, o Query Profile apresentou:

```text
Scan Table
↓
Top K
↓
Columnar to Row
↓
Result Query Stage
```

Diferentemente do Experimento A, a execução não iniciou novamente no `Range` e não repetiu as etapas utilizadas para produzir o resultado original.

## Comando de análise

Foram utilizados:

```python
df_resultado.explain(True)
```

e:

```python
df_delta.explain(True)
```

além do Query Profile disponibilizado pelo Databricks para as Actions executadas.

### Experimento A — Physical Plan

O plano apresentou:

```text
PhotonRange
↓
PhotonProject
↓
PhotonGroupingAgg (partial)
↓
PhotonShuffleExchangeSink
↓
PhotonShuffleExchangeSource
↓
PhotonGroupingAgg (final)
↓
PhotonColumnarToRow
↓
PhotonResultStage
```

O Shuffle foi realizado utilizando:

```text
hashpartitioning(group_id, 38)
```

O `Range` foi criado com:

```text
splits=8
```

### Experimento C — Physical Plan

Após a materialização, o plano passou a iniciar pela leitura da tabela:

```text
PhotonResultStage
+- PhotonColumnarToRow
   +- PhotonScan parquet workspace.default.lab13_cache_persist_resultado
```

O plano também apresentou:

```text
PreparedDeltaFileIndex
```

A tabela foi criada no formato Delta, enquanto os dados físicos da tabela são armazenados em arquivos Parquet. Por isso o operador de leitura física aparece como:

```text
PhotonScan parquet
```

A presença do `PreparedDeltaFileIndex` está associada à leitura da estrutura Delta utilizada pela tabela.

## Código

### Experimento A — Recomputação sem persistência

```python
from pyspark.sql import functions as F

df = (
    spark.range(5_000_000)
    .withColumn("group_id", F.col("id") % 100)
)

df_resultado = (
    df
    .groupBy("group_id")
    .agg(
        F.count("*").alias("quantidade"),
        F.sum("id").alias("soma_id")
    )
)

df_resultado.count()

df_resultado.orderBy(F.desc("quantidade")).show()

df_resultado.explain(True)
```

### Experimento B1 — Cache

```python
df_cache = (
    spark.range(5_000_000)
    .withColumn("group_id", F.col("id") % 100)
    .groupBy("group_id")
    .count()
)

df_cache.cache()
```

### Experimento B2 — Persist

```python
from pyspark import StorageLevel

df_persist = (
    spark.range(5_000_000)
    .withColumn("group_id", F.col("id") % 100)
    .groupBy("group_id")
    .count()
)

df_persist.persist(StorageLevel.MEMORY_AND_DISK)
```

### Experimento C — Materialização em Delta

```python
from pyspark.sql import functions as F

df = (
    spark.range(5_000_000)
    .withColumn("group_id", F.col("id") % 100)
)

df_resultado = (
    df
    .groupBy("group_id")
    .agg(
        F.count("*").alias("quantidade"),
        F.sum("id").alias("soma_id")
    )
)

df_resultado.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lab13_cache_persist_resultado")

df_delta = spark.table("lab13_cache_persist_resultado")

df_delta.count()

df_delta.orderBy(F.desc("quantidade")).show()

df_delta.explain(True)
```

## Conclusão

No Experimento A, o mesmo DataFrame foi utilizado em duas Actions diferentes sem aplicação de persistência. Tanto o `count()` quanto o `show()` apresentaram novamente no Query Profile o processamento iniciado pelo `Range`, passando pelas etapas de agregação e Shuffle. Isso mostrou que reutilizar o mesmo DataFrame não significa reutilizar automaticamente o resultado já calculado, pois o Spark pode executar novamente o lineage necessário para atender cada Action.

Cache e Persist são mecanismos do Apache Spark utilizados justamente para evitar esse tipo de recomputação quando um resultado intermediário será reutilizado. O `cache()` utiliza o nível de armazenamento padrão, enquanto o `persist()` permite definir explicitamente um `StorageLevel`, possibilitando maior controle sobre a estratégia de armazenamento.

No Experimento B, porém, tanto `cache()` quanto `persist()` retornaram erro no Databricks Serverless, informando que operações de persistência desse tipo não são suportadas nesse ambiente.

No Experimento C, o resultado intermediário foi então materializado em uma tabela Delta. A primeira Action foi o `write`, responsável por executar o processamento original e gravar seu resultado. Nas Actions posteriores, o Query Profile e o Physical Plan passaram a apresentar a leitura por meio de `Scan Table` e `PhotonScan`, sem repetir o `Range`, as agregações e o Shuffle utilizados para construir o resultado original.

O laboratório mostrou, portanto, três situações diferentes: sem persistência, o mesmo conjunto de transformações pode ser novamente processado por Actions diferentes; Cache e Persist são mecanismos do Spark para reutilização de resultados intermediários, mas não estão disponíveis no Databricks Serverless utilizado no experimento; e, nesse ambiente, a materialização do resultado em uma tabela Delta permite que consultas posteriores partam diretamente do dado já produzido, evitando a reconstrução do lineage original.


### Observação adicional

Em um ambiente Databricks que suporte `cache()` e `persist()`, após a primeira Action materializar o DataFrame persistido, as Actions seguintes podem reutilizar o resultado armazenado em vez de reconstruir todo o lineage original.

No Physical Plan, essa reutilização pode ser identificada pelo operador `InMemoryTableScan`, conforme apresentado na documentação do Databricks para `cache()` e `persist()`.

No Query Profile, a execução também deverá refletir a leitura do resultado persistido, porém o nome exibido pela interface pode variar e não deve ser assumido obrigatoriamente como `InMemoryTableScan`.
