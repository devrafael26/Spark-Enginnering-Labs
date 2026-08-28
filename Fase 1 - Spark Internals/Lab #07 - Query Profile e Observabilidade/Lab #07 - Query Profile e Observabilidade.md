*Spark Engineering Lab ##07 — Query Profile e Observabilidade*

## Categoria: Spark Internals

## Objetivo
Demonstrar como utilizar os recursos de observabilidade disponíveis no Databricks para analisar a execução de uma consulta Spark e seus recursos computacionais.

## Pergunta
Como podemos observar e analisar a execução de uma consulta Spark por meio do Query Profile?

## Experimento
DataFrame → `withColumn()` → `groupBy()` → `count()` → `show()` → `Query Text` → `explain(True)`

## Dados
Como os dados foram criados?

`spark.range(1_000_000)`

## Transformações
Quais operações foram realizadas?

Aplicação de `withColumn()`, `groupBy()` e `count()` (como agregação da transformação lógica).

## Action
Qual operação disparou a execução?

`df_grouped.show()`

## Comando de análise
`Query Text`

O `Query Text` apresenta a execução por meio de operadores e permite analisá-la sob três perspectivas: `Rows`, `Time Spent` e `Memory Peak`.

Quantas linhas passaram por cada operador?

```text
Range                1M
Grouping Aggregate   80
Shuffle              80
Grouping Aggregate   10
Columnar to Row      10
Limit                10
Result Query Stage   10
```

Quanto tempo foi associado a cada operador?

```text
Range                0ms
Grouping Aggregate   11ms
Shuffle              15ms
Grouping Aggregate   1ms
Columnar to Row      0ms
Limit                0ms
Result Query Stage   0ms
```

Qual foi o pico de memória associado a cada operador?

```text
Range                0 bytes
Grouping Aggregate   48MB
Shuffle              1.05GB
Grouping Aggregate   4MB
Columnar to Row      64,39KB
Limit                0 bytes
Result Query Stage   0 bytes
```

## Código

```python
df = spark.range(1_000_000)

df_grouped = (
    df
    .withColumn("group_id", df.id % 10)
    .groupBy("group_id")
    .count()
)

df_grouped.show()
df_grouped.explain(True)
```

## Conclusão
A execução do código por meio da Action `show()` permitiu utilizar o `Query Text` para observar os operadores envolvidos no processamento e analisá-los sob três perspectivas: quantidade de linhas (`Rows`), tempo associado aos operadores (`Time Spent`) e pico de memória (`Memory Peak`). No experimento, o `Range` iniciou com 1 milhão de registros distribuídos em 8 splits. A agregação parcial produziu 80 registros intermediários, compatíveis com os 10 valores possíveis de `group_id` em cada um dos 8 splits observados no experimento. Esses registros passaram pelo `Shuffle` e posteriormente foram consolidados pela agregação final em 10 grupos, correspondentes aos valores de `group_id` gerados pela expressão `id % 10`.
