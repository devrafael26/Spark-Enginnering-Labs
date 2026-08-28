## *Spark Engineering Lab #04 — Logical Plan*

## Categoria: Spark Internals

## Objetivo
Observar como o Spark representa logicamente as operações realizadas sobre um DataFrame.
O `explain(True)` passa a ser a ferramenta principal do Lab ##04.

## Pergunta
Como o Spark representa logicamente as operações que eu escrevi?

## Experimento
DataFrame + `filter()` + `select()` + `explain(True)`.

## Dados
Como os dados foram criados?

`spark.range(1_000_000)`

## Transformações
Quais operações foram realizadas?

Aplicação de `filter()` e `select()`.

## Action
Qual operação disparou a execução?

Não se aplica. O experimento não utiliza uma Action para executar os dados.

## Comando de análise
`df_selected.explain(True)`

O `explain(True)` não executa o processamento dos dados; ele solicita ao Spark a representação dos planos utilizados durante o planejamento da execução.

## Código

```python
df = spark.range(1_000_000)

df_filtered = df.filter("id > 500000")

df_selected = df_filtered.select("id")

df_selected.explain(True)
```

## Conclusão
Através da saída do `explain(True)`, podemos visualizar as diferentes etapas do planejamento, evidenciando que o código PySpark não é executado diretamente como foi escrito, mas sim transformado em uma representação que o Spark pode analisar, otimizar e executar.

```text
== Parsed Logical Plan ==
'Project ['id]
+- 'Filter ('id > 500000)
   +- Range (0, 1000000, step=1, splits=Some(8))

== Analyzed Logical Plan ==
id: bigint
Project [id#11178L]
+- Filter (id#11178L > cast(500000 as bigint))
   +- Range (0, 1000000, step=1, splits=Some(8))

== Optimized Logical Plan ==
Filter (id#11178L > 500000)
+- Range (0, 1000000, step=1, splits=Some(8))

== Physical Plan ==
PhotonResultStage
+- PhotonColumnarToRow
   +- PhotonFilter (id#11178L > 500000)
      +- PhotonRange Range (0, 1000000, step=1, splits=8)
