## *Spark Engineering Lab 09 — Partitions*

## Categoria: Performance Engineering

## Objetivo

Investigar se o aumento do volume de dados altera a quantidade de splits utilizada pelo operador `Range` no ambiente do experimento.

## Pergunta
O aumento do volume de dados modifica a quantidade de splits utilizada pelo `spark.range()`?

## Experimento

5M → `explain(True)` → `splits=8`  
40M → `explain(True)` → `splits=8`  
300M → `explain(True)` → `splits=8`

## Dados
Como os dados foram criados?

`spark.range()` com 5M, 40M e 300M de registros.

## Transformações
Quais operações foram realizadas?

Não foram aplicadas transformações. O experimento utilizou diretamente os DataFrames criados com `spark.range()`.

## Action
Qual operação disparou a execução?

Não houve Action. O objetivo foi analisar o plano gerado pelo Spark.

## Comando de análise
`df_5m.explain(True)`, `df_40m.explain(True)` e `df_300m.explain(True)`

## Código

```python
df_5m = spark.range(5_000_000)
df_5m.explain(True)

df_40m = spark.range(40_000_000)
df_40m.explain(True)

df_300m = spark.range(300_000_000)
df_300m.explain(True)
```

## Conclusão

Nos experimentos realizados, o operador `Range` utilizou 8 splits para todos os volumes testados. Mesmo com o aumento de 5 milhões para 300 milhões de registros, a quantidade de splits permaneceu a mesma. Portanto, neste ambiente e para esse operador, o aumento do volume de dados não provocou alteração automática no número de splits.

O resultado indica que, nesse caso, a quantidade de splits está relacionada ao paralelismo definido pelo ambiente (`defaultParallelism` do `SparkContext`), e não diretamente ao número de registros informado no `Range`.
