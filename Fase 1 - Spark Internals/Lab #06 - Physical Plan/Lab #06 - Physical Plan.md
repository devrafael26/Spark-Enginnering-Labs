*Spark Engineering Lab ##06 — Physical Plan*

## Categoria: Spark Internals

## Objetivo
Observar como o Spark transforma o Logical Plan otimizado em um Physical Plan composto por operadores físicos de execução.

## Pergunta
Como o Spark transforma o plano lógico otimizado em execução física?

## Experimento
DataFrame → `filter()` → `select()` → `explain(True)`

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

O `explain(True)` apresentou o Physical Plan com operadores como `PhotonRange`, `PhotonFilter` e `PhotonColumnarToRow`. O plano físico representa as operações concretas utilizadas pelo mecanismo de execução para processar o plano lógico.

## Código

```python
df = spark.range(1_000_000)

df_filtered = df.filter("id > 500000")

df_selected = df_filtered.select("id")

df_selected.explain(True)
```

## Conclusão
O experimento demonstrou que, após o planejamento e otimização do Logical Plan, o Spark gera um Physical Plan composto por operadores concretos de execução. No ambiente utilizado, esses operadores foram apresentados com implementação Photon. O Physical Plan representa, portanto, a estratégia física escolhida para executar as operações definidas no código.
