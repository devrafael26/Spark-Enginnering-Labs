*Spark Engineering Lab ##05 — Catalyst Optimizer*

## Categoria: Spark Internals

## Objetivo
Demonstrar como o Catalyst Optimizer transforma o Logical Plan em um plano lógico otimizado, aplicando regras de otimização.

## Pergunta
Quais alterações o Catalyst Optimizer realiza no Logical Plan antes da execução?

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

O `explain(True)` não executa o processamento dos dados; ele solicita ao Spark a representação dos planos utilizados durante o planejamento da execução, permitindo comparar o Analyzed Logical Plan com o Optimized Logical Plan.

## Código

```python
df = spark.range(1_000_000)

df_filtered = df.filter("id > 500000")

df_selected = df_filtered.select("id")

df_selected.explain(True)
```

## Conclusão
A comparação entre o Analyzed Logical Plan e o Optimized Logical Plan mostrou que o Catalyst pode eliminar operações consideradas desnecessárias ou simplificar o plano antes da execução. No experimento, a operação `Project` presente no plano analisado não apareceu no plano otimizado, demonstrando uma transformação realizada pelo otimizador.
