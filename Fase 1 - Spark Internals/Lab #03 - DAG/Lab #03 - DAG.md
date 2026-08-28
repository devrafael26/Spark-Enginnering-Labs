## *Spark Engineering Lab 03 — DAG — Directed Acyclic Graph*

## Categoria: Spark Internals

## Objetivo
Demonstrar como o Spark organiza as operações de uma consulta em uma DAG e observar as dependências entre os operadores após uma Action disparar a execução.

## Pergunta
Depois que uma Action dispara a execução, como o Spark organiza essas operações para executá-las?

## Experimento
Foi criado um DataFrame com 1 milhão de registros utilizando `spark.range()`. Sobre esse DataFrame foram aplicadas transformações para criação de grupos e agregação dos dados. A execução foi disparada com a Action `show()`, e o `Query Text` do Databricks foi utilizado para observar a estrutura da DAG e as dependências entre os operadores envolvidos na execução.

## Dados
Como os dados foram criados?

`spark.range(1_000_000)`

## Transformações
Quais operações foram realizadas?

Criação da coluna `group_id` utilizando `withColumn()`, `groupBy()` e `count()` para agregação.

## Action
Qual operação disparou a execução?

`df_result.show()`

## Comando de análise
`Hide Performance` / `Query Text`, utilizando a visualização apresentada em `Time Spent` para observar a estrutura da DAG e a relação entre os operadores.

## Código

```python
df = spark.range(1_000_000)

df_grouped = (
    df
    .withColumn("group_id", df.id % 10)
)

df_result = (
    df_grouped
    .groupBy("group_id")
    .count()
)

df_result.show()
```

## Conclusão
O `Query Text` da execução de `df_result.show()` permitiu observar os operadores envolvidos no processamento e as dependências entre eles. As Transformations constroem o lineage que representa as dependências entre as operações. Quando uma Action é chamada, o Spark utiliza esse lineage para construir e executar a DAG correspondente.
