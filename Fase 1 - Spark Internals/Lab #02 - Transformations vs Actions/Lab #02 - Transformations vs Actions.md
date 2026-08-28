*Spark Engineering Lab #02 — Transformations vs Actions*

## Categoria

Spark Internals

## Objetivo

Demonstrar a diferença entre Transformations e Actions no Spark e observar como diferentes Actions podem disparar execuções distintas sobre o mesmo conjunto de transformações.

## Pergunta

Qual é a diferença entre Transformations e Actions no Spark e como cada uma delas se relaciona com a execução?

## Experimento

DataFrame → `filter()` → `select()` → `join()` → `count()` / `show()` / `saveAsTable()`

Construir uma sequência de Transformations e, em seguida, executar diferentes Actions sobre o mesmo DataFrame resultante.

## Dados

Dados fictícios gerados com:

```python
spark.range()
```

Foram utilizados dois DataFrames:

```python
df_orders = spark.range(1_000_000)

df_customers = spark.range(500000, 600000)
```

## Transformações

Foram aplicadas operações como `filter()`, `select()` e `join()`.

Essas operações definem o processamento que o Spark deverá realizar, mas não solicitam imediatamente um resultado.

## Action

Foram utilizadas três Actions diferentes: `count()`, `show()` e operação de escrita com `saveAsTable()`.

## Comando de análise

Observação da execução gerada por cada Action no ambiente do Databricks.

## Código

```python
# Criação dos DataFrames
df_orders = spark.range(1_000_000)

df_customers = spark.range(500000, 600000)

# Transformations
df_filtered = df_orders.filter("id > 500000")

df_selected = df_filtered.select("id")

df_joined = (
    df_selected
    .join(
        df_customers,
        df_selected.id == df_customers.id,
        "inner"
    )
    .select(df_selected.id)
)

# Actions
df_joined.count()

df_joined.show(5)

df_joined.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("lab02_transformations_actions")
```

## Conclusão

Transformations como `filter()`, `select()` e `join()` constroem o processamento que o Spark deverá executar, enquanto Actions como `count()`, `show()` e `saveAsTable()` solicitam efetivamente um resultado e, consequentemente, desencadeiam a execução.

O experimento também mostrou, por meio das informações de execução apresentadas nos Statements/Performance gerados separadamente por cada Action, que diferentes Actions dispararam execuções separadas sobre o mesmo conjunto de Transformations, apresentando diferentes quantidades de tasks e durações.

```text
count() → 17/17 tasks, 1,370 s
show() → 16/16 tasks, 779 ms
saveAsTable() → 16/16 tasks, 14,553 s
