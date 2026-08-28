*Spark Engineering Lab #01 — Lazy Evaluation*

# Categoria

Spark Internals

# Objetivo

Demonstrar na prática que as transformações no Spark são avaliadas de forma lazy e que o processamento dos dados ocorre somente quando uma Action é executada.

# Pergunta

O Spark processa os dados imediatamente quando aplicamos transformações como `filter()` e `select()`?

# Experimento

DataFrame → `filter()` → `select()` → `show()`

Observar o comportamento do Spark antes e depois da execução da Action.

# Dados

Dados fictícios gerados com:

```python
spark.range(1_000_000)
```

# Transformações

Aplicação das transformações:

* `filter()` para selecionar registros com `id > 500000`;
* `select()` para selecionar a coluna `id`.

# Action

`show()`

A Action dispara a execução do processamento necessário para produzir o resultado.

# Comando de análise

`explain(True)`

Utilizado para observar o plano construído pelo Spark antes da execução da Action.

## Código

```python
df = spark.range(1_000_000)

df_filtered = df.filter("id > 500000")

df_selected = df_filtered.select("id")

df_selected.explain(True)

df_selected.show()
```

## Conclusão

As operações `filter()` e `select()` não processaram imediatamente os dados. O `explain(True)` evidenciou que essas transformações já estavam definidas no plano de execução do Spark. O processamento necessário para produzir o resultado ocorreu somente quando a Action `show()` foi chamada, demonstrando o comportamento de Lazy Evaluation.

== Parsed Logical Plan ==
'Project ['id]
+- 'Filter ('id > 500000)
   +- Range (0, 1000000, step=1, splits=Some(8))

== Analyzed Logical Plan ==
id: bigint
Project [id#11189L]
+- Filter (id#11189L > cast(500000 as bigint))
   +- Range (0, 1000000, step=1, splits=Some(8))

== Optimized Logical Plan ==
Filter (id#11189L > 500000)
+- Range (0, 1000000, step=1, splits=Some(8))

== Physical Plan ==
PhotonResultStage
+- PhotonColumnarToRow
   +- PhotonFilter (id#11189L > 500000)
      +- PhotonRange Range (0, 1000000, step=1, splits=8)

== Photon Explanation ==
The query is fully supported by Photon.
