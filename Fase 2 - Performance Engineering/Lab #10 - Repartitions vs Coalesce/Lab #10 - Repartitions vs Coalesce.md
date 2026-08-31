## *Spark Engineering Lab 10 - Repartitions vs Coalesce*

## Categoria: Performance Engineering

## Objetivo

Demonstrar como `repartition()` e `coalesce()` alteram o particionamento dos dados e observar as diferenças entre as estratégias utilizadas pelo Spark.

## Pergunta

Qual é a diferença entre `repartition()` e `coalesce()` ao alterar o número de partições de um DataFrame?

## Experimento

`DataFrame → repartition(16) → explain(True)`

`DataFrame → repartition(4) → explain(True)`

`DataFrame → coalesce(4) → explain(True)`

`DataFrame → coalesce(16) → explain(True)`

## Dados

Como os dados foram criados?

`spark.range()` com 1 milhão de registros.

## Transformações

Quais operações foram realizadas?

Aplicação de `repartition()` para aumentar e reduzir o número de partições e aplicação de `coalesce()` para reduzir o número de partições, comparando o comportamento apresentado no plano de execução.

## Action

Qual operação disparou a execução?

Não foi utilizada uma Action para análise do laboratório. O comportamento das operações foi observado por meio do plano gerado pelo `explain(True)`.

## Comando de análise

`explain(True)`

Utilizado para observar:

* o número inicial de splits do operador `Range`;
* a presença de `ShuffleExchange` nas operações com `repartition()`;
* o `RoundRobinPartitioning(N)` utilizado na redistribuição;
* o operador `Coalesce N`;
* a ausência de Shuffle completo na redução com `coalesce()`.

## Código

```python
df = spark.range(1_000_000)

# Aumentando o número de partições
df_repartition_16 = df.repartition(16)
df_repartition_16.explain(True)

# Reduzindo o número de partições com repartition
df_repartition_4 = df.repartition(4)
df_repartition_4.explain(True)

# Reduzindo o número de partições com coalesce
df_coalesce_4 = df.coalesce(4)
df_coalesce_4.explain(True)

# Tentando aumentar o número de partições com coalesce
df_coalesce_16 = df.coalesce(16)
df_coalesce_16.explain(True)
```

## Conclusão

Ao executar `repartition(16)`, podemos observar que o Spark inicia o processamento com `splits=8`, pois essa é a divisão utilizada pelo operador `Range`, criado pelo `spark.range()` neste ambiente. O `repartition(16)` não altera os splits de origem do Range; ele cria uma nova distribuição posteriormente.

No plano físico, essa redistribuição pode ser identificada pelo `PhotonShuffleExchangeSink RoundRobinPartitioning(16)`, mostrando que ocorreu um Shuffle para redistribuir os dados em 16 partições.

Ao alterar para `repartition(4)`, observamos o mesmo comportamento: o `Range` continua com `splits=8`, mas ocorre novamente um Shuffle, agora com `RoundRobinPartitioning(4)`, redistribuindo os dados em 4 partições. Isso mostra que `repartition()` pode ser utilizado tanto para aumentar quanto para diminuir o número de partições.

Quando utilizamos `coalesce(4)`, o comportamento é diferente. No plano físico aparece o operador `Coalesce 4`, mas não aparece um `ShuffleExchange`. Nesse caso, o Spark reduz de 8 para 4 partições aproveitando a estrutura das partições existentes, sem realizar uma redistribuição completa dos registros.

Por não realizar Shuffle, `coalesce()` pode ter um custo menor quando o objetivo é apenas reduzir o número de partições. Porém, ele não tem como objetivo reequilibrar os dados entre as novas partições, podendo manter desequilíbrios já existentes. Já o `repartition()`, por realizar a redistribuição dos registros, tem um custo maior, mas permite uma nova distribuição dos dados entre as partições de destino.

O `coalesce()` é utilizado para redução de partições. Caso seja solicitado um número maior que o número atual de partições, o DataFrame permanece com a quantidade atual, pois `coalesce()` não realiza o Shuffle para aumentar o particionamento.

Dessa forma, a escolha entre as duas operações depende do objetivo: `coalesce()` é adequado quando queremos apenas reduzir a quantidade de partições aproveitando a estrutura existente, enquanto `repartition()` é utilizado quando precisamos aumentar partições ou quando queremos realizar uma nova redistribuição dos dados.

```text
== Parsed Logical Plan ==
Repartition 16, true, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Analyzed Logical Plan ==
id: bigint
Repartition 16, true, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Optimized Logical Plan ==
Repartition 16, true, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Initial Plan ==
   PhotonResultStage
   +- PhotonColumnarToRow
      +- PhotonShuffleExchangeSource false
         +- PhotonShuffleMapStage REPARTITION_BY_NUM, [id=#7331]
            +- PhotonShuffleExchangeSink RoundRobinPartitioning(16)
               +- PhotonRange Range (0, 1000000, step=1, splits=8)

== Photon Explanation ==
The query is fully supported by Photon.

--------------------------------------------------------------------------

== Parsed Logical Plan ==
Repartition 4, true, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Analyzed Logical Plan ==
id: bigint
Repartition 4, true, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Optimized Logical Plan ==
Repartition 4, true, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- == Initial Plan ==
   PhotonResultStage
   +- PhotonColumnarToRow
      +- PhotonShuffleExchangeSource false
         +- PhotonShuffleMapStage REPARTITION_BY_NUM, [id=#7365]
            +- PhotonShuffleExchangeSink RoundRobinPartitioning(4)
               +- PhotonRange Range (0, 1000000, step=1, splits=8)

== Photon Explanation ==
The query is fully supported by Photon.

--------------------------------------------------------------------------

== Parsed Logical Plan ==
Repartition 4, false, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Analyzed Logical Plan ==
id: bigint
Repartition 4, false, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Optimized Logical Plan ==
Repartition 4, false, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Physical Plan ==
Coalesce 4
+- *(1) ColumnarToRow
   +- PhotonResultStage
      +- PhotonRange Range (0, 1000000, step=1, splits=8)

== Photon Explanation ==
Photon does not fully support the query because:
		Unsupported node: Coalesce 4.

Reference node:
	Coalesce 4

--------------------------------------------------------------------------

== Parsed Logical Plan ==
Repartition 16, false, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Analyzed Logical Plan ==
id: bigint
Repartition 16, false, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Optimized Logical Plan ==
Repartition 16, false, false, 0, Unknown
+- Range (0, 1000000, step=1, splits=Some(8))

== Physical Plan ==
Coalesce 16
+- *(1) ColumnarToRow
   +- PhotonResultStage
      +- PhotonRange Range (0, 1000000, step=1, splits=8)

== Photon Explanation ==
Photon does not fully support the query because:
		Unsupported node: Coalesce 16.

Reference node:
	Coalesce 16
```

### Observações adicionais

Segundo a documentação oficial do Databricks, `repartition()` pode receber apenas o número de partições ou também uma ou mais colunas. Quando colunas são informadas, elas participam da estratégia de particionamento e o DataFrame resultante é particionado por hash.



COALESCE
* 8 → 4
* aproveita partições existentes
* não faz Shuffle completo
* mais barato
* não garante reequilíbrio

REPARTITION
* 8 → Shuffle → 4
* redistribui os registros
* mais caro
* pode produzir uma distribuição mais equilibrada
