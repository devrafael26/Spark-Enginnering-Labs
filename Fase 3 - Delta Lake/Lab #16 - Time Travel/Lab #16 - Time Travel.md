## *Spark Engineering Lab #16 — Time Travel*

## Categoria

Delta Lake

## Objetivo

Entender como o Delta Lake mantém diferentes versões de uma tabela e como o recurso de Time Travel permite consultar estados anteriores dos dados utilizando o número da versão ou um timestamp.

O laboratório também busca relacionar o Time Travel ao Transaction Log estudado no Lab #15, mostrando que as versões registradas no histórico podem ser utilizadas para acessar snapshots anteriores da tabela.

## Pergunta

Como o Delta Lake permite consultar estados anteriores de uma tabela utilizando seu histórico de versões?

## Experimento

O experimento foi dividido em três etapas.

### Experimento A — Criação da versão inicial

```text
DataFrame
        ↓
3 produtos
        ↓
write.format("delta")
        ↓
saveAsTable()
        ↓
Tabela Delta
        ↓
versão inicial
````

Foi criado um DataFrame contendo três produtos e seus respectivos preços.

Em seguida, os dados foram gravados como uma tabela Delta utilizando `saveAsTable()`.

Essa gravação criou o estado inicial da tabela, que passou a fazer parte de seu histórico de versões.

### Experimento B — Alteração dos dados

Após a criação da tabela, foram realizadas duas operações sobre ela:

```text
Tabela inicial
        ↓
UPDATE
        ↓
alteração do preço do Notebook
        ↓
nova versão
        ↓
DELETE
        ↓
remoção do Mouse
        ↓
nova versão
```

Primeiro foi executado um `UPDATE`, alterando o preço do produto Notebook.

Depois foi executado um `DELETE`, removendo o produto Mouse.

Cada alteração gerou um novo estado da tabela registrado em seu histórico.

### Experimento C — Consulta do histórico e Time Travel

Após as alterações foi consultado o histórico da tabela:

```text
DESCRIBE HISTORY
        ↓
version
timestamp
operation
        ↓
consulta da tabela atual
        ↓
VERSION AS OF
        ↓
consulta de versões anteriores
        ↓
TIMESTAMP AS OF
        ↓
consulta do estado da tabela
em determinado momento
```

Foram então comparados:

* o estado atual da tabela;
* versões anteriores utilizando `VERSION AS OF`;
* um estado anterior utilizando `TIMESTAMP AS OF`.

O objetivo foi verificar que a mesma tabela pode ser consultada em diferentes estados históricos.

## Dados

Os dados utilizados foram fictícios e criados diretamente com `spark.createDataFrame()`:

```python
df = spark.createDataFrame([
    (1, "Notebook", 3500.00),
    (2, "Monitor", 1200.00),
    (3, "Mouse", 150.00)
], ["produto_id", "produto", "preco"])
```

Estado inicial dos dados:

```text
produto_id | produto  | preco
-----------|----------|-------
1          | Notebook | 3500
2          | Monitor  | 1200
3          | Mouse    | 150
```

Os dados foram gravados na tabela:

```text
workspace.default.lab16_time_travel
```

utilizando o formato Delta.

## Transformações

### Criação da tabela

O DataFrame inicial foi persistido como uma tabela Delta:

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.lab16_time_travel")
```

Neste ponto, o DataFrame `df` foi utilizado apenas como fonte para a criação da tabela.

Após a gravação, o objeto `df` e a tabela Delta devem ser tratados como objetos diferentes.

Alterações posteriores realizadas diretamente sobre a tabela, como `UPDATE` e `DELETE`, não modificam o DataFrame original criado anteriormente.

Para observar o estado atualizado da tabela foi realizada uma nova leitura:

```python
display(
    spark.table("workspace.default.lab16_time_travel")
)
```

### UPDATE

Foi alterado o preço do Notebook:

```python
spark.sql("""
    UPDATE workspace.default.lab16_time_travel
    SET preco = 3900
    WHERE produto_id = 1
""")
```

O estado da tabela passou de:

```text
Notebook = 3500
```

para:

```text
Notebook = 3900
```

### DELETE

Em seguida foi removido o produto Mouse:

```python
spark.sql("""
    DELETE FROM workspace.default.lab16_time_travel
    WHERE produto_id = 3
""")
```

O estado atual passou a conter apenas:

```text
Notebook = 3900
Monitor  = 1200
```

Mesmo após o `UPDATE` e o `DELETE`, estados anteriores puderam ser consultados por meio do Time Travel.

## Action

Neste laboratório não foi utilizada uma única Action como elemento central da análise, como ocorreu em alguns laboratórios anteriores de Spark Internals e Performance Engineering.

As operações principais foram comandos que modificaram ou consultaram diretamente a tabela Delta:

```text
saveAsTable()
UPDATE
DELETE
SELECT
DESCRIBE HISTORY
```

O foco do laboratório não foi analisar a execução física de uma Action, mas acompanhar a evolução do estado da tabela e utilizar o histórico mantido pelo Delta Lake.

## Comando de análise

O principal comando utilizado para analisar o histórico foi:

```python
display(
    spark.sql("""
        DESCRIBE HISTORY workspace.default.lab16_time_travel
    """)
)
```

O `DESCRIBE HISTORY` permite observar informações como:

```text
version
timestamp
operation
```

As operações são apresentadas da mais recente para a mais antiga.

Dessa forma foi possível identificar os diferentes estados criados durante o experimento e utilizar os números das versões e timestamps nas consultas de Time Travel.

### Observação sobre operações OPTIMIZE

Durante o experimento também foram observadas entradas `OPTIMIZE` no `DESCRIBE HISTORY`, embora nenhum comando `OPTIMIZE` tenha sido executado manualmente no código do laboratório.

Essas entradas representam operações de otimização registradas separadamente no histórico da tabela e não devem ser interpretadas como se `UPDATE` ou `DELETE` fossem automaticamente transformados em `OPTIMIZE`.

O experimento permite afirmar que o ambiente executou e registrou operações `OPTIMIZE`.

A Databricks possui mecanismos capazes de executar otimizações automaticamente em determinadas condições. Entretanto, apenas a presença da operação no histórico não foi utilizada neste laboratório para determinar qual mecanismo específico a disparou.

https://docs.databricks.com/aws/en/optimizations/predictive-optimization

## Time Travel por versão

A consulta ao estado atual foi realizada com:

```python
display(
    spark.table("workspace.default.lab16_time_travel")
)
```

Para acessar uma versão anterior foi utilizado:

```python
display(
    spark.sql("""
        SELECT *
        FROM workspace.default.lab16_time_travel
        VERSION AS OF 1
    """)
)
```

Também foi consultada a versão inicial:

```python
display(
    spark.sql("""
        SELECT *
        FROM workspace.default.lab16_time_travel
        VERSION AS OF 0
    """)
)
```

Conceitualmente:

```text
versão inicial
        ↓
UPDATE
        ↓
nova versão
        ↓
DELETE
        ↓
nova versão

              ↑
              │
      VERSION AS OF
              │
      consulta estados
         anteriores
```

O número utilizado em `VERSION AS OF` corresponde a uma versão registrada no histórico da tabela.

## Time Travel por timestamp

Além do número da versão, o Delta Lake permite consultar a tabela pelo timestamp associado ao histórico.

Primeiro foi utilizado:

```python
display(
    spark.sql("""
        DESCRIBE HISTORY workspace.default.lab16_time_travel
    """)
)
```

A partir do valor da coluna `timestamp`, um estado anterior pode ser consultado com:

```python
display(
    spark.sql("""
        SELECT *
        FROM workspace.default.lab16_time_travel
        TIMESTAMP AS OF '<timestamp>'
    """)
)
```

Exemplo:

```python
display(
    spark.sql("""
        SELECT *
        FROM workspace.default.lab16_time_travel
        TIMESTAMP AS OF '2026-09-04 14:00:00'
    """)
)
```

O timestamp utilizado deve corresponder ao período histórico que se deseja consultar.

A diferença conceitual entre as duas formas é:

```text
VERSION AS OF
        ↓
consulta utilizando
uma versão específica


TIMESTAMP AS OF
        ↓
consulta o estado
da tabela naquele
ponto no tempo
```


## Relação entre Time Travel e VACUUM

O Time Travel depende da disponibilidade do histórico da tabela e dos arquivos de dados necessários para reconstruir determinada versão.

Arquivos que deixaram de fazer parte do estado atual podem continuar armazenados durante o período de retenção. Após serem removidos pelo `VACUUM`, versões que dependam desses arquivos podem deixar de estar disponíveis para Time Travel.

O aprofundamento sobre `VACUUM` será realizado no Lab #18.

Documentação oficial:
https://docs.databricks.com/aws/en/delta/vacuum


## Código

```python
# ============================================================
# Limpeza da tabela para reiniciar o laboratório
# ============================================================

spark.sql("""
    DROP TABLE IF EXISTS workspace.default.lab16_time_travel
""")


# ============================================================
# Experimento A — Criação da versão inicial
# ============================================================

df = spark.createDataFrame([
    (1, "Notebook", 3500.00),
    (2, "Monitor", 1200.00),
    (3, "Mouse", 150.00)
], ["produto_id", "produto", "preco"])


df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.lab16_time_travel")


# Estado inicial da tabela
display(
    spark.table("workspace.default.lab16_time_travel")
)


## ============================================================
## Experimento B — Alterações na tabela
## ============================================================

# UPDATE
spark.sql("""
    UPDATE workspace.default.lab16_time_travel
    SET preco = 3900
    WHERE produto_id = 1
""")


# Estado após UPDATE
display(
    spark.table("workspace.default.lab16_time_travel")
)


# DELETE
spark.sql("""
    DELETE FROM workspace.default.lab16_time_travel
    WHERE produto_id = 3
""")


# Estado após DELETE
display(
    spark.table("workspace.default.lab16_time_travel")
)


## ============================================================
## Experimento C — Histórico e Time Travel
## ============================================================

#### Histórico da tabela
display(
    spark.sql("""
        DESCRIBE HISTORY workspace.default.lab16_time_travel
    """)
)


#### Estado atual
display(
    spark.table("workspace.default.lab16_time_travel")
)


### Time Travel por versão

display(
    spark.sql("""
        SELECT *
        FROM workspace.default.lab16_time_travel
        VERSION AS OF 1
    """)
)


display(
    spark.sql("""
        SELECT *
        FROM workspace.default.lab16_time_travel
        VERSION AS OF 0
    """)
)


## ============================================================
## Time Travel por timestamp
## ============================================================


#### Consultar primeiro o DESCRIBE HISTORY e substituir
#### o timestamp abaixo por um timestamp observado no histórico.

display(
    spark.sql("""
        DESCRIBE HISTORY workspace.default.lab16_time_travel
    """)
)

display(
    spark.sql("""
        SELECT *
        FROM workspace.default.lab16_time_travel
        TIMESTAMP AS OF '2026-09-04T18:01:43'
    """)
)
```

## Conclusão

O laboratório demonstrou que uma tabela Delta não representa apenas seu estado atual.

As operações realizadas sobre a tabela criam diferentes versões registradas em seu histórico, permitindo consultar estados anteriores dos dados por meio do recurso de Time Travel.

O fluxo observado foi:

```text
criação da tabela
        ↓
estado inicial
        ↓
UPDATE
        ↓
novo estado
        ↓
DELETE
        ↓
novo estado
        ↓
DESCRIBE HISTORY
        ↓
histórico de versões
        ↓
Time Travel
```

Por meio de `DESCRIBE HISTORY`, foi possível identificar as versões, timestamps e operações realizadas sobre a tabela.

Com:

```sql
VERSION AS OF
```

foi possível escolher diretamente uma versão anterior.

Com:

```sql
TIMESTAMP AS OF
```

foi possível consultar o estado correspondente a determinado ponto no tempo.

O experimento também reforçou a relação entre este laboratório e o Lab #15.

No Lab #15 foi estudado que o Transaction Log registra as alterações realizadas sobre uma tabela Delta.

Neste laboratório foi observado como esse histórico de versões pode ser utilizado para acessar estados anteriores da tabela.

A relação pode ser resumida como:

```text
Transaction Log
        ↓
histórico das alterações
        ↓
versões da tabela
        ↓
Time Travel
        ↓
consulta de estados anteriores
```

Outro ponto observado foi que o DataFrame utilizado inicialmente para criar a tabela e a própria tabela Delta não são o mesmo objeto.

Após:

```python
saveAsTable()
```

operações executadas diretamente sobre a tabela, como `UPDATE` e `DELETE`, alteram seu estado persistido, mas não modificam o DataFrame original utilizado como fonte.

Por isso, após uma alteração na tabela, seu novo estado deve ser consultado novamente por meio de uma leitura da própria tabela.

### Observações adicionais

* Cada operação que modifica uma tabela Delta cria uma nova versão da tabela.

* O `DESCRIBE HISTORY` apresenta o histórico em ordem cronológica inversa, ou seja, começando pelas operações mais recentes.

* O Time Travel pode utilizar tanto o número da versão quanto um timestamp.

* `VERSION AS OF` permite indicar diretamente qual versão histórica deve ser consultada.

* `TIMESTAMP AS OF` permite consultar o estado que correspondia a determinado ponto no tempo.

* Time Travel realiza uma consulta de um estado histórico; ele não significa, por si só, que a tabela atual foi restaurada para aquela versão.

* Durante o experimento foram observadas operações `OPTIMIZE` no histórico que não haviam sido executadas explicitamente no código. Elas foram registradas como operações independentes pelo ambiente.

* A existência de uma versão no histórico não significa que ela estará necessariamente disponível para Time Travel indefinidamente.

* Para consultar uma versão histórica, devem permanecer disponíveis tanto as informações do log quanto os arquivos de dados necessários para representar aquele estado.

* Arquivos que deixaram de fazer parte do estado atual da tabela podem continuar armazenados durante o período de retenção, permitindo a consulta de versões anteriores.

* O `VACUUM` remove arquivos que não são mais referenciados pelas versões mantidas dentro da janela de retenção.

* A propriedade `delta.deletedFileRetentionDuration`, utilizada pelo `VACUUM`, possui por padrão uma retenção de 7 dias.

* O histórico do log possui uma retenção própria, controlada por `delta.logRetentionDuration`, cujo padrão é 30 dias.

* Portanto, a possibilidade de realizar Time Travel depende da disponibilidade conjunta do histórico e dos arquivos necessários para reconstruir aquela versão.

* Após o `VACUUM` remover os arquivos necessários para determinado estado histórico, aquele estado pode deixar de estar disponível para Time Travel.

* O aprofundamento sobre funcionamento, retenção e execução do `VACUUM` será realizado no Lab #18.

* O histórico da tabela e o Time Travel não devem ser tratados como uma solução de backup de longo prazo.













