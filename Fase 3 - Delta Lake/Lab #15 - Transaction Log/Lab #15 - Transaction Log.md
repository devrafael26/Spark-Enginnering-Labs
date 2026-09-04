## *Spark Engineering Lab 15 — Transaction Log*

## Categoria

Delta Lake

## Objetivo

Entender como o Delta Lake registra as alterações realizadas em uma tabela através do Transaction Log e como essas alterações podem ser observadas pelo histórico da tabela e, quando há acesso baseado em path, pelos arquivos físicos armazenados no diretório `_delta_log`.

O laboratório também busca diferenciar uma tabela Delta gerenciada pelo Unity Catalog de uma tabela Delta criada diretamente em um caminho dentro de um Volume.

## Pergunta

Como o Delta Lake registra as alterações realizadas em uma tabela e como podemos observar o histórico dessas operações?

## Experimento

### Experimento A — Managed Table e `DESCRIBE HISTORY`

```text
DataFrame
    ↓
saveAsTable()
    ↓
Managed Delta Table
    ↓
INSERT
    ↓
UPDATE
    ↓
DELETE
    ↓
DESCRIBE HISTORY
```

Foi criada uma tabela Delta utilizando `saveAsTable()`, registrando a tabela no catálogo e schema ativos da sessão.

Após a criação, foram executadas diferentes operações sobre a tabela:

* criação/gravação inicial;
* `INSERT`;
* `UPDATE`;
* `DELETE`.

Após cada alteração foi utilizado `DESCRIBE HISTORY` para observar as versões e operações registradas.

---

### Experimento B — Inspeção física do Transaction Log

#### Parte 1 — Tentativa de acesso ao `_delta_log` da Managed Table

```text
Managed Delta Table
    ↓
DESCRIBE DETAIL
    ↓
tentativa de obter location
    ↓
dbutils.fs.ls(.../_delta_log)
    ↓
acesso não permitido
```

Foi utilizado:

```sql
DESCRIBE DETAIL
```

para consultar os metadados da tabela.

Entre as informações apresentadas estavam:

* `format = delta`;
* identificador da tabela;
* nome;
* data de criação;
* última modificação;
* quantidade de arquivos;
* tamanho em bytes;
* propriedades;
* versões mínimas de reader e writer;
* table features;
* clustering columns;
* `clusterByAuto = false`.

No ambiente utilizado, o campo `location` não apresentou um caminho físico utilizável.

Foi realizada uma tentativa de acessar diretamente:

```text
_delta_log
```

utilizando `dbutils.fs.ls()`.

A operação retornou erro relacionado ao acesso ao DBFS.

A documentação atual do Databricks também estabelece que tabelas gerenciadas pelo Unity Catalog devem ser acessadas pelo identificador da tabela:

```text
catalog.schema.table
```

e que o acesso baseado em path às Managed Tables do Unity Catalog não é suportado.

Portanto, o acesso físico ao `_delta_log` não representa a forma normal de interação com uma Managed Table governada pelo Unity Catalog.

---

#### Parte 2 — Delta Table baseada em path dentro de um Volume

Para permitir a inspeção física do Transaction Log foi criado um Volume no Unity Catalog.

```text
Unity Catalog
    ↓
Volume
    ↓
Path
    ↓
Delta Table
    ↓
arquivos de dados
+
_delta_log
```

Dentro desse Volume foi definida uma path e os dados foram gravados utilizando:

```python
.write.format("delta").save(path)
```

Diferentemente do `saveAsTable()`, essa operação criou uma tabela Delta baseada em path, sem registrá-la como uma tabela nomeada no catálogo.

Foram realizadas novamente operações de:

```text
WRITE
  ↓
INSERT
  ↓
UPDATE
  ↓
DELETE
```

O histórico foi consultado utilizando a própria path Delta.

Em seguida, o diretório:

```text
_delta_log
```

foi acessado utilizando `dbutils.fs.ls()`.

A operação apresentou os arquivos existentes no Transaction Log, incluindo arquivos com nomes sequenciais semelhantes a:

```text
00000000000000000000.json
00000000000000000000.crc

00000000000000000001.json
00000000000000000001.crc

00000000000000000002.json
00000000000000000002.crc
...
```

Os arquivos JSON representam commits/versões sucessivos do Transaction Log.

Cada commit registra ações relacionadas àquela versão específica da tabela. O histórico completo é formado pela sequência dos commits, e não pela repetição de todo o histórico dentro do JSON mais recente.

# Dados

Os dados foram criados artificialmente utilizando `spark.range()`.

Foram gerados 100.000 registros e adicionadas as colunas:

```text
id
customer_id
amount
status
```

A expressão:

```python
id % 1000
```

foi utilizada para gerar 1.000 valores possíveis de `customer_id`.

A expressão:

```python
id % 500
```

foi utilizada para gerar valores artificiais para `amount`.

O uso do módulo não é uma exigência do `spark.range()`. Ele foi utilizado apenas como uma forma simples e previsível de gerar dados sintéticos para o laboratório.

# Transformações

Durante a preparação dos dados foram realizadas:

* criação de `customer_id`;
* criação de `amount`;
* criação da coluna `status`.

Após a materialização em Delta, foram realizadas operações que modificaram o estado da tabela:

* `WRITE`;
* `INSERT`;
* `UPDATE`;
* `DELETE`.

## Operações executadas

Nesta fase, o campo `Action` utilizado nos laboratórios anteriores foi substituído por `Operações executadas`, pois os experimentos de Delta Lake envolvem operações de tabela e não apenas Actions de DataFrame.

Foram executadas:

```text
WRITE
INSERT
UPDATE
DELETE
```

Cada operação que modificou a tabela produziu uma nova versão no histórico Delta.

## Comandos de análise

### DESCRIBE HISTORY

Utilizado para observar o histórico de versões da tabela:

```sql
DESCRIBE HISTORY
```

No experimento foram observadas, em ordem cronológica inversa, operações como:

```text
DELETE
UPDATE
WRITE
CREATE OR REPLACE TABLE
```

O `DESCRIBE HISTORY` apresenta informações como:

* `version`;
* `timestamp`;
* `operation`;
* `operationParameters`;
* `operationMetrics`;
* usuário responsável pela operação.

---

### DESCRIBE DETAIL

Utilizado para observar os metadados da Managed Table:

```sql
DESCRIBE DETAIL
```

Entre os metadados observados estavam formato, quantidade de arquivos, tamanho, propriedades, recursos da tabela e informações de clustering.

---

### dbutils.fs.ls()

Utilizado para listar fisicamente o conteúdo do `_delta_log` da tabela Delta criada dentro do Volume:

```python
dbutils.fs.ls(f"{path}/_delta_log")
```

A saída apresentou informações como:

```text
path
name
size
modificationTime
```

permitindo observar os arquivos físicos que compõem o Transaction Log.

## Código

### Experimento A — Managed Table

```python
from pyspark.sql import functions as F

table_name = "lab15_transaction_log"

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

df = (
    spark.range(100_000)
    .withColumn("customer_id", F.col("id") % 1000)
    .withColumn(
        "amount",
        (F.col("id") % 500 + 10).cast("double")
    )
    .withColumn(
        "status",
        F.lit("ACTIVE")
    )
)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)
```

#### Histórico após a criação

```python
display(
    spark.sql(f"""
        DESCRIBE HISTORY {table_name}
    """)
)
```

#### INSERT

```python
spark.sql(f"""
    INSERT INTO {table_name}
    VALUES
        (100000, 1000, 250.0, 'ACTIVE'),
        (100001, 1001, 300.0, 'ACTIVE'),
        (100002, 1002, 450.0, 'ACTIVE')
""")
```

#### UPDATE

```python
spark.sql(f"""
    UPDATE {table_name}
    SET status = 'INACTIVE'
    WHERE customer_id < 10
""")
```

#### DELETE

```python
spark.sql(f"""
    DELETE FROM {table_name}
    WHERE id < 100
""")
```

#### Histórico final

```python
display(
    spark.sql(f"""
        DESCRIBE HISTORY {table_name}
    """)
)
```

#### Metadados da tabela

```python
display(
    spark.sql(f"""
        DESCRIBE DETAIL {table_name}
    """)
)
```

#### Tentativa de obter a localização

```python
detail = spark.sql(f"""
    DESCRIBE DETAIL {table_name}
""")

location = detail.select("location").first()["location"]

print(repr(location))
```

#### Tentativa de acesso direto ao `_delta_log`

```python
display(
    dbutils.fs.ls(f"{location}/_delta_log")
)
```

No ambiente utilizado, essa tentativa não permitiu acessar o Transaction Log físico da Managed Table.

---

### Experimento B — Delta Table em Volume

#### Criação do Volume

```python
spark.sql("""
    CREATE VOLUME IF NOT EXISTS
    workspace.default.lab15_volume
""")
```

#### Definição da path

```python
path = (
    "/Volumes/workspace/default/"
    "lab15_volume/transaction_log"
)
```

#### Criação da Delta Table baseada em path

```python
from pyspark.sql import functions as F

df = (
    spark.range(100_000)
    .withColumn("customer_id", F.col("id") % 1000)
    .withColumn(
        "amount",
        (F.col("id") % 500 + 10).cast("double")
    )
    .withColumn(
        "status",
        F.lit("ACTIVE")
    )
)

(
    df.write
    .format("delta")
    .mode("overwrite")
    .save(path)
)
```

#### INSERT

```python
spark.sql(f"""
    INSERT INTO delta.`{path}`
    VALUES
        (100000, 1000, 250.0, 'ACTIVE'),
        (100001, 1001, 300.0, 'ACTIVE'),
        (100002, 1002, 450.0, 'ACTIVE')
""")
```

#### UPDATE

```python
spark.sql(f"""
    UPDATE delta.`{path}`
    SET status = 'INACTIVE'
    WHERE customer_id < 10
""")
```

#### DELETE

```python
spark.sql(f"""
    DELETE FROM delta.`{path}`
    WHERE id < 100
""")
```

#### Histórico

```python
display(
    spark.sql(f"""
        DESCRIBE HISTORY delta.`{path}`
    """)
)
```

#### Inspeção do `_delta_log`

```python
display(
    dbutils.fs.ls(f"{path}/_delta_log")
)
```

## Observações

### Managed Table x Delta Table baseada em path

O laboratório permitiu observar duas formas diferentes de trabalhar com Delta.

#### Managed Table

Criada utilizando:

```python
.saveAsTable(...)
```

Nesse caso, os dados são armazenados em Delta e a tabela é registrada no catálogo.

Ela é normalmente acessada utilizando:

```text
catalog.schema.table
```

Quando o nome completo não é informado no `saveAsTable()`, são utilizados o catálogo e o schema ativos da sessão.

Para tabelas gerenciadas pelo Unity Catalog, o próprio Unity Catalog administra a localização física dos arquivos.

---

#### Delta Table baseada em path

Criada utilizando:

```python
.save(path)
```

Nesse caso, existe uma tabela Delta persistida naquele caminho, com arquivos de dados e Transaction Log, mas ela não é automaticamente registrada como uma tabela nomeada no catálogo.

O acesso ocorre utilizando a path:

```text
delta.`/Volumes/...`
```

O Volume utilizado no experimento é um objeto governado pelo Unity Catalog, mas a Delta Table armazenada dentro dele continua sendo acessada pelo caminho.

---

### Unity Catalog

O Unity Catalog funciona como a camada de governança e organização dos objetos.

Uma Managed Table normalmente é acessada através de:

```text
catalog.schema.table
```

Já os arquivos armazenados dentro de um Volume são acessados por:

```text
/Volumes/catalog/schema/volume/...
```

Para novas tabelas tabulares no Databricks, Managed Tables do Unity Catalog representam o modelo recomendado.

O uso do Volume neste laboratório teve principalmente uma finalidade didática: permitir descer um nível e observar diretamente os arquivos físicos do Transaction Log.

---

### Transaction Log

O `_delta_log` é parte fundamental de uma Delta Table.

Cada commit gera uma nova versão da tabela.

Os arquivos JSON sequenciais representam esses commits.

Um JSON de uma versão não contém obrigatoriamente todo o histórico anterior. Ele registra as ações daquele commit.

O histórico da tabela é formado pela sequência das versões:

```text
versão 0
   ↓
versão 1
   ↓
versão 2
   ↓
versão 3
   ↓
...
```

A partir dessa sequência, o Delta consegue determinar o estado da tabela em determinada versão.

## Conclusão

O laboratório demonstrou que alterações realizadas sobre uma tabela Delta são registradas como versões sucessivas no Transaction Log.

No primeiro experimento, uma Managed Table foi criada com `saveAsTable()` e posteriormente modificada através de `INSERT`, `UPDATE` e `DELETE`. O `DESCRIBE HISTORY` permitiu observar essas operações no histórico, com as versões mais recentes sendo apresentadas primeiro.

Também foi observado que uma Managed Table governada pelo Unity Catalog é normalmente acessada pelo identificador `catalog.schema.table`. O acesso baseado em path ao armazenamento físico de Managed Tables não representa a forma suportada de interação com esse tipo de objeto.

Para observar diretamente o Transaction Log, foi criado um Volume no Unity Catalog e, dentro dele, uma Delta Table baseada em path. Nesse cenário foi possível acessar o diretório `_delta_log` e visualizar os arquivos sequenciais que representam os commits da tabela.

Dessa forma, os dois experimentos mostraram o mesmo mecanismo por perspectivas diferentes:

```text
Managed Table
    ↓
DESCRIBE HISTORY
    ↓
visão de alto nível das versões

Delta por path
    ↓
_delta_log
    ↓
visão física dos commits
```

O `DESCRIBE HISTORY` representa a forma mais direta de consultar o histórico de uma tabela durante o uso normal no Databricks, enquanto a inspeção do `_delta_log` permitiu entender de forma mais detalhada o mecanismo que sustenta esse versionamento.

# Referências oficiais

* Databricks — Managed Tables no Unity Catalog
  https://docs.databricks.com/aws/en/tables/managed

* Databricks — Path rules and access in Unity Catalog Volumes
  https://docs.databricks.com/aws/en/volumes/paths

* Databricks — Work with table history
  https://docs.databricks.com/aws/en/tables/history

* Databricks — Table history schema and operation metrics
  https://docs.databricks.com/gcp/en/tables/history-schema





















Observado no Experimento A: a tabela Delta foi criada como managed table e seu histórico pôde ser consultado normalmente por DESCRIBE HISTORY. A tentativa de acessar diretamente seu _delta_log não foi possível pelo caminho utilizado.

Documentação: managed tables do Unity Catalog devem ser acessadas pelo identificador catalog.schema.table; o acesso baseado em path não é suportado. Além disso, DBFS root e mounts são recursos depreciados.

Experimento B: foi criado um Unity Catalog Volume, que oferece acesso baseado em path. Dentro dele foram gravados dados no formato Delta, permitindo inspecionar diretamente o diretório _delta_log.
