## *Spark Engineering Lab 17 — Merge*

## Categoria

Delta Lake

## Objetivo

Entender como a operação `MERGE` do Delta Lake permite aplicar alterações de uma fonte de dados sobre uma tabela de destino, atualizando registros existentes e inserindo novos registros.

O laboratório também demonstra um cenário em que a tabela de origem e a tabela de destino possuem schemas diferentes, utilizando uma `TEMP VIEW` como camada intermediária de adaptação.

---

## Pergunta

Como o Delta Lake utiliza `MERGE` para atualizar registros existentes e inserir novos registros em uma tabela de destino quando a origem possui um schema diferente?

---

## Experimento

O experimento simula uma carga incremental de clientes.

O fluxo utilizado foi:

```text
SOURCE (Delta Table)
        ↓
TEMP VIEW (Schema Adapter)
        ↓
TARGET (Delta Table)
```

A tabela de origem representa os dados recebidos de um sistema externo.

A `TEMP VIEW` adapta o schema da origem para o formato esperado pela tabela de destino.

A operação `MERGE` compara os registros da fonte preparada com a tabela de destino utilizando `customer_id` como chave.

Quando o cliente já existe, seus dados são atualizados.

Quando o cliente ainda não existe, um novo registro é inserido.

---

## Dados

### Source

A tabela de origem possui um schema diferente da tabela de destino:

```text
client_id
full_name
new_city
new_status
ingestion_timestamp
```

Foram utilizados dois registros:

```text
client_id = 1
Rafael
Florianópolis
ativo

client_id = 4
Ana
Salvador
ativo
```

O cliente `1` representa um registro já existente na tabela de destino, porém com alteração de cidade.

O cliente `4` representa um novo registro.

---

### TEMP VIEW

A view intermediária adapta os nomes das colunas da source para o schema utilizado pela target:

```text
client_id   → customer_id
full_name   → name
new_city    → city
new_status  → status
```

A coluna técnica `ingestion_timestamp` não é necessária para a operação realizada e não é exposta pela view.

A `TEMP VIEW` funciona, portanto, como uma camada de adaptação entre o schema recebido da origem e o schema esperado pela tabela de destino.

---

### Target

A tabela de destino possui o seguinte schema:

```text
customer_id
name
city
status
```

Estado inicial:

```text
1 | Rafael | Rio de Janeiro | ativo
2 | Maria  | São Paulo      | ativo
3 | João   | Curitiba       | ativo
```

---

## Transformações

A primeira transformação realizada foi a adaptação do schema da tabela de origem através de uma `TEMP VIEW`.

```sql
SELECT
    client_id  AS customer_id,
    full_name  AS name,
    new_city   AS city,
    new_status AS status
FROM workspace.default.lab17_merge_source
```

Após essa transformação, a view apresenta os dados no formato esperado pela tabela de destino:

```text
1 | Rafael | Florianópolis | ativo
4 | Ana    | Salvador      | ativo
```

Em seguida, a operação `MERGE` utiliza:

```sql
ON target.customer_id = source.customer_id
```

como condição para determinar se um registro da source já existe na target.

A lógica aplicada foi:

```text
MATCHED
→ UPDATE

NOT MATCHED
→ INSERT
```

---

## Action / Operação

A operação que modifica a tabela Delta é o próprio `MERGE`.

```sql
WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
```

Como a `TEMP VIEW` adapta previamente o schema da source para o schema esperado pela target, `UPDATE SET *` e `INSERT *` podem ser utilizados neste experimento.

A documentação oficial do Databricks informa que `UPDATE SET *` atualiza as colunas da target utilizando as colunas correspondentes da source e pressupõe compatibilidade entre os schemas. A mesma consideração se aplica a `INSERT *`.

Documentação oficial:

https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into

---

## Comando de análise

### Consulta da tabela após o MERGE

```sql
SELECT *
FROM workspace.default.lab17_merge_target
ORDER BY customer_id
```

### Histórico da tabela Delta

```sql
DESCRIBE HISTORY workspace.default.lab17_merge_target
```

O `DESCRIBE HISTORY` permite observar as operações de escrita realizadas sobre a tabela Delta e a versão criada por cada alteração.

Documentação oficial:

https://docs.databricks.com/aws/en/sql/language-manual/delta-describe-history

---

## Código

### 1. Criação da tabela Source

```python
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.lab17_merge_source (
    client_id INT,
    full_name STRING,
    new_city STRING,
    new_status STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
""")
```

```python
spark.sql("""
INSERT INTO workspace.default.lab17_merge_source
VALUES
    (1, 'Rafael', 'Florianópolis', 'ativo', current_timestamp()),
    (4, 'Ana', 'Salvador', 'ativo', current_timestamp())
""")
```

---

### 2. Criação da TEMP VIEW para adaptação do schema

```python
spark.sql("""
CREATE OR REPLACE TEMP VIEW lab17_customer_updates AS

SELECT
    client_id  AS customer_id,
    full_name  AS name,
    new_city   AS city,
    new_status AS status

FROM workspace.default.lab17_merge_source
""")
```

```python
spark.sql("""
SELECT *
FROM lab17_customer_updates
ORDER BY customer_id
""").show()
```

---

### 3. Criação da tabela Target

```python
spark.sql("""
CREATE OR REPLACE TABLE workspace.default.lab17_merge_target (
    customer_id INT,
    name STRING,
    city STRING,
    status STRING
)
USING DELTA
""")
```

```python
spark.sql("""
INSERT INTO workspace.default.lab17_merge_target
VALUES
    (1, 'Rafael', 'Rio de Janeiro', 'ativo'),
    (2, 'Maria', 'São Paulo', 'ativo'),
    (3, 'João', 'Curitiba', 'ativo')
""")
```

### Estado inicial da Target

```python
spark.sql("""
SELECT *
FROM workspace.default.lab17_merge_target
ORDER BY customer_id
""").show()
```

---

### 4. MERGE

```python
spark.sql("""
MERGE INTO workspace.default.lab17_merge_target AS target

USING lab17_customer_updates AS source

ON target.customer_id = source.customer_id

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
""")
```

---

### 5. Resultado após o MERGE

```python
spark.sql("""
SELECT *
FROM workspace.default.lab17_merge_target
ORDER BY customer_id
""").show()
```

Resultado observado:

```text
1 | Rafael | Florianópolis | ativo
2 | Maria  | São Paulo     | ativo
3 | João   | Curitiba      | ativo
4 | Ana    | Salvador      | ativo
```

---

### 6. Histórico da tabela

```python
spark.sql("""
DESCRIBE HISTORY workspace.default.lab17_merge_target
""").show(truncate=False)
```

---

## Resultado observado

Antes do `MERGE`, a target possuía três clientes.

A source trouxe dois registros:

```text
customer_id = 1
customer_id = 4
```

O registro de `customer_id = 1` encontrou correspondência na target através da condição:

```sql
target.customer_id = source.customer_id
```

Portanto:

```text
MATCHED
→ UPDATE
```

A cidade do cliente foi alterada:

```text
Rio de Janeiro
      ↓
Florianópolis
```

O registro de `customer_id = 4` não encontrou correspondência na target.

Portanto:

```text
NOT MATCHED
→ INSERT
```

Ana foi adicionada como um novo registro.

Os clientes `2` e `3`, que não estavam presentes na source, permaneceram inalterados.

---

## Observações

### MERGE e chave de correspondência

O `MERGE` não determina se dois registros representam a mesma entidade pelo nome ou pela semelhança entre seus atributos.

A correspondência é definida explicitamente pela condição `ON`.

Neste experimento:

```sql
ON target.customer_id = source.customer_id
```

Portanto, `customer_id` determina se o registro da source corresponde a um registro existente na target.

---

### Schemas diferentes entre Source e Target

Em pipelines reais, source e target não precisam possuir o mesmo schema.

Neste experimento, a tabela de origem utiliza:

```text
client_id
full_name
new_city
new_status
```

enquanto a target utiliza:

```text
customer_id
name
city
status
```

A `TEMP VIEW` foi utilizada como uma camada de adaptação.

Ela lê as colunas utilizando o schema da source e as expõe utilizando os nomes esperados pela target.

Exemplo:

```sql
client_id AS customer_id
```

Por isso, conceitualmente, a view funciona como um **adaptador de schema**:

```text
SOURCE
schema da origem
      ↓
TEMP VIEW
adaptação
      ↓
schema esperado
pela TARGET
      ↓
MERGE
```

---

### UPDATE e INSERT explícitos

Quando source e target não possuem schemas compatíveis e nenhuma camada anterior realiza essa adaptação, as colunas podem ser mapeadas diretamente no `MERGE`.

Exemplo:

```sql
WHEN MATCHED THEN
    UPDATE SET
        target.city = source.new_city,
        target.status = source.new_status
```

Para novos registros:

```sql
WHEN NOT MATCHED THEN
    INSERT (
        customer_id,
        name,
        city,
        status
    )
    VALUES (
        source.client_id,
        source.full_name,
        source.new_city,
        source.new_status
    )
```

Portanto, a preparação da source antes do `MERGE` permite desacoplar parte da transformação do schema da própria operação de sincronização.

---

### Relação com CDC e SCD Type 1

O comportamento utilizado no experimento é semelhante ao utilizado em uma estratégia **SCD Type 1**.

Quando um cliente existente recebe novos valores, seus atributos anteriores são sobrescritos.

Neste laboratório:

```text
Rafael | Rio de Janeiro
```

foi substituído por:

```text
Rafael | Florianópolis
```

O histórico do atributo não foi mantido na própria estrutura da tabela.

É importante, porém, separar os conceitos:

```text
CDC
→ identifica ou transporta mudanças ocorridas nos dados

SCD
→ define como as diferentes versões dos registros serão armazenadas

MERGE
→ operação que pode ser utilizada para aplicar essas alterações sobre uma tabela Delta
```

Portanto, `MERGE`, CDC e SCD não são o mesmo conceito, embora possam ser utilizados em conjunto em pipelines de dados.

---

### Relação com Transaction Log e Time Travel

O `MERGE` modifica a tabela Delta e cria uma nova versão da tabela.

Isso conecta diretamente este laboratório aos Labs anteriores:

```text
Lab 15 — Transaction Log
        ↓
registra as alterações realizadas na tabela

Lab 16 — Time Travel
        ↓
permite consultar versões anteriores

Lab 17 — MERGE
        ↓
realiza UPDATE/INSERT
e gera uma nova versão da tabela Delta
```

A documentação oficial do Databricks informa que operações que modificam tabelas Delta geram novas versões, que podem ser observadas através do histórico da tabela.

Documentação oficial:

https://docs.databricks.com/aws/pt/tables/history

---

## Conclusão

O `MERGE` permite aplicar alterações de uma fonte de dados sobre uma tabela Delta utilizando uma condição para determinar se cada registro já existe no destino.

No experimento, `customer_id` foi utilizado como chave de correspondência.

Quando `customer_id = 1` foi encontrado tanto na source quanto na target, a cláusula `WHEN MATCHED` executou um `UPDATE`, alterando a cidade de Rafael de Rio de Janeiro para Florianópolis.

Quando `customer_id = 4` não encontrou correspondência na target, a cláusula `WHEN NOT MATCHED` executou um `INSERT`, adicionando Ana à tabela.

Os demais registros da target permaneceram inalterados.

Como source e target possuíam schemas diferentes, foi utilizada uma `TEMP VIEW` como camada intermediária de adaptação. A view transformou os nomes das colunas da origem para o formato esperado pelo destino, permitindo manter o `MERGE` simples através de `UPDATE SET *` e `INSERT *`.

O experimento também demonstrou a relação do `MERGE` com os conceitos estudados anteriormente em Delta Lake: a operação modifica a tabela de forma transacional e gera uma nova versão registrada no histórico da tabela.

Assim, o `MERGE` pode ser entendido como uma operação de sincronização entre uma fonte de alterações e uma tabela Delta de destino, permitindo aplicar atualizações e inserções de acordo com regras de correspondência previamente definidas.
