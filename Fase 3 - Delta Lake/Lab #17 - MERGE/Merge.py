
# SOURCE (Delta Table) → TEMP VIEW (Schema Adapter) → TARGET (Delta Table)

#  Source — tabela de origem

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


spark.sql("""
INSERT INTO workspace.default.lab17_merge_source
VALUES
    (1, 'Rafael', 'Florianópolis', 'ativo', current_timestamp()),
    (4, 'Ana', 'Salvador', 'ativo', current_timestamp())
""")




# View intermediária

spark.sql("""
CREATE OR REPLACE TEMP VIEW lab17_customer_updates AS

SELECT
    client_id  AS customer_id,
    full_name  AS name,
    new_city   AS city,
    new_status AS status

FROM workspace.default.lab17_merge_source
""")


spark.sql("""
SELECT *
FROM lab17_customer_updates
ORDER BY customer_id
""").show()



# Target — tabela final Delta

spark.sql("""
CREATE OR REPLACE TABLE workspace.default.lab17_merge_target (
    customer_id INT,
    name STRING,
    city STRING,
    status STRING
)
USING DELTA
""")


spark.sql("""
INSERT INTO workspace.default.lab17_merge_target
VALUES
    (1, 'Rafael', 'Rio de Janeiro', 'ativo'),
    (2, 'Maria', 'São Paulo', 'ativo'),
    (3, 'João', 'Curitiba', 'ativo')
""")



# Executando o MERGE


spark.sql("""
MERGE INTO workspace.default.lab17_merge_target AS target

USING lab17_customer_updates AS source

ON target.customer_id = source.customer_id

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *
""")



spark.sql("""
SELECT *
FROM workspace.default.lab17_merge_target
ORDER BY customer_id
""").show()


# Localizando o MERGE pelo DESCRIBE HISTORY

spark.sql("""
DESCRIBE HISTORY workspace.default.lab17_merge
""").show(truncate=False)



#  ```
#  lab17_merge_source
#  TABELA DELTA
#  schema da origem
#          ↓
#  lab17_customer_updates
#  TEMP VIEW
#  adaptador de schema
#          ↓
#  lab17_merge_target
#  TABELA DELTA
#  schema de destino
#  ```