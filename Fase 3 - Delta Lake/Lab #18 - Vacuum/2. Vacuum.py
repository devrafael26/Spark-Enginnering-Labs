
#  Lab 15 — Transaction Log
#          ↓
#  Delta sabe quais arquivos pertencem
#  a cada estado da tabela
# 
#  Lab 16 — Time Travel
#          ↓
#  usa estados anteriores + arquivos antigos
# 
#  Lab 17 — MERGE
#          ↓
#  pode criar um novo estado da tabela,
#  tornando arquivos anteriores obsoletos
# 
#  Lab 18 — VACUUM
#          ↓
#  remove fisicamente arquivos antigos
#  quando eles deixam de precisar ser preservados



#  7 dias
#  delta.deletedFileRetentionDuration
#          ↓
#  arquivos de dados antigos
#          ↓
#  VACUUM pode removê-los
# 
#  30 dias
#  delta.logRetentionDuration
#          ↓
#  histórico / arquivos do transaction log
#          ↓
#  retenção do histórico da tabela
#  ```



# Limpar e criar a tabela

table_name = "workspace.default.lab18_vacuum"

spark.sql(f"""
DROP TABLE IF EXISTS {table_name}
""")


spark.sql(f"""
CREATE TABLE {table_name} (
    id INT,
    produto STRING,
    preco DOUBLE
)
USING DELTA
""")


spark.sql(f"""
INSERT INTO {table_name}
VALUES
    (1, 'Notebook', 3500.00),
    (2, 'Mouse', 150.00),
    (3, 'Teclado', 300.00)
""")


spark.sql(f"""
SELECT *
FROM {table_name}
ORDER BY id
""").show()


spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").show(truncate=False)


spark.sql(f"""
UPDATE {table_name}
SET preco = 200.00
WHERE id = 2
""")


spark.sql(f"""
SELECT *
FROM {table_name}
ORDER BY id
""").show()


spark.sql(f"""
DELETE FROM {table_name}
WHERE id = 3
""")


spark.sql(f"""
SELECT *
FROM {table_name}
ORDER BY id
""").show()


spark.sql(f"""
DESCRIBE HISTORY {table_name}
""").show(truncate=False)


spark.sql(f"""
SELECT *
FROM {table_name}
VERSION AS OF 1
ORDER BY id
""").show()



# Agora entra o VACUUM



spark.sql(f"""
VACUUM {table_name} DRY RUN
""").show(truncate=False)


spark.sql(f"""
VACUUM {table_name} RETAIN 168 HOURS DRY RUN
""").show(truncate=False)


spark.conf.set(
    "spark.databricks.delta.retentionDurationCheck.enabled",
    "false"
)


spark.sql(f"""
VACUUM {table_name}
RETAIN 0 HOURS
DRY RUN
""").show(truncate=False)