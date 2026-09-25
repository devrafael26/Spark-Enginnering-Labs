

######### Proposta para otimização #########

# Não vamos sair aplicando broadcast, repartition, filtro antecipado e etc., 
# só porque aprendemos essas técnicas. O Lab 31 mostrou justamente que várias coisas já foram 
# resolvidas pelo próprio Spark/Databricks.
# No noso baseline, os 30 milhões de orders já caíram para cerca de 10,56 milhões antes dos joins,
# os dois joins já foram PhotonBroadcastHashJoin, houve column pruning, e a agregação parcial reduziu
# 10,56 milhões → 560 antes do shuffle → 70 no resultado final. Ou seja: mexer nisso sem uma justificativa seria tuning “por receita”.

# E o diagnóstico mais importante foi outro: Scan concentrou 87,6% do Time Spent, enquanto o Shuffle
# concentrou principalmente o Memory Peak, 87,2%, e não o tempo da execução. O skew existia fortemente
# nos dados, mas não conseguimos provar que ele estava causando um problema no join, justamente porque o join de customers foi broadcast.
 




######### Experimento A — Reescrever explicitamente filtros e projeções #########



# Primeiro nós fazemos aquilo que intuitivamente um engenheiro poderia tentar ao olhar o código do Lab 30:

from pyspark.sql import functions as F

orders_a = (
    spark.table("workspace.default.lab30_orders")
    .filter(
        (F.col("order_status") == "COMPLETED") &
        (F.col("order_date") >= F.lit("2026-07-01"))
    )
    .select(
        "customer_id",
        "product_id",
        "quantity",
        "amount"
    )
)

products_a = (
    spark.table("workspace.default.lab30_products")
    .select(
        "product_id",
        "category_id"
    )
)

customers_a = (
    spark.table("workspace.default.lab30_customers")
    .filter(F.col("customer_status") == "ACTIVE")
    .select(
        "customer_id",
        "region_id"
    )
)



# Depois fazemos os joins
# A ideia aqui não é esperar uma grande melhora.
# É justamente testar:
# “Se eu escrever explicitamente filtros e projeções antes dos joins, o plano físico melhora?”
# Provavelmente vamos descobrir que o plano fica praticamente equivalente, porque o Catalyst já havia antecipado filtros e removido colunas desnecessárias no Lab 30.

df_a = (
    orders_a
    .join(
        products_a,
        on="product_id",
        how="inner"
    )
    .join(
        customers_a,
        on="customer_id",
        how="inner"
    )
)



# e a agregação.

result_a = (
    df_a
    .groupBy(
        "region_id",
        "category_id"
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("total_orders")
    )
)



result_a.explain(True)



(
    result_a.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.lab32_result_a")
)




# Conclusão provisória do Experimento A:
# 
# A reorganização manual da pipeline, antecipando filtros e projeções antes dos joins, tornou o código mais alinhado ao plano lógico otimizado, porém não produziu mudança relevante na estratégia física observada. O Catalyst já realizava essas otimizações no baseline do Lab 30, mantendo column pruning, filtros antecipados e Broadcast Hash Join para customers e products.




######### Experimento B — Atacar o gargalo que realmente apareceu: leitura #########


#  Se o diagnóstico mostrou:
#  SCAN
#  ≈ 87,6% do Time Spent
# 
#  a pergunta correta passa a ser:
# 
#  Como fazer a consulta ler menos dados?
# 
#  E aí nós mexemos não na lógica do join, mas no layout físico da principal tabela, lab30_orders.
# 
#  A Databricks documenta o data skipping: estatísticas de arquivos, como mínimos e máximos, podem permitir que arquivos que não atendem ao predicado sejam ignorados durante a leitura. A recomendação atual da Databricks para organização física das tabelas é Liquid Clustering.
# 
#  https://docs.databricks.com/aws/pt/tables/data-skipping?utm_source=chatgpt.com



#  %sql
#  DROP TABLE IF EXISTS workspace.default.lab32_orders_clustered;



#  %sql
#  CREATE TABLE workspace.default.lab32_orders_clustered
#  CLUSTER BY (order_date)
#  AS
#  SELECT *
#  FROM workspace.default.lab30_orders;



#  %sql
#  DESCRIBE DETAIL workspace.default.lab32_orders_clustered;



#  %sql
#  OPTIMIZE workspace.default.lab32_orders_clustered FULL;



#  %sql
#  -- 1. Estatísticas do otimizador
#  ANALYZE TABLE workspace.default.lab32_orders_clustered
#  COMPUTE STATISTICS FOR ALL COLUMNS;
# 
#  DESCRIBE DETAIL workspace.default.lab32_orders_clustered;



orders_b = (
    spark.table("workspace.default.lab32_orders_clustered")
    .filter(
        (F.col("order_status") == "COMPLETED") &
        (F.col("order_date") >= F.lit("2026-07-01"))
    )
    .select(
        "customer_id",
        "product_id",
        "quantity",
        "amount"
    )
)



# Dimensões

products_b = (
    spark.table("workspace.default.lab30_products")
    .select(
        "product_id",
        "category_id"
    )
)

customers_b = (
    spark.table("workspace.default.lab30_customers")
    .filter(F.col("customer_status") == "ACTIVE")
    .select(
        "customer_id",
        "region_id"
    )
)



df_b = (
    orders_b
    .join(
        products_b,
        on="product_id",
        how="inner"
    )
    .join(
        customers_b,
        on="customer_id",
        how="inner"
    )
)



result_b = (
    df_b
    .groupBy(
        "region_id",
        "category_id"
    )
    .agg(
        F.sum("amount").alias("total_amount"),
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("total_orders")
    )
)



result_b.explain(True)



(
    result_b.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.lab32_result_b")
)



# Nós entramos pensando:
    
# Liquid Clustering(order_date)
#         ↓
# Data Skipping melhor
#         ↓
# Scan menor

# Mas descobrimos:
    
# CTAS
#         ↓
# Databricks escreveu a tabela em apenas 1 arquivo
#         ↓
# Liquid Clustering configurado
#         ↓
# mas praticamente não há arquivos entre os quais fazer skipping




######### Experimento B (parte 2) #########



from pyspark.sql import functions as F

orders_large = (
    spark.range(300_000_000)
    .withColumn("customer_id", (F.col("id") % 1_000_000).cast("long"))
    .withColumn("product_id", (F.col("id") % 50_000).cast("long"))
    .withColumn("quantity", ((F.col("id") % 5) + 1).cast("int"))
    .withColumn("amount", ((F.col("id") % 1000) / 10.0).cast("double"))
    .withColumn(
        "order_status",
        F.when((F.col("id") % 10) < 7, "COMPLETED").otherwise("PENDING")
    )
    .withColumn(
        "order_date",
        F.date_add(
            F.lit("2026-01-01"),
            (F.col("id") % 270).cast("int")
        )
    )
    .drop("id")
)



(
    orders_large
    .repartition(32)
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.lab32_orders_large")
)



#  %sql
#  DESCRIBE DETAIL workspace.default.lab32_orders_large;



#  %sql
#  CREATE OR REPLACE TABLE workspace.default.lab32_orders_large_clustered
#  CLUSTER BY (order_date)
#  AS
#  SELECT *
#  FROM workspace.default.lab32_orders_large;



#  %sql
#  OPTIMIZE workspace.default.lab32_orders_large_clustered FULL;



#  %sql
#  DESCRIBE DETAIL workspace.default.lab32_orders_large_clustered;



#  %sql
#  ANALYZE TABLE workspace.default.lab32_orders_large_clustered
#  COMPUTE STATISTICS FOR ALL COLUMNS;



#  %sql
#  ANALYZE TABLE workspace.default.lab32_orders_large
#  COMPUTE STATISTICS FOR ALL COLUMNS;



# Comparar a mesma consulta de filtro, sem joins nem agregações ainda. Isso isola o efeito do data skipping.

# tabela sem clustering

from pyspark.sql import functions as F

baseline_large = (
    spark.table("workspace.default.lab32_orders_large")
    .filter(F.col("order_date") >= F.lit("2026-07-01"))
)

baseline_large.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.lab32_filter_baseline"
)



# tabela clusterizada

clustered_large = (
    spark.table("workspace.default.lab32_orders_large_clustered")
    .filter(F.col("order_date") >= F.lit("2026-07-01"))
)

clustered_large.write.mode("overwrite").format("delta").saveAsTable(
    "workspace.default.lab32_filter_clustered"
)



# | Métrica      | Sem clustering | Com Liquid Clustering |
# | ------------ | -------------: | --------------------: |
# | Files read   |             32 |                     4 |
# | Files pruned |              0 |                     4 |
# | Bytes pruned |            0 B |              35,72 MB |
# | Bytes read   |      221,06 MB |             161,30 MB |
# | Rows read    |     98.888.879 |            98.888.879 |
# | Cache        |           100% |                  100% |





######### Experimento C #########
#  Validar se a otimização mudou o comportamento da execução



print(
    "Resultado A:",
    spark.table("workspace.default.lab32_result_a").count()
)

print(
    "Resultado B:",
    spark.table("workspace.default.lab32_result_b").count()
)



result_a.createOrReplaceTempView("result_a")
result_b.createOrReplaceTempView("result_b")



#  %sql
#  SELECT *
#  FROM result_a
# 
#  EXCEPT
# 
#  SELECT *
#  FROM result_b;



#  %sql
#  SELECT *
#  FROM result_b
# 
#  EXCEPT
# 
#  SELECT *
#  FROM result_a;




## Conclusão experimento C:

# As versões A e B produziram 70 registros cada. A comparação bidirecional com EXCEPT não
# retornou linhas em nenhuma direção, indicando que as otimizações aplicadas preservaram o resultado lógico da pipeline.





# A — Reescrita explícita
# → plano físico praticamente equivalente
# → mostra que Catalyst já fazia boa parte do trabalho

# B Parte 1 — Liquid Clustering inicial
# → 1 arquivo
# → cenário inadequado para demonstrar skipping

# B Parte 2 — massa maior e múltiplos arquivos
# → 32 arquivos base
# → 8 após clustering/OPTIMIZE
# → 4 arquivos pruned
# → 35,72 MB pruned
# → menos bytes lidos

# C — Validação funcional
# → 70 vs 70
# → EXCEPT nas duas direções = vazio
# → resultado preservado