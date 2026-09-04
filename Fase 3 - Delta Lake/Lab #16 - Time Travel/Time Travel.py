
############# Experimento A — Criar a versão inicial #############

spark.sql("DROP TABLE IF EXISTS workspace.default.lab16_time_travel")


df = spark.createDataFrame([
    (1, "Notebook", 3500.00),
    (2, "Monitor", 1200.00),
    (3, "Mouse", 150.00)
], ["produto_id", "produto", "preco"])


df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.lab16_time_travel")


display(
    spark.table("workspace.default.lab16_time_travel")
)




############# Experimento B — Alterar os dados #############



 %sql
 UPDATE workspace.default.lab16_time_travel
 SET preco = 3900
 WHERE produto_id = 1;



display(
    spark.table("workspace.default.lab16_time_travel")
)



 %sql
 DELETE FROM workspace.default.lab16_time_travel
 WHERE produto_id = 3;




############# Experimento C — Viajar entre as versões #############


# Histórico da tabela
display(
    spark.sql("""
        DESCRIBE HISTORY workspace.default.lab16_time_travel
    """)
)

# Estado atual
display(
    spark.sql("""
        SELECT *
        FROM workspace.default.lab16_time_travel
    """)
)


### Time Travel por versão

# Versão 1

display(
    spark.sql("""
        SELECT *
        FROM workspace.default.lab16_time_travel
        VERSION AS OF 1
    """)
)


# Versão 0

display(
    spark.sql("""
        SELECT *
        FROM workspace.default.lab16_time_travel
        VERSION AS OF 0
    """)
)




### Time Travel por timestamp

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