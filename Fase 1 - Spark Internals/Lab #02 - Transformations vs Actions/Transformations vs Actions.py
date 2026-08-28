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