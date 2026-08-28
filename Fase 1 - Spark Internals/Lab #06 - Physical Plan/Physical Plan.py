df = spark.range(1_000_000)

df_filtered = df.filter("id > 500000")

df_selected = df_filtered.select("id")

df_selected.explain(True)