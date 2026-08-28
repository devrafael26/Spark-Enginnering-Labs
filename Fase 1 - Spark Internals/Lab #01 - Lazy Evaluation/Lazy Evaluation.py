
df = spark.range(1_000_000)

df_filtered = df.filter("id > 500000")

df_selected = df_filtered.select("id")

df_selected.show(5)


df_selected.explain(True)



# FLUXO

# spark.range()
#       ↓
# dados gerados pelo Spark
#       ↓
# filter()
#       ↓
# select()
#       ↓
# show()
#       ↓
# EXECUÇÃO