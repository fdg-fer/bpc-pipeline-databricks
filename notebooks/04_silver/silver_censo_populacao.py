# Databricks notebook source
# MAGIC %md
# MAGIC ------------------------------
# MAGIC ###  1. Leitura da camada bronze
# MAGIC -------------------------------

# COMMAND ----------

df = spark.table("portfolio_inss.bronze.bronze_ibge_censo_2022")

# COMMAND ----------

# MAGIC %md
# MAGIC  ----------------------------------
# MAGIC ###  2. Visualização inicial da estrutura
# MAGIC  ---------------------------------- 

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------
# MAGIC ### 4. Seleção de colunas úteis
# MAGIC ---------------------------
# MAGIC

# COMMAND ----------

df = df.select('id_municipio','id_municipio_nome','sexo','idade_anos','populacao')

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------
# MAGIC ### 6. Criação de colunas
# MAGIC ----------------------

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("portfolio_inss.silver.silver_ibge_populacao")
