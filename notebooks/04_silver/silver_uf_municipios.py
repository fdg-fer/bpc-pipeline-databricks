# Databricks notebook source
# MAGIC %md
# MAGIC ------------------------------
# MAGIC ###  1. Leitura da camada bronze
# MAGIC -------------------------------

# COMMAND ----------

df = spark.table("portfolio_inss.bronze.bronze_municipios_ibge")

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

df = df.select('CD_MUN', 'NM_MUN', 'CD_UF', 'NM_UF', 'SIGLA_UF')

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("portfolio_inss.silver.silver_municipios_ibge")
