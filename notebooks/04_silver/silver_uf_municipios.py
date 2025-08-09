# Databricks notebook source
# MAGIC %md
# MAGIC ------------------------------
# MAGIC ###  1. Leitura da camada bronze
# MAGIC -------------------------------

# COMMAND ----------

# Leitura da tabela na camada bronze

df = spark.table("portfolio_inss.bronze.bronze_municipios_ibge")

# COMMAND ----------

# Visualização estrutura da tabela

df.printSchema()

# COMMAND ----------

# Seleção de colunas úteis

df = df.select('CD_MUN', 'NM_MUN', 'CD_UF', 'NM_UF', 'SIGLA_UF')

# COMMAND ----------

# Criando tabela na camada silver

df.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("portfolio_inss.silver.silver_municipios_ibge")
