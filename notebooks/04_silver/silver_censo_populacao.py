# Databricks notebook source
# Leitura da tabela na camada bronze

df = spark.table("portfolio_inss.bronze.bronze_ibge_censo_2022")

# COMMAND ----------

# Visualização inicial da estrutura

df.printSchema()

# COMMAND ----------

# Seleção de colunas úteis

df = df.select('id_municipio','id_municipio_nome','sexo','idade_anos','populacao')

# COMMAND ----------

# Criando tabela na camada silver

df.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("portfolio_inss.silver.silver_ibge_populacao")
