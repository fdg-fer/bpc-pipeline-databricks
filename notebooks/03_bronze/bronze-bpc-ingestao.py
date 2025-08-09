# Databricks notebook source
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")  # se já é UTF-8, use isso explicitamente
    .load("dbfs:/Volumes/portfolio_inss/base_bpc/benef_conced/")
)


# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import functions as F
import re

def limpar_nome_coluna(nome):
    # Remove espaços extras, caracteres especiais e deixa em minúsculas
    nome = nome.strip()
    nome = re.sub(r"[ ,{}()\n\t=]", "_", nome)   # Substitui por "_"
    nome = re.sub(r"__+", "_", nome)              # Evita múltiplos "__"
    nome = nome.strip("_")                        # Remove "_" do início e fim
    return nome.lower()

df = df.select([F.col(c).alias(limpar_nome_coluna(c)) for c in df.columns])

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy("competência_concessão").count().display()

# COMMAND ----------

# Grava tabela no schema bronze
df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("portfolio_inss.bronze.bronze_inss_bpc_2025_01_06")

# COMMAND ----------

# MAGIC %sql
# MAGIC  --drop table if exists portfolio_inss.bronze.bpc
