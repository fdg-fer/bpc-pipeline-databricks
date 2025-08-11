# Databricks notebook source
# Leitura de todos arquivos csv da pasta benef_conced contidos no volume

df = (
    spark.read.format("csv") 
    .option("header", "true") # se tem cabeçalho
    .option("inferSchema", "true") # inferir o schema do arquivo csv
    .option("delimiter", ";") # delimitador do arquivo csv
    .option("encoding", "UTF-8")  # encoding do arquivo csv
    .load("dbfs:/Volumes/portfolio_inss/base_bpc/benef_conced/")
)


# COMMAND ----------

# Biblioteca regex e funções pyspark

from pyspark.sql import functions as F
import re

# Remove dos nomes das colunas espaços extras, caracteres especiais e deixa em minúsculas 

def limpar_nome_coluna(nome):
    
    nome = nome.strip()
    nome = re.sub(r"[ ,{}()\n\t=]", "_", nome)   # Substitui por "_"
    nome = re.sub(r"__+", "_", nome)              # Evita múltiplos "__"
    nome = nome.strip("_")                        # Remove "_" do início e fim
    return nome.lower()

df = df.select([F.col(c).alias(limpar_nome_coluna(c)) for c in df.columns]) # Renomeia as colunas

# COMMAND ----------

# Grava dados do df na tabela delta na camada bronze 

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("portfolio_inss.bronze.bronze_inss_bpc_2025_01_06")
