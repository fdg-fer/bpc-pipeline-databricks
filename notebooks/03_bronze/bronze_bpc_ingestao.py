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

from pyspark.sql import functions as F  # Importa funções do PySpark
import re  # Módulo para operações com expressões regulares 

def limpar_nome_coluna(nome):
    # 1. Remove espaços no início/fim
    nome = nome.strip()
    
    # 2. Substitui caracteres especiais por underscore
    nome = re.sub(r"[ ,{}()\n\t=]", "_", nome)
    
    # 3. Remove underscores consecutivos
    nome = re.sub(r"__+", "_", nome)
    
    # 4. Remove underscores no início/fim
    nome = nome.strip("_")
    
    # 5. Converte tudo para minúsculas
    return nome.lower()

# Aplica a função a todas as colunas do DataFrame
df = df.select([F.col(c).alias(limpar_nome_coluna(c)) for c in df.columns]) # Renomeia as colunas

# COMMAND ----------

# Grava dados do df na tabela delta na camada bronze 

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("portfolio_inss.bronze.bronze_inss_bpc_2025_01_06")
