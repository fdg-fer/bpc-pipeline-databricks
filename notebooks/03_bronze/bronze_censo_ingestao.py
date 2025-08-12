# Databricks notebook source
# Leitura do arquivo csv

df = (
    spark.read.format("csv")
    .option("header", "true") # se tem cabeçalho
    .option("inferSchema", "true") # inferir o schema do arquivo csv
    .option("delimiter", ",") # delimitador do arquivo csv
    .option("encoding", "UTF-8")  # encoding do arquivo csv
    .load("dbfs:/Volumes/portfolio_inss/base_bpc/raw_uploads/censo_pop_2022.csv")
)


# COMMAND ----------

# Grava dados do df na tabela delta na camada bronze 

df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("portfolio_inss.bronze.bronze_ibge_censo_2022")


