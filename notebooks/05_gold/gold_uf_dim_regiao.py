# Databricks notebook source
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")  # se já é UTF-8, use isso explicitamente
    .load("dbfs:/Volumes/portfolio_inss/base_bpc/raw_uploads/uf_regiao.csv")
)


# COMMAND ----------

# Grava tabela no schema bronze
df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable("portfolio_inss.gold.gold_dim_uf_regiao")
