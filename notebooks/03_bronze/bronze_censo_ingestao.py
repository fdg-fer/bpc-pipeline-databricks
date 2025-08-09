# Databricks notebook source
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .option("encoding", "UTF-8")  # se já é UTF-8, use isso explicitamente
    .load("dbfs:/Volumes/portfolio_inss/base_bpc/raw_uploads/censo_pop_2022.csv")
)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("portfolio_inss.bronze.bronze_ibge_censo_2022")


