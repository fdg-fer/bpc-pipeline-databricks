# Databricks notebook source
# MAGIC %md
# MAGIC ------------------------------
# MAGIC ###  1. Leitura da camada bronze
# MAGIC -------------------------------

# COMMAND ----------

df = spark.table("portfolio_inss.bronze.bronze_inss_bpc_2025_01_06")

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

df = df.select('competência_concessão','espécie4','cid6','despacho','dt_nascimento','sexo','mun_resid','uf','dt_ddb','dt_dib')


# COMMAND ----------

df.groupBy("competência_concessão").count().display()
df.groupBy("espécie4").count().display()
df.groupBy("sexo").count().display()
df.groupBy("uf").count().display()
df.groupBy("despacho").count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------
# MAGIC ### 5. Conversão de datas
# MAGIC -----------------------------

# COMMAND ----------

df = df.withColumnRenamed('competência_concessão', 'competencia')

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import to_date, year, current_date, date_diff, floor,when, col, lower, expr

df = df.withColumn("competencia", to_date(expr("concat(competencia, '01')"),"yyyyMMdd"))\
        .withColumn("dt_nascimento", to_date(df.dt_nascimento, "dd/MM/yyyy"))\
        .withColumn("dt_ddb", to_date(df.dt_ddb, "dd/MM/yyyy"))\
        .withColumn("dt_dib", to_date(df.dt_dib, "dd/MM/yyyy"))
        

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------
# MAGIC ### 6. Criação de colunas
# MAGIC ----------------------

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - **Criando colunas com regex para UF/Cidade onde o beneficiário reside**
# MAGIC

# COMMAND ----------

# Criando colunas 
from pyspark.sql.functions import regexp_extract

df = df.withColumn('COD_IBGE_resid', regexp_extract('mun_resid', r'(\d+)', 1))\
            .withColumn('uf_resid', regexp_extract('mun_resid', r'(\d+)-(\w{2})-(.+)', 2))\
            .withColumn('cidade_resid', regexp_extract('mun_resid', r'(\d+)-(\w{2})-(.+)', 3))

# COMMAND ----------

# MAGIC %md
# MAGIC - **Cálculo de colunas derivadas(idade, tempo até despacho, tipo de despacho)**

# COMMAND ----------

from pyspark.sql.types import FloatType
df = df.withColumn("tipo_despacho", when(lower(col("despacho")).contains("judicial"), "judicial").otherwise("administrativo"))\
       .withColumn("idade", floor(date_diff(current_date(), df.dt_nascimento)/365).cast(FloatType()))\
       .withColumn("dias_ate_despacho", date_diff("dt_ddb", "dt_dib").cast(FloatType()))

# COMMAND ----------

# MAGIC %md
# MAGIC **Expressão regex:** `r'(\d+)-(\w{2})-(.+)'`
# MAGIC
# MAGIC **Vamos quebrar por partes:**
# MAGIC
# MAGIC `(\d+)`	****->** Grupo 1:** um ou mais dígitos (\d = número) — extrai o código IBGE
# MAGIC
# MAGIC `(\w{2})`	****->** Grupo 2:** duas letras (\w = letra ou número) — extrai a UF
# MAGIC
# MAGIC `(.+)`	****->** Grupo 3:** um ou mais caracteres — extrai o nome da cidade

# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------
# MAGIC ### 7. Padronização de campos nulos/vazios
# MAGIC ------------------------------------------

# COMMAND ----------

df = df.withColumn('cidade_resid', when(col('cidade_resid')== "", None).otherwise(col('cidade_resid')))\
       .withColumn('uf_resid', when(col('uf_resid')== "", None).otherwise(col('uf_resid')))
                   

df = df.fillna({'uf_resid': 'desconhecido', 'cidade_resid': 'desconhecido'})

# COMMAND ----------

# MAGIC %md
# MAGIC ----------------------------------------------
# MAGIC ### 8. Apagando colunas que não serão mais uasadas
# MAGIC ----------------------------------------------

# COMMAND ----------

df = df.drop('mun_resid', 'despacho')


df = df.withColumnRenamed('uf', 'uf_julgado')\
       .withColumnRenamed('espécie4', 'beneficio')\
       .withColumnRenamed('dt_ddb', 'dt_despacho')\
       .withColumnRenamed('dt_dib', 'dt_inicio_beneficio')\
       .withColumnRenamed('cid6', 'cid')

# COMMAND ----------

from pyspark.sql.functions import when, col, lower

# Carrega os DataFrames
df_uf_municipios = spark.table("portfolio_inss.silver.uf_regiao")

# Faz o join
df_joined = df.join(
    df_uf_municipios,
    df["uf_resid"] == df_uf_municipios["Sigla"],
    how="left"
)

# Cria as novas colunas com lógica condicional
df_final = df_joined.withColumn(
    "uf_final",
    when((lower(col("uf_resid")) == "desconhecido") | col("uf_resid").isNull(), col("uf_julgado"))
    .otherwise(col("UF"))
).withColumn(
    "flag_origem_uf",
    when((lower(col("uf_resid")) == "desconhecido") | col("uf_resid").isNull(), "julgado")
    .otherwise("resid")
)

# Exibe o resultado
colunas_originais = df.columns
df_result = df_final.select(*colunas_originais,"uf_final", "flag_origem_uf")



# COMMAND ----------

df_result.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("portfolio_inss.silver.silver_bpc_concessoes")
