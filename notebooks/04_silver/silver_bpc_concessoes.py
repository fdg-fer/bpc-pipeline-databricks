# Databricks notebook source
# Bibliotecas

from pyspark.sql.functions import when, col, lower, to_date, year, current_date, date_diff, floor, expr, regexp_extract

# COMMAND ----------

# Leitura da camada bronze

df = spark.table("portfolio_inss.bronze.bronze_inss_bpc_2025_01_06")

# COMMAND ----------

# Visualização inicial da estrutura

df.printSchema()

# COMMAND ----------

# Seleção de colunas úteis
df = df.select('competência_concessão','espécie4','cid6','despacho','dt_nascimento','sexo','mun_resid','uf','dt_ddb','dt_dib')


# COMMAND ----------

df.groupBy("competência_concessão").count().display()
df.groupBy("espécie4").count().display()
df.groupBy("sexo").count().display()
df.groupBy("uf").count().display()
df.groupBy("despacho").count().display()

# COMMAND ----------

df = df.withColumnRenamed('competência_concessão', 'competencia')

# COMMAND ----------

# Conversão de datas

df = df.withColumn("competencia", to_date(expr("concat(competencia, '01')"),"yyyyMMdd"))\
        .withColumn("dt_nascimento", to_date(df.dt_nascimento, "dd/MM/yyyy"))\
        .withColumn("dt_ddb", to_date(df.dt_ddb, "dd/MM/yyyy"))\
        .withColumn("dt_dib", to_date(df.dt_dib, "dd/MM/yyyy"))
        

# COMMAND ----------

# Criando colunas 
# Criando colunas com regex para UF/Cidade onde o beneficiário reside

df = df.withColumn('COD_IBGE_resid', regexp_extract('mun_resid', r'(\d+)', 1))\
            .withColumn('uf_resid', regexp_extract('mun_resid', r'(\d+)-(\w{2})-(.+)', 2))\
            .withColumn('cidade_resid', regexp_extract('mun_resid', r'(\d+)-(\w{2})-(.+)', 3))

# COMMAND ----------

# Cálculo de colunas derivadas(idade, tempo até despacho, tipo de despacho

from pyspark.sql.types import FloatType
df = df.withColumn("tipo_despacho", when(lower(col("despacho")).contains("judicial"), "judicial").otherwise("administrativo"))\
       .withColumn("idade", floor(date_diff(current_date(), df.dt_nascimento)/365).cast(FloatType()))\
       .withColumn("dias_ate_despacho", date_diff("dt_ddb", "dt_dib").cast(FloatType()))

# COMMAND ----------

# Padronização de campos nulos/vazios

df = df.withColumn('cidade_resid', when(col('cidade_resid')== "", None).otherwise(col('cidade_resid')))\
       .withColumn('uf_resid', when(col('uf_resid')== "", None).otherwise(col('uf_resid')))
                   

df = df.fillna({'uf_resid': 'desconhecido', 'cidade_resid': 'desconhecido'})

# COMMAND ----------

# Apagando colunas que não serão mais usadas

df = df.drop('mun_resid', 'despacho')

df = df.withColumnRenamed('uf', 'uf_julgado')\
       .withColumnRenamed('espécie4', 'beneficio')\
       .withColumnRenamed('dt_ddb', 'dt_despacho')\
       .withColumnRenamed('dt_dib', 'dt_inicio_beneficio')\
       .withColumnRenamed('cid6', 'cid')

# COMMAND ----------

# Carrega os DataFrames
# Em uf resid desconhecido assumir uf julgado

df_uf_municipios = spark.table("portfolio_inss.silver.silver_uf_regiao")

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

# Criando tabela

df_result.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("portfolio_inss.silver.silver_bpc_concessoes")
