# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table portfolio_inss.silver.silver_ibge_populacao_uf
# MAGIC   select 
# MAGIC     m.CD_MUN as cod_municipio,
# MAGIC     m.NM_MUN as nome_municipio,
# MAGIC     m.SIGLA_UF as sigla_uf,
# MAGIC     m.NM_UF as nome_uf,
# MAGIC     p.idade_anos,
# MAGIC     p.populacao,
# MAGIC     p.sexo
# MAGIC   from portfolio_inss.silver.silver_ibge_populacao as p
# MAGIC   left join portfolio_inss.silver.silver_municipios_ibge as m
# MAGIC     on p.id_municipio = m.CD_MUN
# MAGIC
