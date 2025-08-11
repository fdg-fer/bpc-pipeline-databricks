# Databricks notebook source
# Criando tabela delta para armazenar dados a partir da join das tabelas de municípios e população 
# O resultado é uma tabela com os dados populacionais por limites e divisas

query = """

CREATE OR REPLACE TABLE portfolio_inss.silver.silver_ibge_populacao_uf
USING DELTA
  SELECT 
    m.CD_MUN as cod_municipio,
    m.NM_MUN as nome_municipio,
    m.SIGLA_UF as sigla_uf,
    m.NM_UF as nome_uf,
    p.idade_anos,
    p.populacao,
    p.sexo
  FROM portfolio_inss.silver.silver_ibge_populacao as p
  LEFT JOIN portfolio_inss.silver.silver_municipios_ibge as m
    ON p.id_municipio = m.CD_MUN
"""
spark.sql(query)	
