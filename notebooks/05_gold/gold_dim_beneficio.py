# Databricks notebook source

# Cria a tabela dimensao beneficio

query = """
	
CREATE OR REPLACE portfolio_inss.gold.gold_dim_beneficio as (
USING DELTA
  SELECT 
    DISTINCT beneficio
  FROM portfolio_inss.gold.bpc_prazos
)
"""
spark.sql(query)

