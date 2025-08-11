# Databricks notebook source

# Cria a tabela d_calendário com nomes de meses em português

query = """

CREATE OR REPLACE TABLE portfolio_inss.gold.gold_dim_calendario AS
USING DELTA
SELECT 
  data_calendario,
  YEAR(data_calendario) AS ano,
  MONTH(data_calendario) AS mes,
  DATE_FORMAT(data_calendario, 'yyyy-MM') AS ano_mes,
  quarter(data_calendario) AS trimestre,
  
  -- Nome do mês completo em português
  CASE MONTH(data_calendario)
    WHEN 1 THEN 'Janeiro'
    WHEN 2 THEN 'Fevereiro'
    WHEN 3 THEN 'Março'
    WHEN 4 THEN 'Abril'
    WHEN 5 THEN 'Maio'
    WHEN 6 THEN 'Junho'
    WHEN 7 THEN 'Julho'
    WHEN 8 THEN 'Agosto'
    WHEN 9 THEN 'Setembro'
    WHEN 10 THEN 'Outubro'
    WHEN 11 THEN 'Novembro'
    WHEN 12 THEN 'Dezembro'
  END AS nome_mes_completo,

  -- Nome do mês abreviado (3 letras) em português
  CASE MONTH(data_calendario)
    WHEN 1 THEN 'Jan'
    WHEN 2 THEN 'Fev'
    WHEN 3 THEN 'Mar'
    WHEN 4 THEN 'Abr'
    WHEN 5 THEN 'Mai'
    WHEN 6 THEN 'Jun'
    WHEN 7 THEN 'Jul'
    WHEN 8 THEN 'Ago'
    WHEN 9 THEN 'Set'
    WHEN 10 THEN 'Out'
    WHEN 11 THEN 'Nov'
    WHEN 12 THEN 'Dez'
  END AS nome_mes_abreviado,

  CASE 
    WHEN MONTH(data_calendario) BETWEEN 1 AND 3 THEN 'T1'
    WHEN MONTH(data_calendario) BETWEEN 4 AND 6 THEN 'T2'
    WHEN MONTH(data_calendario) BETWEEN 7 AND 9 THEN 'T3'
    ELSE 'T4'
  END AS nome_trimestre

FROM (
  SELECT explode(sequence(to_date('2024-01-01'), to_date('2025-12-31'), interval 1 day)) AS data_calendario
)
ORDER BY data_calendario;
"""
spark.sql(query)

