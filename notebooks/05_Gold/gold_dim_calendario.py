# Databricks notebook source
# MAGIC %sql
# MAGIC -- Criação da tabela de calendário no Databricks com nomes de meses em português
# MAGIC CREATE OR REPLACE TABLE portfolio_inss.gold.gold_dim_calendario AS
# MAGIC SELECT 
# MAGIC   data_calendario,
# MAGIC   YEAR(data_calendario) AS ano,
# MAGIC   MONTH(data_calendario) AS mes,
# MAGIC   DATE_FORMAT(data_calendario, 'yyyy-MM') AS ano_mes,
# MAGIC   quarter(data_calendario) AS trimestre,
# MAGIC   
# MAGIC   -- Nome do mês completo em português
# MAGIC   CASE MONTH(data_calendario)
# MAGIC     WHEN 1 THEN 'Janeiro'
# MAGIC     WHEN 2 THEN 'Fevereiro'
# MAGIC     WHEN 3 THEN 'Março'
# MAGIC     WHEN 4 THEN 'Abril'
# MAGIC     WHEN 5 THEN 'Maio'
# MAGIC     WHEN 6 THEN 'Junho'
# MAGIC     WHEN 7 THEN 'Julho'
# MAGIC     WHEN 8 THEN 'Agosto'
# MAGIC     WHEN 9 THEN 'Setembro'
# MAGIC     WHEN 10 THEN 'Outubro'
# MAGIC     WHEN 11 THEN 'Novembro'
# MAGIC     WHEN 12 THEN 'Dezembro'
# MAGIC   END AS nome_mes_completo,
# MAGIC
# MAGIC   -- Nome do mês abreviado (3 letras) em português
# MAGIC   CASE MONTH(data_calendario)
# MAGIC     WHEN 1 THEN 'Jan'
# MAGIC     WHEN 2 THEN 'Fev'
# MAGIC     WHEN 3 THEN 'Mar'
# MAGIC     WHEN 4 THEN 'Abr'
# MAGIC     WHEN 5 THEN 'Mai'
# MAGIC     WHEN 6 THEN 'Jun'
# MAGIC     WHEN 7 THEN 'Jul'
# MAGIC     WHEN 8 THEN 'Ago'
# MAGIC     WHEN 9 THEN 'Set'
# MAGIC     WHEN 10 THEN 'Out'
# MAGIC     WHEN 11 THEN 'Nov'
# MAGIC     WHEN 12 THEN 'Dez'
# MAGIC   END AS nome_mes_abreviado,
# MAGIC
# MAGIC   CASE 
# MAGIC     WHEN MONTH(data_calendario) BETWEEN 1 AND 3 THEN 'T1'
# MAGIC     WHEN MONTH(data_calendario) BETWEEN 4 AND 6 THEN 'T2'
# MAGIC     WHEN MONTH(data_calendario) BETWEEN 7 AND 9 THEN 'T3'
# MAGIC     ELSE 'T4'
# MAGIC   END AS nome_trimestre
# MAGIC
# MAGIC FROM (
# MAGIC   SELECT explode(sequence(to_date('2024-01-01'), to_date('2025-12-31'), interval 1 day)) AS data_calendario
# MAGIC )
# MAGIC ORDER BY data_calendario;
# MAGIC
# MAGIC
