# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table portfolio_inss.gold.gold_fato_populacao_bpc as(
# MAGIC
# MAGIC WITH 
# MAGIC   bpc AS (
# MAGIC       SELECT 
# MAGIC         uf_final,
# MAGIC         beneficio,
# MAGIC         SUM(qtd_total_concedido) AS qtd_beneficio_bpc,
# MAGIC         count(distinct competencia) as qtd_competencias,
# MAGIC         round(qtd_beneficio_bpc/qtd_competencias, 0) as media_beneficio
# MAGIC       FROM portfolio_inss.gold.gold_bpc_uf
# MAGIC       where competencia >= '2025-01-01'
# MAGIC       GROUP BY
# MAGIC         uf_final, beneficio
# MAGIC   ), 
# MAGIC   populacao_bpc AS (
# MAGIC       SELECT 
# MAGIC         sigla_uf,
# MAGIC         nome_uf,
# MAGIC         SUM(CASE WHEN idade_anos < 65 THEN populacao ELSE 0 END) AS populacao_deficiente,
# MAGIC         SUM(CASE WHEN idade_anos >= 65 THEN populacao ELSE 0 END) AS populacao_idosa
# MAGIC       FROM portfolio_inss.silver.silver_ibge_populacao_uf
# MAGIC       GROUP BY sigla_uf ,nome_uf
# MAGIC   )
# MAGIC SELECT 
# MAGIC     p.sigla_uf,
# MAGIC     b.uf_final,
# MAGIC     b.beneficio,
# MAGIC     b.qtd_beneficio_bpc,
# MAGIC     b.media_beneficio,
# MAGIC     CASE 
# MAGIC         WHEN b.beneficio = 'Amp. Social Pessoa Portadora Deficiencia' THEN p.populacao_deficiente
# MAGIC         WHEN b.beneficio = 'Amparo Social ao Idoso' THEN p.populacao_idosa
# MAGIC         ELSE NULL
# MAGIC     END AS populacao_alvo
# MAGIC FROM bpc b
# MAGIC LEFT JOIN populacao_bpc p
# MAGIC     ON b.uf_final = p.nome_uf
# MAGIC )
