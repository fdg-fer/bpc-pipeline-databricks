# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create or replace table portfolio_inss.gold.gold_fato_bpc_uf as (
# MAGIC   WITH 
# MAGIC
# MAGIC   -- Cálculo dos prazos médios por tipo
# MAGIC   prazo_medio AS (
# MAGIC     SELECT
# MAGIC       competencia,
# MAGIC       beneficio,
# MAGIC       uf_final,
# MAGIC       tipo_despacho,
# MAGIC       ROUND(avg(dias_ate_despacho), 1) AS prazo_medio,
# MAGIC       ROUND(median(dias_ate_despacho), 1) AS prazo_mediana
# MAGIC     FROM portfolio_inss.silver.silver_bpc_concessoes
# MAGIC     WHERE dt_inicio_beneficio >= '2024-01-01'
# MAGIC     GROUP BY competencia, beneficio, tipo_despacho, uf_final
# MAGIC   ),
# MAGIC
# MAGIC   -- Cálculo da quantidade por tipo
# MAGIC   qtd_processos AS (
# MAGIC     SELECT
# MAGIC       competencia,
# MAGIC       beneficio,
# MAGIC       uf_final,
# MAGIC       SUM(CASE WHEN tipo_despacho = 'administrativo' THEN 1 ELSE 0 END) AS qtd_administrativo,
# MAGIC       SUM(CASE WHEN tipo_despacho = 'judicial' THEN 1 ELSE 0 END) AS qtd_judicial
# MAGIC     FROM portfolio_inss.silver.silver_bpc_concessoes
# MAGIC     WHERE dt_inicio_beneficio >= '2024-01-01'
# MAGIC     GROUP BY competencia, beneficio, uf_final
# MAGIC
# MAGIC   )
# MAGIC
# MAGIC   -- Tabela final
# MAGIC   SELECT
# MAGIC     q.competencia,
# MAGIC     q.uf_final,
# MAGIC     q.beneficio,
# MAGIC     q.qtd_administrativo,
# MAGIC     q.qtd_judicial,
# MAGIC     (q.qtd_administrativo + q.qtd_judicial) AS qtd_total_concedido,
# MAGIC     ROUND(q.qtd_judicial / NULLIF((q.qtd_administrativo + q.qtd_judicial), 0), 3) AS pct_judicializacao,
# MAGIC     MAX(CASE WHEN p.tipo_despacho = 'administrativo' THEN p.prazo_mediana END) AS prazo_mediana_adm,
# MAGIC     MAX(CASE WHEN p.tipo_despacho = 'judicial' THEN p.prazo_medio END) AS prazo_medio_jud
# MAGIC   FROM qtd_processos q
# MAGIC   LEFT JOIN prazo_medio p 
# MAGIC     ON q.competencia = p.competencia 
# MAGIC     AND q.uf_final = p.uf_final
# MAGIC     AND q.beneficio = p.beneficio
# MAGIC   GROUP BY 
# MAGIC     q.competencia,
# MAGIC     q.uf_final,
# MAGIC     q.beneficio,
# MAGIC     q.qtd_administrativo,
# MAGIC     q.qtd_judicial
# MAGIC   ORDER BY q.competencia, q.uf_final, q.beneficio
# MAGIC )
# MAGIC
