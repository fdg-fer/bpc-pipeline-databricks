# Databricks notebook source

query = """

create or replace table portfolio_inss.gold.gold_fato_bpc_uf as (
  WITH 

  -- Cálculo dos prazos médios por tipo
  prazo_medio AS (
    SELECT
      competencia,
      beneficio,
      uf_final,
      tipo_despacho,
      ROUND(avg(dias_ate_despacho), 1) AS prazo_medio,
      ROUND(median(dias_ate_despacho), 1) AS prazo_mediana
    FROM portfolio_inss.silver.silver_bpc_concessoes
    WHERE dt_inicio_beneficio >= '2024-01-01'
    GROUP BY competencia, beneficio, tipo_despacho, uf_final
  ),

  -- Cálculo da quantidade por tipo
  qtd_processos AS (
    SELECT
      competencia,
      beneficio,
      uf_final,
      SUM(CASE WHEN tipo_despacho = 'administrativo' THEN 1 ELSE 0 END) AS qtd_administrativo,
      SUM(CASE WHEN tipo_despacho = 'judicial' THEN 1 ELSE 0 END) AS qtd_judicial
    FROM portfolio_inss.silver.silver_bpc_concessoes
    WHERE dt_inicio_beneficio >= '2024-01-01'
    GROUP BY competencia, beneficio, uf_final

  )

  -- Tabela final
  SELECT
    q.competencia,
    q.uf_final,
    q.beneficio,
    q.qtd_administrativo,
    q.qtd_judicial,
    (q.qtd_administrativo + q.qtd_judicial) AS qtd_total_concedido,
    ROUND(q.qtd_judicial / NULLIF((q.qtd_administrativo + q.qtd_judicial), 0), 3) AS pct_judicializacao,
    MAX(CASE WHEN p.tipo_despacho = 'administrativo' THEN p.prazo_mediana END) AS prazo_mediana_adm,
    MAX(CASE WHEN p.tipo_despacho = 'judicial' THEN p.prazo_medio END) AS prazo_medio_jud
  FROM qtd_processos q
  LEFT JOIN prazo_medio p 
    ON q.competencia = p.competencia 
    AND q.uf_final = p.uf_final
    AND q.beneficio = p.beneficio
  GROUP BY 
    q.competencia,
    q.uf_final,
    q.beneficio,
    q.qtd_administrativo,
    q.qtd_judicial
  ORDER BY q.competencia, q.uf_final, q.beneficio
)
"""
spark.sql(query)
