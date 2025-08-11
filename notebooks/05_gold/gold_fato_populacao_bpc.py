# Databricks notebook source
# Cria a tabela púplivo alvo BPC granularidade por UF a partir do censo IBGE 2022
# Público >= 65 BPC Idoso
# Público < 65 BPC Deficiente
 

query =  """

CREATE OR REPLACE TABLE portfolio_inss.gold.gold_fato_populacao_bpc as(

WITH 
  bpc AS (
      SELECT 
        uf_final,
        beneficio,
        SUM(qtd_total_concedido) AS qtd_beneficio_bpc,
        count(distinct competencia) as qtd_competencias,
        round(qtd_beneficio_bpc/qtd_competencias, 0) as media_beneficio
      FROM portfolio_inss.gold.gold_bpc_uf
      where competencia >= '2025-01-01'
      GROUP BY
        uf_final, beneficio
  ), 
  populacao_bpc AS (
      SELECT 
        sigla_uf,
        nome_uf,
        SUM(CASE WHEN idade_anos < 65 THEN populacao ELSE 0 END) AS populacao_deficiente,
        SUM(CASE WHEN idade_anos >= 65 THEN populacao ELSE 0 END) AS populacao_idosa
      FROM portfolio_inss.silver.silver_ibge_populacao_uf
      GROUP BY sigla_uf ,nome_uf
  )
SELECT 
    p.sigla_uf,
    b.uf_final,
    b.beneficio,
    b.qtd_beneficio_bpc,
    b.media_beneficio,
    CASE 
        WHEN b.beneficio = 'Amp. Social Pessoa Portadora Deficiencia' THEN p.populacao_deficiente
        WHEN b.beneficio = 'Amparo Social ao Idoso' THEN p.populacao_idosa
        ELSE NULL
    END AS populacao_alvo
FROM bpc b
LEFT JOIN populacao_bpc p
    ON b.uf_final = p.nome_uf
)
"""
spark.sql(queey)
