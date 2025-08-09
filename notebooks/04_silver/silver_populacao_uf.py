# Databricks notebook source

# Unindo e criando a tabela com dados populacionais por uf 
%sql
create or replace table portfolio_inss.silver.silver_ibge_populacao_uf
  select 
    m.CD_MUN as cod_municipio,
    m.NM_MUN as nome_municipio,
    m.SIGLA_UF as sigla_uf,
    m.NM_UF as nome_uf,
    p.idade_anos,
    p.populacao,
    p.sexo
  from portfolio_inss.silver.silver_ibge_populacao as p
  left join portfolio_inss.silver.silver_municipios_ibge as m
    on p.id_municipio = m.CD_MUN

