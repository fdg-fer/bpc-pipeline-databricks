# Databricks notebook source

# Criando tabela dimensao beneficio

%sql
create or replace table portfolio_inss.gold.gold_dim_beneficio as (

  SELECT 
    DISTINCT beneficio
  FROM portfolio_inss.gold.bpc_prazos
)

