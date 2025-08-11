%md

# Projeto BPC - Análise de Judicialização, Cobertura e Prazos
_Dados de concessões BPC do primeiro semestre de 2025._

O Benefício de Prestação Continuada (BPC) é um dos temas mais debatidos no âmbito da assistência social no Brasil. Voltado para pessoas idosas ou com deficiência em situação de vulnerabilidade, o BPC se diferencia de benefícios previdenciários como aposentadorias ou auxílios por incapacidade, pois não exige contribuição prévia do beneficiário. Essa característica, somada ao seu impacto social e orçamentário, o torna alvo frequente de debates políticos, ajustes fiscais e mudanças legislativas.

A concessão do BPC pode ocorrer de duas formas: **administrativa**, diretamente pelo INSS, ou **judicial**, quando o pedido inicial é negado e o requerente recorre à Justiça. A judicialização representa não apenas um aumento da demanda para o Judiciário, mas também uma oportunidade para escritórios e profissionais jurídicos identificarem regiões com maior potencial de atuação.

Compreender **quais regiões apresentam índices elevados de judicialização e como se comportam os indicadores de cobertura e prazos** permite tomadas de decisão mais estratégicas. Por exemplo:

  - No **BPC-Idoso**, que possui menos barreiras técnicas, regiões com alta judicialização e baixa cobertura podem sinalizar alto potencial de novas ações.

  - No **BPC-Deficiente**, que exige perícias e laudos mais complexos, indicadores como prazos médios e tipo de decisão ajudam a identificar áreas com maior necessidade de apoio jurídico especializado.

Este projeto propõe uma solução baseada em indicadores estruturados e atualizados, permitindo monitorar a situação do BPC por unidade federativa e modalidade, ajudando gestores e advogados a agir de forma mais direcionada e eficaz.

---

## Tecnologias Utilizadas 

- **Databricks Free Edition** (ambiente de notebooks)
- **Pyspark, Python e SQL** (tranformações, limpeza, análise exploratória, cálculos)
- **Power BI**: (visualização final dos dados)
- **GitHub** (versionamento e documentação - integrado ao Databricks)

---

## Fonte de Dados

Os dados utilizados no projeto foram extraídos de três principais fontes púplicas:

- **INSS**: Tabela de concessões do BPC por mês, disponível em csv.
 [Fonte: INSS - Dados Abertos](https://dadosabertos.inss.gov.br/dataset/beneficios-concedidos-plano-de-dados-abertos-jun-2023-a-jun-2025)
  - Quantidade de arquivos: 6 cvs - Concessões de jan/25 a jun/25

- **IBGE (Censo 2022)**: População total por município, utilizada para cálculo de cobertura e identificação do público-alvo.  
  [Fonte: Censo IBGE 2022](https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?=&t=downloads/)
  - Quantidade de arquivos: 1 csv

- **Municípios/UF/Região**: Tabela de referência com códigos de municípios, sigla UF e região geográfica.  
  [Fonte: IBGE – Tabela de Referência Territorial](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html/)
  - Quantidade de arquivos: 1 csv

---

## Arquitetura de dados
O pipeline foi estruturado seguindo o modelo **Medallion Architecture (Bronze, Silver, Gold)** que facilita a rastreabilidade, versionamento e reutilização dos dados em múltiplos estágios.

![Medallion Architecture](<img/medallion.png>)

### Por que usar arquitetura em camadas?

A Medallion Architecture permite:

- **Rastreabilidade**: Cada transformação tem uma origem clara, facilitando auditorias.
- **Reprodutibilidade**: Permite fazer análises com segurança, a partir dos dados brutos.
- **Separação da responsabilidade**: Cada camada tem um propósito distinto, facilitando manutenção e escabilidade.
- **Versionamento lógico**: A organização em camadas ajuda a entender a evolução dos dados ao longo do pipeline.

---

## Camadas:

### 🥉 Bronze 
- Dados brutos carregados diretamente dos arquivos CSV das fontes públicas.
- Pouco ou nenhum tratamento.
- Objetivo: manter a versão original para rastreabilidade.

```
# Leitura de todos arquivos csv da pasta benef_conced contidos no volume
python
df = (
    spark.read.format("csv")
    .option("header", "true") # se tem cabeçalho
    .option("inferSchema", "true") # inferir o schema do arquivo csv
    .option("delimiter", ",") # delimitador do arquivo csv
    .option("encoding", "UTF-8")  # encoding do arquivo csv
    .load("dbfs:/Volumes/portfolio_inss/base_bpc/raw_uploads/censo_pop_2022.csv")
)

# Grava dados do df na tabela delta na camada bronze 

df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("portfolio_inss.bronze.bronze_ibge_censo_2022")
```
---

### 🥈 Silver 
- Aplicação de regras de negócios e limpeza dos dados. 
- Seleção de colunas relevantes, padronização de tipos, nomes e tipo de despacho (administrativo/judicial).


```
# Leitura da tabela delta na camada bronze

df = spark.table("portfolio_inss.bronze.bronze_inss_bpc_2025_01_06")


# Renomeando colunas

df = df.withColumnRenamed('competência_concessão', 'competencia')\
       .withColumnRenamed('uf', 'uf_julgado')\
       .withColumnRenamed('espécie4', 'beneficio')\
       .withColumnRenamed('dt_ddb', 'dt_despacho')\
       .withColumnRenamed('dt_dib', 'dt_inicio_beneficio')\
       .withColumnRenamed('cid6', 'cid')  


# Conversão de datas

df = df.withColumn("competencia", to_date(expr("concat(competencia, '01')"),"yyyyMMdd"))\
        .withColumn("dt_nascimento", to_date(df.dt_nascimento, "dd/MM/yyyy"))\
        .withColumn("dt_despacho", to_date(df.dt_despacho, "dd/MM/yyyy"))\
        .withColumn("dt_inicio_beneficio", to_date(df.dt_inicio_beneficio, "dd/MM/yyyy"))


# Grava dados do df na tabela delta na camada silver

df_result.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("portfolio_inss.silver.silver_bpc_concessoes")
```

#### Estrutura das Tabelas Silver
- [Baixar Dicionário Silver](https://github.com/fdg-fer/bpc-pipeline-databricks/blob/main/dic/silver.xlsx)

---
### 🥇 Camada Gold

Nesta camada, os dados já passaram por limpeza e transformações, estando prontos para **consumo final** em dashboards, relatórios e análises exploratórias.  
A modelagem segue o formato **Star Schema**, com tabelas fato e tabelas dimensão, permitindo consultas otimizadas e agregações consistentes.

#### Objetivos
- Consolidar informações calculadas e agregadas.
- Organizar dados para fácil integração com ferramentas de BI.
- Garantir consistência em métricas como **cobertura**, **prazos médios/medianos** e segmentações por UF e público-alvo.


 
```
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
spark.sql(query)
```

#### Estrutura das Tabelas Gold
- [Baixar Dicionário Gold](https://github.com/fdg-fer/bpc-pipeline-databricks/blob/main/dic/gold.xlsx)

**Tabelas Fato**

As tabelas Fato BPC reúnem informações consolidadas sobre solicitações e concessões do BPC, filtradas de acordo com o critério temporal definido para a análise: consideram-se apenas processos cuja data de entrada seja igual ou posterior a 01/01/2024.
Esse recorte temporal é aplicado para assegurar que a análise se concentre em pedidos recentes, possibilitando a avaliação de prazos e perfis de concessão.

- **Fato BPC Geral** – Dados consolidados do BPC em nível nacional, com métricas de cobertura e prazos.

  ![Fato BPC Geral](<img/fato_bpc_geral.png>)

- **Fato BPC por UF** – Mesma granularidade da tabela geral, mas segmentada por Unidade Federativa.

  ![Fato BPC por UF](<img/fato_bpc_uf.png>) 

- **Fato População/Público-alvo** – Informações demográficas e quantitativas sobre o público-alvo do benefício.

  ![Fato População](<img/fato_populacao.png>)

**Tabelas Dimensão**
- **Dimensão Calendário** – Datas de referência para análises temporais (ano, mês, trimestre, etc.).

  ![Dimensão Calendário](<img/dim_calendario.png>)

- **Dimensão Benefício** – Classificação e tipo de benefício dentro do BPC.

  ![Dimensão Benefício](<img/dim_beneficio.png>)

- **Dimensão UF/Região** – Mapeamento de Unidades Federativas para suas respectivas regiões.

  ![Dimensão UF/Região](<img/dim_uf.png>)

#### Exemplos de Uso
- Cálculo de cobertura por UF ao longo do tempo.
- Comparação de prazos médios administrativos e judiciais.
- Dashboards interativos no Power BI segmentados por região e público.

---

## Fluxo de Transformação Databricks

Abaixo, o fluxo visual que mostra a transformação dos dados da camada Bronze até a Gold:

Fluxo de camadas da tabela BPC

- **Volume**
  - `6 arquivos csv`
- **Bronze**
  - Tabela:`bronze_inss_bpc_2025_01_06`
- **Silver**
  - Tabela:`silver_bpc_concessoes`
- **Gold**
  - Tabela:`gold_fato_bpc_uf`
  - Tabela:`gold_fato_bpc_geral`

  ![Fluxo de Tranformação de tabelas](<img/fluxo_bpc.png>)

Fluxo de  camadas da tabela População PBC 

- **Volume**
  - `2 arquivos csv`
- **Bronze**
  - Tabela:`bronze_inss_bpc_2025_01_06`
- **Silver**
  - Tabela:`silver_bpc_concessoes`
- **Gold**
  - Tabela:`gold_fato_bpc_uf`
  - Tabela:`gold_fato_bpc_geral`

  ![Fluxo de Tranformação de tabelas](<img/fluxo_populacao_bpc.png>)

---

## Estrutura de Pastas do Projeto

```
📦 bpc-databricks-pipeline
│
├── 📁 notebooks
│   ├── 📁 bronze
│   │   ├── bronze_bpc_ingestao.ipynb           # PySpark - CSV do BPC → bronze
│   │   ├── bronze_censo_ingestao.ipynb         # PySpark - CSV do Censo → bronze
│   │   └── bronze_uf_municipios_ingestao.ipynb # PySpark - CSV de UF → bronze
│   │
│   ├── 📁 silver
│   │   ├── silver_bpc_concessoes.ipynb              # PySpark - Tratamento BPC
│   │   ├── silver_censo_tratado.ipynb              # PySpark - População tratada
│   │   ├── silver_uf_regiao_tratado.ipynb          # PySpark - UF e região
│   │   └── silver_populacao_bpc.sql                # SQL - União para gerar população BPC
│   │
│   ├── 📁 gold
│   │   ├── gold_fato_bpc_uf.sql                    # SQL - Fato por UF
│   │   ├── gold_fato_bpc_geral.sql                 # SQL - Fato geral
│   │   ├── gold_dim_uf_regiao.sql                  # SQL - Dimensão UF
│   │   ├── gold_dim_populacao.sql                  # SQL - População/público-alvo
│   │   ├── gold_dim_beneficio.sql                  # SQL - Dimensão benefício
│   │   └── gold_dim_calendario.sql                 # SQL - Dimensão calendário
│   │
├── 📁 dashboards
│   └── prints_dashboards/                          # Imagens do Power BI ou links
│
├── 📁 img
│   ├── fluxo_tabelas_databricks.png                   # Fluxo visual entre tabelas
│   └── prints_tabelas/                                # Prints detalhados por camada
│
└── README.md                                          # Visão geral do projeto

```


