# Projeto BPC - Análise de Judicialização, Cobertura e Prazos

O Benefício de Prestação Continuada (BPC) é um dos temas mais debatidos no âmbito da assistência social no Brasil. Voltado para pessoas idosas ou com deficiência em situação de vulnerabilidade, o BPC se diferencia de benefícios previdenciários como aposentadorias ou auxílios por incapacidade, pois não exige contribuição prévia do beneficiário. Essa característica, somada ao seu impacto social e orçamentário, o torna alvo frequente de debates políticos, ajustes fiscais e mudanças legislativas.

A concessão do BPC pode ocorrer de duas formas: **administrativa**, diretamente pelo INSS, ou **judicial**, quando o pedido inicial é negado e o requerente recorre à Justiça. A judicialização representa não apenas um aumento da demanda para o Judiciário, mas também uma oportunidade para escritórios e profissionais jurídicos identificarem regiões com maior potencial de atuação.

Compreender **quais regiões apresentam índices elevados de judicialização e como se comportam os indicadores de cobertura e prazos** permite tomadas de decisão mais estratégicas. Por exemplo:

  - No **BPC-Idoso**, que possui menos barreiras técnicas, regiões com alta judicialização e baixa cobertura podem sinalizar alto potencial de novas ações.

  - No **BPC-Deficiente**, que exige perícias e laudos mais complexos, indicadores como prazos médios e tipo de decisão ajudam a identificar áreas com maior necessidade de apoio jurídico especializado.

Este projeto propõe uma solução baseada em indicadores estruturados e atualizados, permitindo monitorar a situação do BPC por unidade federativa e modalidade, ajudando gestores e advogados a agir de forma mais direcionada e eficaz.

---

# Objetivo do Projeto
Este pipeline foi desenvolvido para monitorar e analisar concessões do Benefício de Prestação Continuada (BPC), com foco em pedidos cujo processo iniciou a partir de 2024 e foram concedidos entre janeiro e junho de 2025.
A análise permite identificar padrões de concessão, prazos e cobertura territorial, fornecendo informações estratégicas para áreas como advocacia previdenciária, órgãos públicos e estudos de políticas sociais
---

## Tecnologias Utilizadas 

- **Databricks Free Edition** (ambiente de notebooks)
- **Pyspark, Python e SQL** (tranformações, limpeza, análise exploratória, cálculos)
- **Power BI**: (visualização final dos dados)
- **GitHub** (versionamento e documentação - integrado ao Databricks)

---

## Fonte de Dados

Os dados utilizados no projeto foram extraídos de três principais fontes púplicas:

- **INSS**: Dados de concessões do BPC por mês, disponível em csv.
 [Fonte: INSS - Dados Abertos](https://dadosabertos.inss.gov.br/dataset/beneficios-concedidos-plano-de-dados-abertos-jun-2023-a-jun-2025)
  - Quantidade de arquivos: 6 cvs - Concessões de jan/25 a jun/25

- **IBGE (Censo 2022)**: População total por município, utilizada para cálculo de cobertura e identificação do público-alvo.  
  [Fonte: Censo IBGE 2022](https://www.ibge.gov.br/estatisticas/sociais/trabalho/22827-censo-demografico-2022.html?=&t=downloads/)
  - Quantidade de arquivos: 1 csv

- **Municípios/UF/Região**: Dados de referência com códigos de municípios, sigla UF e região geográfica.  
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

df = (
    spark.read.format("csv") 
    .option("header", "true") # se tem cabeçalho
    .option("inferSchema", "true") # inferir o schema do arquivo csv
    .option("delimiter", ";") # delimitador do arquivo csv
    .option("encoding", "UTF-8")  # encoding do arquivo csv
    .load("dbfs:/Volumes/portfolio_inss/base_bpc/benef_conced/")
)

from pyspark.sql import functions as F  # Importa funções do PySpark
import re  # Módulo para operações com expressões regulares 

def limpar_nome_coluna(nome):
    
    nome = nome.strip() # Remove espaços no início/fim
    nome = re.sub(r"[ ,{}()\n\t=]", "_", nome) # Substitui caracteres especiais por underscore
    nome = re.sub(r"__+", "_", nome) # Remove underscores consecutivos
    nome = nome.strip("_") # Remove underscores no início/fim
    return nome.lower() # Converte tudo para minúsculas

# Aplica a função a todas as colunas do DataFrame
df = df.select([F.col(c).alias(limpar_nome_coluna(c)) for c in df.columns]) # Renomeia as colunas


# Grava dados do df na tabela delta na camada bronze 

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("portfolio_inss.bronze.bronze_inss_bpc_2025_01_06")
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

#### Estrutura das Tabelas da Camada Silver
- [Baixar Dicionário Silver](https://github.com/fdg-fer/bpc-pipeline-databricks/blob/main/dic/silver.xlsx)

---
### 🥇 Camada Gold

Nesta camada, os dados já passaram por limpeza e transformações, estando prontos para **consumo final** em dashboards, relatórios e análises exploratórias.  
A modelagem segue o formato **Star Schema**, com tabelas fato e tabelas dimensão, permitindo consultas otimizadas e agregações consistentes.

#### Objetivos
- Consolidar informações calculadas e agregadas.
- Organizar dados para fácil integração com ferramentas de BI.
- Garantir consistência em métricas como **cobertura**, **prazos médios/medianos** e segmentações por UF e público-alvo.

A métrica de prazo, tempo em que é requerido e benefício e até a sua concessão tem um comportamento bem distinto entre e administrativo judicial, nessa análise exploratória pode entender melhor 
qual media foi aplicada.
- [Análise exploratória de prazos](https://github.com/fdg-fer/bpc-pipeline-databricks/blob/main/exploratoria_prazos.ipynb)

 
```
# Cria na camada gold a tabela fato_bpc_geral com granularidade por competência

query = """

CREATE OR REPLACE TABLE portfolio_inss.gold.gold_fato_bpc_geral (
USING DELTA

  WITH 
  -- Cálculo dos prazos médios por tipo
  
  prazo_medio AS (
    SELECT
      competencia,
      beneficio,
      tipo_despacho,
      ROUND(avg(dias_ate_despacho), 1) AS prazo_medio,
      ROUND(median(dias_ate_despacho), 1) AS prazo_mediana
    FROM portfolio_inss.silver.silver_bpc_concessoes
    WHERE dt_inicio_beneficio >= '2024-01-01'
    GROUP BY competencia, beneficio, tipo_despacho
  ),

  -- Cálculo da quantidade por tipo
  qtd_processos AS (
    SELECT
      competencia,
      beneficio,
      SUM(CASE WHEN tipo_despacho = 'administrativo' THEN 1 ELSE 0 END) AS qtd_administrativo,
      SUM(CASE WHEN tipo_despacho = 'judicial' THEN 1 ELSE 0 END) AS qtd_judicial
    FROM portfolio_inss.silver.silver_bpc_concessoes
    WHERE dt_inicio_beneficio >= '2024-01-01'
    GROUP BY competencia, beneficio
  )

  -- Tabela final
  SELECT
    q.competencia,
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
    AND q.beneficio = p.beneficio
  GROUP BY 
    q.competencia,
    q.beneficio,
    q.qtd_administrativo,
    q.qtd_judicial
  ORDER BY
   q.competencia,
   q.beneficio
)
"""
spark.sql(query)
```

#### Estrutura das Tabelas da Camada Gold
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

  ![Dimensão UF/Região](<img/dim_regiao.png>)

#### Exemplos de Uso
- Cálculo de cobertura por UF ao longo do tempo.
- Comparação de prazos médios administrativos e judiciais.
- Dashboards interativos no Power BI segmentados por região e público.

---

## Fluxo de Transformação Databricks

Abaixo, o fluxo visual que mostra a transformação dos dados da camada Bronze até a Gold:

Fluxo de camadas das tabelas -> `gold_fato_bpc_geral` e `gold_fato_bpc_uf`


| Volume               | Bronze                          | Silver                  | Gold                                  |
|:--------------------:|:-------------------------------:|:-----------------------:|:-------------------------------------:|
| `6 arquivos csv`     | `bronze_inss_bpc_2025_01_06`    | `silver_bpc_concessoes` | `gold_fato_bpc_uf` / `gold_fato_bpc_geral` |


  ![Fluxo de Tranformação de tabelas](<img/fluxo_bpc.png>)


Fluxo de camadas das tabelas -> `gold_fato_populacao_bpc`

| Volume          | Bronze                                                         | Silver                                       | Gold                | 
|---------------------|---------------------------------------------------------------------|-------------------------------------------------|--------------------|
|  `2 arquivos csv`   |`bronze_ibge_bronze_censo_2022`/`bronze_ibge_bronze_municipios_ibge`| `silver_ibge_populacao`/`silver_municipios_ibge`| `gold_fato_populacao_bpc`| 


  ![Fluxo de Tranformação de tabelas](<img/fluxo_populacao_bpc.png>)

---


### Visões Gold e Regras de Negócio

| Tabela                   | Descrição                                   | Regra de Negócio / Filtro                                 |
|--------------------------|---------------------------------------------|-----------------------------------------------------------|
| `gold_fato_bpc_geral`    | BPC concedidos granularidade mensal com share de jucialização, prazos médios     | Considera apenas processos iniciados a partir de 2024 e concedidos entre jan–jun/2025 |           
| `gold_fato_bpc_uf_` | BPC concedidos com granularidade mensal por UF com share de jucialização, prazos médios | Considera apenas processos iniciados a partir de 2024 e concedidos entre jan–jun/2025 |
| `gold_fato_bpc_populacao_uf` | Distribuição por público-alvo baseado na idade por UF | Sem recorte temporal                                       |


**Observação:**  
A distinção de recorte temporal é feita apenas em visões específicas para análises recentes. As camadas Bronze e Silver não aplicam esse filtro.


#### Modelagem Star Schema - Power BI

  ![Fluxo de Tranformação de tabelas](<img/schema_pbi.png>)


#### Dashboard 

  No painel, os cards mostram que o prazo judicial teve um aumento de 17% no segundo semestre em relação ao primeiro, refletindo uma tendência de crescimento mensal constante, conforme a tabela detalhada.
  A concessão administrativa também cresceu 12% do primeiro para o segundo trimestre.

  Esses indicadores evidenciam uma pressão crescente tanto na via judicial quanto administrativa, importante para o gestor acompanhar e ajustar estratégias no escritório.

  ![Fluxo de Tranformação de tabelas](<dashboard/visao_nacional.png>)

  A visão que mostra o cenário regional ao longo dos meses por Tipo de Benefício revela alguns pontos importantes na região Sul. Em Santa Catarina, o número absoluto de concessões do BPC Idoso é o menor entre os estados da região. Além disso, a cobertura por mil habitantes também é relativamente baixa, o que pode revelar uma demanda subatendida.

  Chama atenção, ainda, a judicialização relativamente alta nesse tipo de benefício, mesmo considerando que o BPC Idoso possui uma legislação mais objetiva e com prazos mais curtos, o que normalmente resulta em maior proporção de concessões administrativas.

  Esse cenário indica uma possível maior necessidade de representação profissional para os beneficiários do BPC Idoso em Santa Catarina, evidenciando uma oportunidade de negócio para serviços especializados de assessoria jurídica voltados para essa demanda.

  ![Fluxo de Tranformação de tabelas](<dashboard/visao_regional.png>)

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


