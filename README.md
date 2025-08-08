%md

# Engenharia de Dados - Projeto BPC

Este documento descreve a parte de engenharia do portfólio BPC, com foco na coleta, organização e transformação dos dados em uma arquitetura em camadas, utilizando o Databricks com notebooks em PySpark e SQL. 

---

## Fonte de Dados

Os dados utilizados no projeto foram extraídos de três principais fontes púplicas:

- **INSS**: Tabela de concessões do BPC por mês, disponível em csv.
 [Fonte: INSS - Dados Abertos](https://dadosabertos.inss.gov.br/dataset/beneficios-concedidos-plano-de-dados-abertos-jun-2023-a-jun-2025)

- **IBGE (Censo 2022)**: População total por município, utilizada para cálculo de cobertura e identificação do público-alvo.  
  [Fonte: Censo IBGE 2022](https://www.ibge.gov.br/)

- **Municípios/UF/Região**: Tabela de referência com códigos de municípios, sigla UF e região geográfica.  
  [Fonte: IBGE – Tabela de Referência Territorial](https://www.ibge.gov.br/)

---

## Arquitetura de dados
O pipeline foi estruturado seguindo o moedelo **Medallion Architecture (Bronze, Silver, Gold)** que facilita a rastreabilidade, versionamento e reutilização dos dados em múltiplos estágios.

### Camadas:

### Bronze
- Dados brutos carregados diretamente dos arquivos CSV das fontes públicas.
- Pouco ou nehum tratamento.
- Objetivo: manter a versão original para rastreabilidade.

### Silver 
- Aplicação de regras de negócios e limpeza dos dados. 
- Seleção de colunas relevantes, padronização de tipos e nomes, filtragem por tipo de benefício (BPC) e tipo de despacho (administrativo/judicial).

### Gold
- Dados organizados em tabelas fato e dimensões.
- Aplicação de agregações, cálculos de prazos, cobertura, e estrutura para consumo final (dashboards, análises).
- Destaques:
  - Fato BPC Geral
  - Fato BPC por UF
  - Fato População/Público-alvo
  - Dimensão Calendário
  - Dimensão Benefício
  - Dimensão UF/Região

---

## Fluxo de Transformação

Abaixo, o fluxo visual que mostra a transformação dos dados da camada Bronze até a Gold:

- Fluxo de camadas da tabela BPC Geral e BPC por UF

  ![Fluxo de Tranformação de tabelas](</Volumes/portfolio_inss/base_bpc/imagens/fluxo_bpc.png>)

- Fluxo de  camadas da tabela População PBC 

  ![Fluxo de Tranformação de tabelas](</Volumes/portfolio_inss/base_bpc/imagens/fluxo_populacao_bpc.png>)

---

### Por que usar arquitetura em camadas?

A Medallion Architecture permite:

- **Rastreabilidade**: Cada transformação tem uma origem clara, facilitando auditorias.
- **Reprodutibilidade**: Permite fazer análises com segurança, a partir dos dados brutos.
- **Separação da responsabilidade**: Cada camada tem um propósito distinto, facilitando manutenção e escabilidade.
- **Versionamento lógico**: A organização em camadas ajuda a entender a evolução dos dados ao longo do pipeline.

---

## Tecnologias Utilizadas 

- **Databricks Free Edition** (ambiente de notebooks)
- **Pyspark, Python e SQL** (tranformações, limpeza, análise exploratória, cálculos)
- **Power BI**: (visualização final dos dados)
- **GitHub** (versionamento e documentação - integrado ao Databricks)


## Estrutura de Pastas do Projeto (para GitHub)

```
📦 bpc-databricks-pipeline
│
├── 📁 notebooks
│   ├── 📁 bronze
│   │   ├── 🐍 bronze_bpc_ingestao.ipynb           # PySpark - CSV do BPC → bronze
│   │   ├── 🐍 bronze_censo_ingestao.ipynb         # PySpark - CSV do Censo → bronze
│   │   └── 🐍 bronze_uf_municipios_ingestao.ipynb # PySpark - CSV de UF → bronze
│   │
│   ├── 📁 silver
│   │   ├── 🐍 silver_bpc_concessoes.ipynb              # PySpark - Tratamento BPC
│   │   ├── 🐍 silver_censo_tratado.ipynb              # PySpark - População tratada
│   │   ├── 🐍 silver_uf_regiao_tratado.ipynb          # PySpark - UF e região
│   │   └── 📝 silver_populacao_bpc.sql                # SQL - União para gerar população BPC
│   │
│   ├── 📁 gold
│   │   ├── 📝 gold_fato_bpc_uf.sql                    # SQL - Fato por UF
│   │   ├── 📝 gold_fato_bpc_geral.sql                 # SQL - Fato geral
│   │   ├── 📝 gold_dim_uf_regiao.sql                  # SQL - Dimensão UF
│   │   ├── 📝 gold_dim_beneficio.sql                  # SQL - Dimensão benefício
│   │   ├── 📝 gold_dim_calendario.sql                 # SQL - Dimensão calendário
│   │   └── 📝 gold_dim_populacao.sql                  # SQL - População/público-alvo
│
├── 📁 dashboards
│   └── 📸 prints_dashboards/                          # Imagens do Power BI ou links
│
├── 📁 images
│   ├── fluxo_tabelas_databricks.png                   # Fluxo visual entre tabelas
│   └── prints_tabelas/                                # Prints detalhados por camada
│
├── 📁 docs
│   ├── README_engenharia.md                           # Parte técnica do projeto
│   ├── README_negocio.md                              # Parte de negócio e objetivo
│   ├── metodologia.md                                 # Detalhes de abordagem
│   └── dicionario_dados.md                            # Campos por tabela e camada
│
└── README.md                                          # Visão geral do projeto (linkando os outros)

```


