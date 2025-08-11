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

### Bronze
- Dados brutos carregados diretamente dos arquivos CSV das fontes públicas.
- Pouco ou nenhum tratamento.
- Objetivo: manter a versão original para rastreabilidade.

### Silver 
- Aplicação de regras de negócios e limpeza dos dados. 
- Seleção de colunas relevantes, padronização de tipos, nomes e tipo de despacho (administrativo/judicial).

#### Estrutura das Tabelas Silver
- [Baixar Dicionário Silver](https://github.com/fdg-fer/bpc-pipeline-databricks/blob/main/dic/silver.xlsx)

### Camada Gold

Nesta camada, os dados já passaram por limpeza e transformações, estando prontos para **consumo final** em dashboards, relatórios e análises exploratórias.  
A modelagem segue o formato **Star Schema**, com tabelas fato e tabelas dimensão, permitindo consultas otimizadas e agregações consistentes.

#### Objetivos
- Consolidar informações calculadas e agregadas.
- Organizar dados para fácil integração com ferramentas de BI.
- Garantir consistência em métricas como **cobertura**, **prazos médios/medianos** e segmentações por UF e público-alvo.

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

- Fluxo de  camadas da tabela População PBC 

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
├── 📁 img
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


