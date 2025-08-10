%md

# Projeto BPC - Análise de Judicialização, Cobertura e Prazos


O Benefício de Prestação Continuada (BPC) é um dos programas mais debatidos no âmbito da Previdência e da Assistência Social no Brasil. Diferente de benefícios como aposentadoria ou auxílio-doença, o BPC não exige contribuição prévia do beneficiário, sendo voltado exclusivamente para pessoas idosas ou com deficiência em situação de vulnerabilidade social.

Essa característica de não depender de arrecadação individual faz com que o BPC seja constantemente alvo de discussões políticas e propostas de alteração legislativa, especialmente em momentos de ajuste fiscal. Como consequência, seu cenário de concessão e manutenção exige monitoramento constante, já que mudanças nas regras podem impactar diretamente milhões de famílias que dependem desse auxílio.


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
O pipeline foi estruturado seguindo o modelo **Medallion Architecture (Bronze, Silver, Gold)** que facilita a rastreabilidade, versionamento e reutilização dos dados em múltiplos estágios.

### Camadas:

### Bronze
- Dados brutos carregados diretamente dos arquivos CSV das fontes públicas.
- Pouco ou nenhum tratamento.
- Objetivo: manter a versão original para rastreabilidade.

### Silver 
- Aplicação de regras de negócios e limpeza dos dados. 
- Seleção de colunas relevantes, padronização de tipos, nomes e tipo de despacho (administrativo/judicial).

## Camada Gold

Nesta camada, os dados já passaram por limpeza e transformações, estando prontos para **consumo final** em dashboards, relatórios e análises exploratórias.  
A modelagem segue o formato **Star Schema**, com tabelas fato e tabelas dimensão, permitindo consultas otimizadas e agregações consistentes.

### Objetivos
- Consolidar informações calculadas e agregadas.
- Organizar dados para fácil integração com ferramentas de BI.
- Garantir consistência em métricas como **cobertura**, **prazos médios/medianos** e segmentações por UF e público-alvo.

### Estrutura das Tabelas

**Tabelas Fato**
- **Fato BPC Geral** – Dados consolidados do BPC em nível nacional, com métricas de cobertura e prazos.


  ![Fato BPC Geral](<imagens/fato_bpc_geral.png>)

- **Fato BPC por UF** – Mesma granularidade da tabela geral, mas segmentada por Unidade Federativa.

  ![Fato BPC por UF](<imagens/fato_bpc_uf.png>) 

- **Fato População/Público-alvo** – Informações demográficas e quantitativas sobre o público-alvo do benefício.

  ![Fato População](<imagens/fato_populacao.png>)

**Tabelas Dimensão**
- **Dimensão Calendário** – Datas de referência para análises temporais (ano, mês, trimestre, etc.).

  ![Dimensão Calendário](<imagens/dim_calendario.png>)

- **Dimensão Benefício** – Classificação e tipo de benefício dentro do BPC.

  ![Dimensão Benefício](<imagens/dim_beneficio.png>)

- **Dimensão UF/Região** – Mapeamento de Unidades Federativas para suas respectivas regiões.

  ![Dimensão UF/Região](<imagens/dim_uf.png>)

### Exemplos de Uso
- Cálculo de cobertura por UF ao longo do tempo.
- Comparação de prazos médios administrativos e judiciais.
- Dashboards interativos no Power BI segmentados por região e público.

---

## Fluxo de Transformação

Abaixo, o fluxo visual que mostra a transformação dos dados da camada Bronze até a Gold:

- Fluxo de camadas da tabela BPC Geral e BPC por UF

  ![Fluxo de Tranformação de tabelas](<imagens/fluxo_populacao_bpc.png>)

- Fluxo de  camadas da tabela População PBC 

  ![Fluxo de Tranformação de tabelas](<imagens/fluxo_populacao_bpc.png>)

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


