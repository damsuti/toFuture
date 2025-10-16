# Projeto de Análise de Dados de Ações Brasileiras

## Descrição do Projeto

Este projeto é uma ferramenta de ETL (Extract, Transform, Load) e análise de dados para o mercado de ações brasileiro. Ele extrai dados históricos de um conjunto de tickers de empresas de setores como saúde, energia, infraestrutura e alimentos, processa esses dados, os armazena em um banco de dados PostgreSQL e permite a visualização interativa de correlações entre os ativos.

O fluxo de trabalho do projeto é dividido em duas partes principais:

1.  **Coleta e Armazenamento de Dados (`get_data.py`):** Um script Python utiliza a biblioteca `yfinance` para extrair dados históricos dos tickers. Esses dados são salvos em arquivos CSV e, em seguida, carregados para uma tabela (`companys`) em um banco de dados PostgreSQL.
2.  **Análise e Visualização (`some_analysis.ipynb`):** Um notebook Jupyter se conecta ao banco de dados, carrega os dados processados e gera um gráfico interativo de correlação, permitindo explorar a relação entre o Ibovespa e os demais ativos do portfólio.

## Arquitetura e Tecnologias

A arquitetura do projeto utiliza contêineres Docker para isolar o ambiente e garantir a reprodutibilidade.

-   **Docker:** Usado para gerenciar e orquestrar o contêiner do banco de dados PostgreSQL.
-   **Docker Compose:** Facilita a configuração e o gerenciamento do serviço do banco de dados com o arquivo `docker-compose.yml`.
-   **PostgreSQL:** O banco de dados relacional usado para armazenar os dados de ações.
-   **Python:** A linguagem principal do projeto, com as seguintes bibliotecas:
    -   `yfinance`: Para extrair dados de ações e índices.
    -   `pandas`: Para a manipulação, processamento e análise dos dados.
    -   `sqlalchemy`: Para a conexão e comunicação com o banco de dados.
    -   `plotly`: Para a criação de gráficos interativos e dinâmicos.
    -   `jupyter`: Para um ambiente de análise exploratória.

## Como Rodar o Projeto

Para executar o projeto, siga os passos abaixo. É necessário ter o Docker e o Docker Compose instalados no seu sistema.

### Passo 1: Iniciar o Banco de Dados com Docker Compose

No terminal, navegue até a pasta onde está o arquivo `docker-compose.yml` e execute o comando para iniciar o contêiner do PostgreSQL.

```bash
docker-compose up -d