# 🏎️ Pitwall Preditivo F1

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Glue-orange?style=for-the-badge&logo=amazon-aws&logoColor=white)
![Status](https://img.shields.io/badge/Status-Em%20Desenvolvimento-yellow?style=for-the-badge)

Bem-vindo ao **Pitwall Preditivo F1**, um projeto de Engenharia de Dados e Machine Learning focado em prever resultados e estratégias na Fórmula 1. Este projeto implementa uma arquitetura de dados moderna (Medallion Architecture) na AWS para processar dados históricos e em tempo real da F1.

---

## 🏗️ Arquitetura do Projeto

O projeto segue a arquitetura **Medallion** (Bronze, Silver, Gold):

1.  **🥉 Camada Bronze (Ingestion)**:
    *   **Fonte**: [OpenF1 API](https://openf1.org/).
    *   **Processo**: Script Python assíncrono (`asyncio`, `aiohttp`) que baixa dados de sessões, pilotos, voltas, clima, etc.
    *   **Armazenamento**: Arquivos CSV brutos no Amazon S3.
    *   **Destaques**: Deduplicação de arquivos e alta performance com concorrência controlada.

2.  **🥈 Camada Silver (Processing)**:
    *   **Fonte**: Arquivos CSV da Camada Bronze.
    *   **Processo**: Limpeza, seleção de colunas e conversão de tipos.
    *   **Armazenamento**: Arquivos **Parquet** otimizados no Amazon S3.
    *   **Catalogação**: Integração automática com **AWS Glue Data Catalog** para criar/atualizar tabelas e schemas.

3.  **🥇 Camada Gold (Analytics & ML)** *(Em Breve)*:
    *   Feature Engineering e Modelagem Preditiva.

---

## 🚀 Como Começar

### Pré-requisitos

*   Python 3.9+
*   Conta AWS com permissões para S3 e Glue.
*   AWS CLI configurado ou variáveis de ambiente.

### Instalação

1.  **Clone o repositório:**
    ```bash
    git clone https://github.com/ItaloRufca/pitwall-preditivo-f1.git
    cd pitwall-preditivo-f1
    ```

2.  **Crie um ambiente virtual (opcional, mas recomendado):**
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure as variáveis de ambiente:**
    Crie um arquivo `.env` na raiz do projeto:
    ```env
    S3_BUCKET_NAME=seu-bucket-datalake
    AWS_ACCESS_KEY_ID=sua-access-key
    AWS_SECRET_ACCESS_KEY=sua-secret-key
    AWS_REGION=us-east-1
    ```

---

## 🛠️ Execução

### 1. Ingestão (Bronze)
Baixa os dados da API e salva como CSV no S3.
```bash
python3 src/ingestion/ingest_bronze.py
```

### 2. Processamento (Silver)
Processa os CSVs, converte para Parquet e atualiza o AWS Glue Catalog.
```bash
python3 src/processing/process_silver.py
```

---

## 📂 Estrutura do Projeto

```plaintext
pitwall-preditivo-f1/
├── data/                   # Dados locais (ignorado no git)
├── scripts/                # Scripts utilitários
├── src/
│   ├── ingestion/
│   │   └── ingest_bronze.py   # Script de ingestão (API -> Bronze)
│   └── processing/
│       └── process_silver.py  # Script de processamento (Bronze -> Silver + Glue)
├── .env                    # Variáveis de ambiente (não comitar!)
├── .gitignore
├── README.md
└── requirements.txt
```

---

## 🤝 Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.

---

## 📝 Licença

Este projeto está sob a licença MIT.
