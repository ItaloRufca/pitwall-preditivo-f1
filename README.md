# 🏎️ Pitwall Preditivo F1

**Pitwall Preditivo F1** é um projeto de Engenharia de Dados e Machine Learning que consome dados da [OpenF1 API](https://openf1.org/), processa em um Data Lake (Bronze/Silver/Gold) e treina modelos preditivos para estimar tempos de volta e estratégias de corrida.

O projeto foi desenhado para rodar tanto **localmente** (com PySpark) quanto na **AWS** (AWS Glue + S3).

---

## 🏗️ Arquitetura

O pipeline segue a arquitetura Medalhão:

1.  **Bronze Layer (Ingestão)**:
    *   Dados brutos da API (JSON) convertidos para CSV.
    *   Estrutura: `s3://bucket/bronze/{tabela}/year={ano}/meeting_key={key}/session_key={key}.csv`
    *   Tabelas: `drivers`, `laps`, `pit`, `weather`, `session_result`, `stints`.

2.  **Silver Layer (Processamento)**:
    *   Dados limpos, tipados e deduplicados.
    *   Formato: **Parquet**.
    *   Estrutura: `s3://bucket/silver/{tabela}/`

3.  **Gold Layer (Modelagem)**:
    *   Camada futura para tabelas agregadas e modelos de ML.

---

## 🚀 Como Rodar Localmente

### Pré-requisitos
*   Python 3.8+
*   Java 8 ou 11 (para Spark)
*   Conta AWS (opcional, se quiser salvar no S3)

### 1. Instalação
Clone o repositório e instale as dependências:
```bash
git clone https://github.com/ItaloRufca/pitwall-preditivo-f1.git
cd pitwall-preditivo-f1
pip install -r requirements.txt
```

### 2. Configuração (.env)
Crie um arquivo `.env` na raiz:
```ini
AWS_ACCESS_KEY_ID=sua_chave
AWS_SECRET_ACCESS_KEY=sua_senha
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=seu-bucket-datalake
```

### 3. Execução
**Passo 1: Ingestão (Bronze)**
Baixa dados da API e salva no S3 (ou local se configurar).
```bash
python -m src.main
```

**Passo 2: Processamento (Silver)**
Lê do Bronze, limpa e salva em Parquet.
```bash
python src/processing/process_silver.py
```

**Passo 3: Modelagem (Gold)**
Treina o modelo e permite predições interativas.
```bash
python src/modeling/predictor.py
```

---

## ☁️ Como Rodar na AWS (AWS Glue)

O projeto possui scripts dedicados para rodar como **Spark Jobs** no AWS Glue.

### Configuração Geral
1.  Crie um bucket S3 (ex: `meu-datalake-f1`).
2.  No console AWS Glue, vá em **ETL jobs**.

### 1. Job Bronze (Ingestão)
*   **Script**: Copie o conteúdo de `src/glue/etl_bronze.py`.
*   **Tipo**: Spark Script Editor.
*   **Parâmetros**: Adicione `--S3_BUCKET_NAME` com o nome do seu bucket.
*   **Credenciais**: Edite o script para incluir suas chaves AWS (ou use IAM Role se configurado).
*   **Execução**: Vai baixar os dados e salvar na pasta `bronze/`.

### 2. Job Silver (Processamento)
*   **Script**: Copie o conteúdo de `src/glue/etl_silver.py`.
*   **Tipo**: Spark Script Editor.
*   **Parâmetros**: `--S3_BUCKET_NAME`.
*   **Execução**: Lê do Bronze, trata e salva em `silver/` (Parquet).

### 3. Job Gold (Treinamento)
*   **Em breve**: Scripts para treinamento de modelos.

---

## 📂 Estrutura do Projeto

```
src/
├── glue/               # Scripts otimizados para AWS Glue
│   ├── etl_bronze.py
│   ├── etl_silver.py
│   └── etl_gold.py
├── ingestion/          # Scripts de ingestão local
│   └── ingest_bronze.py
├── processing/         # Scripts de processamento local
│   └── process_silver.py
├── modeling/           # Scripts de ML local
│   ├── feature_engineering.py
│   └── predictor.py
└── main.py             # Entrypoint local
```

---

## 🛠️ Tecnologias
*   **Linguagem**: Python
*   **Processamento**: Apache Spark (PySpark)
*   **Cloud**: AWS (S3, Glue)
*   **ML**: Spark MLlib
*   **Dados**: OpenF1 API
