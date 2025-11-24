# Pitwall Preditivo F1

Projeto de engenharia de dados para ingestão e processamento de dados da Fórmula 1 utilizando a API OpenF1 e AWS.

## Estrutura do Projeto

- `src/ingestion/`: Scripts de ingestão de dados (Bronze Layer).
- `src/main.py`: Ponto de entrada da aplicação.

## Setup

1. **Crie um ambiente virtual:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

2. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure as variáveis de ambiente:**
   Crie um arquivo `.env` ou exporte as variáveis:
   ```bash
   export S3_BUCKET_NAME="seu-bucket-s3"
   export AWS_ACCESS_KEY_ID="sua-key"
   export AWS_SECRET_ACCESS_KEY="sua-secret"
   export AWS_DEFAULT_REGION="us-east-1"
   ```

4. **Execute a ingestão:**
   ```bash
   python src/main.py
   ```
