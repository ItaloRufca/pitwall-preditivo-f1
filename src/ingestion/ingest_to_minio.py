import requests
import boto3
import json
import time
import logging
import os
import pandas as pd  # Importado para conversão para CSV
from io import StringIO  # Importado para buffer de string em memória
from botocore.exceptions import NoCredentialsError, ClientError
from botocore.config import Config

# --- Configuração do Projeto (Lido do Ambiente Docker) ---

# O nome do bucket que vamos criar e usar no MinIO
BRONZE_BUCKET_NAME = "pitwall"

# O endpoint do MinIO, como definido no docker-compose.yml
# De dentro de um container Docker, usamos o nome do serviço 'minio', não 'localhost'
MINIO_ENDPOINT_URL = os.environ.get("MINIO_ENDPOINT_URL", "http://minio:9000")

# Credenciais do MinIO, lidas do ambiente (definidas no seu docker-compose.yml)
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

# Prefixo da camada Bronze no S3.
BRONZE_LAYER_PREFIX = "bronze/"

# Anos para extração (2020 a 2025)
YEARS_TO_FETCH = range(2025, 2026) # range() é exclusivo no final

# API Base
API_BASE_URL = "https://api.openf1.org/v1"

# --- [IMPORTANTE] ---
# Mude para True para baixar TODOS os datasets, incluindo os de alto volume.
# ATENÇÃO: 'car_data', 'location' e 'position' representam 99% do volume de dados (Terabytes).
FETCH_HIGH_VOLUME_DATA = False 
# --- [IMPORTANTE] ---

# Endpoints de volume padrão (essenciais para o projeto)
STANDARD_ENDPOINTS_PER_SESSION = [
    'drivers', 'intervals', 'laps', 'pit', 'race_control',
    'session_result', 'starting_grid', 'stints', 'team_radio', 'weather',
]

# Endpoints de ALTO VOLUME (Telemetria e Posição em alta frequência)
HIGH_VOLUME_ENDPOINTS_PER_SESSION = [
    'car_data', 'location', 'position',
]

# Configuração de Logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Funções de Conexão e API ---

def get_minio_client():
    """
    Cria e retorna um cliente S3 (boto3) configurado para o MinIO.
    """
    logging.info(f"Conectando ao MinIO em {MINIO_ENDPOINT_URL}...")
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT_URL,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1' # Região padrão, MinIO não a usa mas boto3 exige
        )
        s3_client.list_buckets() # Testa a conexão
        logging.info("Conexão com MinIO bem-sucedida.")
        return s3_client
    except Exception as e:
        logging.error(f"Falha ao conectar ao MinIO: {e}")
        logging.error(f"Verifique se o container MinIO está rodando e acessível em {MINIO_ENDPOINT_URL}.")
        return None

def create_bucket_if_not_exists(s3_client, bucket_name):
    """
    Cria o bucket no MinIO se ele ainda não existir.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' já existe.")
    except ClientError as e:
        # Se o bucket não existe (Erro 404), crie-o
        if e.response['Error']['Code'] == '404':
            logging.info(f"Bucket '{bucket_name}' não encontrado. Criando...")
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' criado com sucesso.")
        else:
            logging.error(f"Erro ao verificar bucket: {e}")
            raise
    except Exception as e:
        logging.error(f"Erro inesperado ao verificar/criar bucket: {e}")
        raise

def fetch_from_api(endpoint, params={}):
    """
    Busca dados de um endpoint da API OpenF1.
    """
    try:
        url = f"{API_BASE_URL}/{endpoint}"
        response = requests.get(url, params=params)
        time.sleep(0.5) # Pausa para evitar rate limiting
        
        if response.status_code == 200:
            data = response.json()
            return data if data else None
        else:
            logging.error(f"Erro {response.status_code} ao buscar {endpoint}: {response.text}")
            return None
    except requests.RequestException as e:
        logging.error(f"Exceção na requisição para {endpoint}: {e}")
        return None

def write_to_minio_bronze(s3_client, bucket, data, s3_key):
    """
    Converte os dados (lista de dicts) para CSV e escreve no MinIO.
    """
    try:
        # 1. Converter dados JSON (lista de dicts) para DataFrame
        df = pd.DataFrame(data)

        # 2. Converter DataFrame para uma string CSV em memória
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, encoding='utf-8')
        csv_data = csv_buffer.getvalue()

        # 3. Fazer o upload do CSV para o MinIO
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_data,
            ContentType='text/csv' # Alterado de application/json para text/csv
        )
        logging.info(f"Sucesso ao escrever CSV em MinIO: s3://{bucket}/{s3_key}")
    except Exception as e:
        logging.error(f"Erro inesperado ao escrever CSV no MinIO: {e}")

# --- Função de Processamento de Endpoint ---

def process_endpoint_list(s3_client, endpoints, session_key, year, meeting_key):
    """
    Função helper para iterar sobre uma lista de endpoints e salvá-los no S3.
    """
    for endpoint in endpoints:
        logging.debug(f"      Buscando {endpoint} para session {session_key}...")
        
        data = fetch_from_api(endpoint, {'session_key': session_key})
        
        if data:
            # Estrutura de partição Hive-style para a camada Bronze
            s3_key = (
                f"{BRONZE_LAYER_PREFIX}year={year}/"
                f"meeting_key={meeting_key}/"
                f"session_key={session_key}/"
                f"dataset_name={endpoint}/" # Partição extra para clareza
                f"{endpoint}_data.csv" # Alterado de .json para .csv
            )
            write_to_minio_bronze(s3_client, BRONZE_BUCKET_NAME, data, s3_key)
        else:
            logging.warning(f"      Nenhum dado para {endpoint} na session {session_key}.")

# --- Função Principal (Pipeline) ---

def main():
    """
    Executa o pipeline de ingestão Batch para a camada Bronze.
    Itera por anos -> meetings -> sessions -> endpoints.
    """
    logging.info(f"--- INICIANDO PIPELINE BATCH: OpenF1 -> MinIO Bronze (CSV) ---")
    
    s3_client = get_minio_client()
    if s3_client is None:
        return

    # Garante que o bucket de destino existe
    create_bucket_if_not_exists(s3_client, BRONZE_BUCKET_NAME)

    # 1. Loop por Ano
    for year in YEARS_TO_FETCH:
        logging.info(f"--- Processando Ano: {year} ---")
        
        # 2. Busca Meetings (GPs) do ano
        meetings = fetch_from_api('meetings', {'year': year})
        if not meetings:
            logging.warning(f"Nenhum meeting encontrado para o ano {year}. Pulando.")
            continue
            
        meetings_key = f"{BRONZE_LAYER_PREFIX}year={year}/dataset_name=meetings/meetings_data.csv" # Alterado de .json para .csv
        write_to_minio_bronze(s3_client, BRONZE_BUCKET_NAME, meetings, meetings_key)

        # 3. Loop por Meeting (GP)
        for meeting in meetings:
            meeting_key = meeting.get('meeting_key')
            if not meeting_key: continue
                
            logging.info(f"  Processando Meeting: {meeting_key} ({meeting.get('meeting_name', '')})")
            
            # 4. Busca Sessions (TL1, TL2, Q, Corrida) do Meeting
            sessions = fetch_from_api('sessions', {'meeting_key': meeting_key})
            if not sessions:
                logging.warning(f"Nenhuma session encontrada para o meeting {meeting_key}. Pulando.")
                continue
            
            sessions_key = f"{BRONZE_LAYER_PREFIX}year={year}/meeting_key={meeting_key}/dataset_name=sessions/sessions_data.csv" # Alterado de .json para .csv
            write_to_minio_bronze(s3_client, BRONZE_BUCKET_NAME, sessions, sessions_key)

            # 5. Loop por Session
            for session in sessions:
                session_key = session.get('session_key')
                if not session_key: continue
                
                logging.info(f"    Processando Session: {session_key} ({session.get('session_name', '')})")
                
                # 6. Loop por Endpoints Padrão
                logging.info(f"      Buscando endpoints PADRÃO...")
                process_endpoint_list(s3_client, STANDARD_ENDPOINTS_PER_SESSION, session_key, year, meeting_key)
                
                # 7. Loop por Endpoints de Alto Volume (se a flag estiver ativa)
                if FETCH_HIGH_VOLUME_DATA:
                    logging.warning(f"      FLAG ATIVA: Buscando endpoints de ALTO VOLUME...")
                    process_endpoint_list(s3_client, HIGH_VOLUME_ENDPOINTS_PER_SESSION, session_key, year, meeting_key)
                else:
                    logging.info(f"      Pulando endpoints de ALTO VOLUME (car_data, location, position).")

    logging.info(f"--- PIPELINE BATCH BRONZE (MinIO) CONCLUÍDO ---")

if __name__ == "__main__":
    # Garante que as libs necessárias para o script estão instaladas
    # No seu Dockerfile de ingestão, adicione: RUN pip install requests boto3 pandas
    main()