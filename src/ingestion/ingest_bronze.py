import os
import logging
import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

logger = logging.getLogger(__name__)

BASE_URL = "https://api.openf1.org/v1"
ENDPOINTS = ["drivers", "laps", "pit", "weather", "session_result", "stints"]

def get_s3_client():
    """Cria e retorna um cliente S3."""
    return boto3.client('s3')

def upload_to_s3(df, bucket, s3_key):
    """Faz upload de um DataFrame pandas para o S3 em formato CSV."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3_client = get_s3_client()
    try:
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())
        logger.info(f"Arquivo salvo no S3: s3://{bucket}/{s3_key}")
    except Exception as e:
        logger.error(f"Erro ao salvar no S3: {e}")
        raise

def fetch_data(endpoint, params=None):
    """Busca dados da API OpenF1."""
    url = f"{BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na requisição para {url}: {e}")
        return []

def run_ingestion():
    """Função principal de ingestão."""
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("A variável de ambiente S3_BUCKET_NAME não está definida.")

    years = range(2020, 2026) # 2020 a 2025

    for year in years:
        logger.info(f"Processando ano: {year}")
        
        # Buscar sessões do ano
        sessions = fetch_data("sessions", params={"year": year})
        
        if not sessions:
            logger.warning(f"Nenhuma sessão encontrada para o ano {year}")
            continue

        for session in sessions:
            meeting_key = session.get('meeting_key')
            session_key = session.get('session_key')
            
            if not meeting_key or not session_key:
                logger.warning(f"Sessão sem meeting_key ou session_key: {session}")
                continue

            logger.info(f"Processando Sessão: {session_key} (Meeting: {meeting_key})")

            for dataset_name in ENDPOINTS:
                # Alguns endpoints podem exigir parâmetros diferentes, mas geralmente session_key filtra bem
                # Nota: 'session_result' não é um endpoint padrão documentado na home page simples, 
                # mas assumindo que existe ou mapeia para algo como 'position' ou similar.
                # Se a API retornar 404, o fetch_data vai logar o erro.
                
                # Ajuste para endpoints específicos se necessário. 
                # A maioria aceita session_key.
                data = fetch_data(dataset_name, params={"session_key": session_key})
                
                if not data:
                    logger.info(f"Sem dados para {dataset_name} na sessão {session_key}")
                    continue

                df = pd.DataFrame(data)
                
                # Caminho S3: bronze/year={YYYY}/meeting_key={KEY}/session_key={KEY}/dataset_name={NOME}/arquivo.csv
                file_name = "arquivo.csv"
                s3_key = f"bronze/year={year}/meeting_key={meeting_key}/session_key={session_key}/dataset_name={dataset_name}/{file_name}"
                
                upload_to_s3(df, bucket_name, s3_key)

    logger.info("Ingestão completa.")
