import sys
import requests
import pandas as pd
import boto3
import logging
from io import StringIO
from awsglue.utils import getResolvedOptions

# Configuração de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 1. Obter Parâmetros do Glue
args = getResolvedOptions(sys.argv, ['S3_BUCKET_NAME'])
BUCKET_NAME = args['S3_BUCKET_NAME']

BASE_URL = "https://api.openf1.org/v1"
ENDPOINTS = ["drivers", "laps", "pit", "weather", "session_result", "stints"]

def get_s3_client():
    return boto3.client('s3')

def upload_to_s3(df, bucket, s3_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    s3_client = get_s3_client()
    try:
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())
        logger.info(f"Salvo: s3://{bucket}/{s3_key}")
    except Exception as e:
        logger.error(f"Erro S3: {e}")
        raise

def fetch_data(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Erro API {url}: {e}")
        return []

def run_ingestion():
    logger.info(f"Iniciando ingestão Bronze no bucket: {BUCKET_NAME}")
    
    years = range(2020, 2026)

    for year in years:
        logger.info(f"Ano: {year}")
        sessions = fetch_data("sessions", params={"year": year})
        
        if not sessions:
            continue

        for session in sessions:
            meeting_key = session.get('meeting_key')
            session_key = session.get('session_key')
            
            if not meeting_key or not session_key:
                continue

            logger.info(f"Sessão: {session_key}")

            for dataset_name in ENDPOINTS:
                data = fetch_data(dataset_name, params={"session_key": session_key})
                
                if not data:
                    continue

                df = pd.DataFrame(data)
                
                file_name = "arquivo.csv"
                s3_key = f"bronze/year={year}/meeting_key={meeting_key}/session_key={session_key}/dataset_name={dataset_name}/{file_name}"
                
                upload_to_s3(df, BUCKET_NAME, s3_key)

if __name__ == "__main__":
    run_ingestion()
