import sys
import os
import json
import urllib.request
import logging
import boto3
import csv
from io import StringIO
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Configuração de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 1. Inicialização do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET_NAME'])

# CREDENCIAIS AWS
# Em produção, use IAM Roles ou variáveis de ambiente.
# Se necessário hardcodar para testes, substitua abaixo.
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "INSIRA_SUA_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "INSIRA_SUA_SECRET_KEY")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET_NAME = args['S3_BUCKET_NAME']
BASE_URL = "https://api.openf1.org/v1"
ENDPOINTS = ["drivers", "laps", "pit", "weather", "session_result", "stints"]

def get_s3_client():
    """Retorna cliente S3 com credenciais se fornecidas."""
    if AWS_ACCESS_KEY != "INSIRA_SUA_ACCESS_KEY":
        return boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
    return boto3.client('s3')

def upload_to_s3(data_list, bucket, s3_key):
    """Salva lista de dicts como CSV no S3."""
    if not data_list:
        return

    csv_buffer = StringIO()
    # Assumindo que todos os dicts tem as mesmas chaves do primeiro
    fieldnames = data_list[0].keys()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(data_list)
    
    s3_client = get_s3_client()
    try:
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())
        print(f"Salvo: s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"Erro S3: {e}")

def fetch_data(endpoint, params=None):
    """Busca dados da API usando urllib (Standard Lib)."""
    url = f"{BASE_URL}/{endpoint}"
    if params:
        query_string = urllib.parse.urlencode(params)
        url = f"{url}?{query_string}"
    
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            if response.status == 200:
                return json.loads(response.read().decode())
    except Exception as e:
        print(f"Erro API {url}: {e}")
    return []

def run_ingestion():
    print(">>> INICIANDO SCRIPT GLUE - DEBUG <<<")
    print(f"Bucket configurado: {BUCKET_NAME}")
    
    years = range(2020, 2026)

    for year in years:
        print(f"Processando Ano: {year}")
        sessions = fetch_data("sessions", params={"year": year})
        
        if not sessions:
            continue

        for session in sessions:
            meeting_key = session.get('meeting_key')
            session_key = session.get('session_key')
            
            if not meeting_key or not session_key:
                continue

            print(f"Sessão: {session_key}")

            for dataset_name in ENDPOINTS:
                data = fetch_data(dataset_name, params={"session_key": session_key})
                
                if not data:
                    continue

                # Caminho de saída: bronze/{dataset}/year={year}/meeting_key={meeting}/session_key={session}.csv
                s3_key = f"bronze/{dataset_name}/year={year}/meeting_key={meeting_key}/session_key={session_key}.csv"
                
                upload_to_s3(data, BUCKET_NAME, s3_key)

    job.commit()

if __name__ == "__main__":
    run_ingestion()
