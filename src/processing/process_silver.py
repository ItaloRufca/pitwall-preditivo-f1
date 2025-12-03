import os
import logging
import boto3
import pandas as pd
from io import BytesIO, StringIO
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================================================================
# CONFIGURAÇÃO DO SCHEMA SILVER
# Defina aqui quais colunas de cada tabela da camada Bronze você quer manter.
# Se a lista estiver vazia ou a chave não existir, a tabela será ignorada.
# ==============================================================================
SILVER_SCHEMA = {
    "drivers": ['driver_number', 'broadcast_name', 'full_name', 'name_acronym', 'team_name', 'team_colour', 'first_name', 'last_name', 'headshot_url', 'country_code', 'session_key', 'meeting_key'],
    "intervals": ['date', 'driver_number', 'gap_to_leader', 'interval', 'meeting_key', 'session_key'],
    "laps": ['meeting_key', 'session_key', 'driver_number', 'i1_speed', 'i2_speed', 'st_speed', 'date_start', 'lap_duration', 'is_pit_out_lap', 'duration_sector_1', 'duration_sector_2', 'duration_sector_3', 'segments_sector_1', 'segments_sector_2', 'segments_sector_3', 'lap_number'],
    "meetings": ['meeting_key', 'meeting_name', 'meeting_official_name', 'location', 'country_key', 'country_code', 'country_name', 'circuit_key', 'circuit_short_name', 'date_start', 'gmt_offset', 'year'],
    "pit": ['date', 'driver_number', 'lap_number', 'meeting_key', 'pit_duration', 'session_key'],
    "position": ['date', 'session_key', 'meeting_key', 'driver_number', 'position'],
    "race_control": ['meeting_key', 'session_key', 'date', 'driver_number', 'lap_number', 'category', 'flag', 'scope', 'sector', 'message'],
    "sessions": ['meeting_key', 'session_key', 'location', 'date_start', 'date_end', 'session_type', 'session_name', 'country_key', 'country_code', 'country_name', 'circuit_key', 'circuit_short_name', 'gmt_offset', 'year'],
    "stints": ['meeting_key', 'session_key', 'stint_number', 'driver_number', 'lap_start', 'lap_end', 'compound', 'tyre_age_at_start'],
    "weather": ['date', 'session_key', 'humidity', 'pressure', 'rainfall', 'track_temperature', 'wind_speed', 'meeting_key', 'wind_direction', 'air_temperature'],
    "session_result": ['position', 'driver_number', 'number_of_laps', 'dnf', 'dns', 'dsq', 'duration', 'gap_to_leader', 'meeting_key', 'session_key'],
    "starting_grid": ['position', 'driver_number', 'lap_duration', 'meeting_key', 'session_key'],
}

def get_s3_client():
    return boto3.client('s3')

def list_bronze_files(bucket, prefix):
    """Lista todos os arquivos CSV no prefixo especificado."""
    s3 = get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.csv'):
                    files.append(obj['Key'])
    return files

def process_file(bucket, bronze_key, columns):
    """Lê um arquivo Bronze, seleciona colunas e salva como Parquet na Silver."""
    s3 = get_s3_client()
    
    # Definir chave de destino (Silver)
    # Ex: bronze/drivers/year=2024/... -> silver/drivers/year=2024/...
    silver_key = bronze_key.replace('bronze/', 'silver/').replace('.csv', '.parquet')
    
    # Verificar se já existe na Silver (Incremental)
    try:
        s3.head_object(Bucket=bucket, Key=silver_key)
        logger.info(f"Arquivo já existe na Silver, pulando: {silver_key}")
        return
    except:
        pass # Não existe, processar

    logger.info(f"Processando: {bronze_key} -> {silver_key}")
    
    try:
        # Ler do Bronze
        obj = s3.get_object(Bucket=bucket, Key=bronze_key)
        df = pd.read_csv(obj['Body'])
        
        # Filtrar colunas
        # Verifica quais colunas do schema realmente existem no DF
        cols_to_keep = [c for c in columns if c in df.columns]
        
        if not cols_to_keep:
            logger.warning(f"Nenhuma coluna do schema encontrada em {bronze_key}. Pulando.")
            return
            
        df_filtered = df[cols_to_keep]
        
        # Converter para Parquet em memória
        out_buffer = BytesIO()
        df_filtered.to_parquet(out_buffer, index=False)
        
        # Salvar no Silver
        s3.put_object(Bucket=bucket, Key=silver_key, Body=out_buffer.getvalue())
        logger.info(f"Salvo com sucesso: {silver_key}")
        
    except Exception as e:
        logger.error(f"Erro ao processar {bronze_key}: {e}")

def run_silver_processing():
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("S3_BUCKET_NAME não definido.")
    
    for table, columns in SILVER_SCHEMA.items():
        logger.info(f"Iniciando processamento da tabela: {table}")
        
        prefix = f"bronze/{table}/"
        files = list_bronze_files(bucket_name, prefix)
        
        if not files:
            logger.warning(f"Nenhum arquivo encontrado para {table} em {prefix}")
            continue
            
        logger.info(f"Encontrados {len(files)} arquivos para {table}. Processando...")
        
        for file_key in files:
            process_file(bucket_name, file_key, columns)

if __name__ == "__main__":
    run_silver_processing()
