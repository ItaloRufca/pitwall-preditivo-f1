import os
import logging
import asyncio
import aiohttp
import aioboto3
import pandas as pd
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL = "https://api.openf1.org/v1"
ENDPOINTS = ["drivers", "intervals", "laps", "location", "pit", "position", "race_control", "sessions", "stints", "weather", "session_result", "starting_grid"]

# Limite de concorrência para evitar rate limits excessivos
MAX_CONCURRENT_REQUESTS = 10

async def upload_to_s3(df, bucket, s3_key, session):
    """Faz upload de um DataFrame pandas para o S3 em formato CSV de forma assíncrona."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    try:
        async with session.client('s3') as s3_client:
            await s3_client.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())
            logger.info(f"Arquivo salvo no S3: s3://{bucket}/{s3_key}")
    except Exception as e:
        logger.error(f"Erro ao salvar no S3: {e}")
        raise

async def fetch_data(session, endpoint, params=None):
    """Busca dados da API OpenF1 de forma assíncrona."""
    url = f"{BASE_URL}/{endpoint}"
    try:
        async with session.get(url, params=params, timeout=60) as response:
            if response.status == 404:
                logger.warning(f"Endpoint não encontrado: {url}")
                return []
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        logger.error(f"Erro na requisição para {url} com params {params}: {e}")
        return []

async def process_endpoint(http_session, boto_session, bucket_name, year, meeting_key, session_key, dataset_name, semaphore):
    """Processa um único endpoint para uma sessão específica."""
    s3_key = f"bronze/{dataset_name}/year={year}/meeting_key={meeting_key}/session_key={session_key}.csv"
    
    # Verificar se o arquivo já existe no S3
    try:
        async with boto_session.client('s3') as s3_client:
            await s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            logger.info(f"Arquivo já existe, pulando: s3://{bucket_name}/{s3_key}")
            return
    except Exception:
        # Se der erro (provavelmente 404 Not Found), continua o processamento
        pass

    async with semaphore:
        data = await fetch_data(http_session, dataset_name, params={"session_key": session_key})
        
        if not data:
            logger.info(f"Sem dados para {dataset_name} na sessão {session_key}")
            return

        df = pd.DataFrame(data)
        
        await upload_to_s3(df, bucket_name, s3_key, boto_session)

async def process_session(http_session, boto_session, bucket_name, year, session, semaphore):
    """Processa todos os endpoints para uma única sessão."""
    meeting_key = session.get('meeting_key')
    session_key = session.get('session_key')
    
    if not meeting_key or not session_key:
        logger.warning(f"Sessão sem meeting_key ou session_key: {session}")
        return

    logger.info(f"Iniciando processamento da Sessão: {session_key} (Meeting: {meeting_key})")
    
    tasks = []
    for dataset_name in ENDPOINTS:
        if dataset_name == "sessions": # Já temos as sessões
            continue
            
        task = process_endpoint(http_session, boto_session, bucket_name, year, meeting_key, session_key, dataset_name, semaphore)
        tasks.append(task)
    
    await asyncio.gather(*tasks)
    logger.info(f"Finalizado processamento da Sessão: {session_key}")

async def ingest_year(http_session, boto_session, bucket_name, year, semaphore):
    """Processa a ingestão de um ano inteiro."""
    logger.info(f"Buscando sessões para o ano: {year}")
    sessions = await fetch_data(http_session, "sessions", params={"year": year})
    
    if not sessions:
        logger.warning(f"Nenhuma sessão encontrada para o ano {year}")
        return

    # Salvar o arquivo de sessões também
    df_sessions = pd.DataFrame(sessions)
    s3_key_sessions = f"bronze/sessions/year={year}/sessions.csv"
    await upload_to_s3(df_sessions, bucket_name, s3_key_sessions, boto_session)

    # Buscar e salvar meetings do ano
    logger.info(f"Buscando meetings para o ano: {year}")
    meetings = await fetch_data(http_session, "meetings", params={"year": year})
    if meetings:
        df_meetings = pd.DataFrame(meetings)
        s3_key_meetings = f"bronze/meetings/year={year}/meetings.csv"
        await upload_to_s3(df_meetings, bucket_name, s3_key_meetings, boto_session)
    else:
        logger.warning(f"Nenhum meeting encontrado para o ano {year}")

    tasks = []
    for session in sessions:
        task = process_session(http_session, boto_session, bucket_name, year, session, semaphore)
        tasks.append(task)
    
    await asyncio.gather(*tasks)

async def main_async():
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("A variável de ambiente S3_BUCKET_NAME não está definida.")

    years = range(2023, 2026)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    session_boto = aioboto3.Session()

    async with aiohttp.ClientSession() as http_session:
        tasks = []
        for year in years:
            task = ingest_year(http_session, session_boto, bucket_name, year, semaphore)
            tasks.append(task)
        
        await asyncio.gather(*tasks)

def run_ingestion():
    """Ponto de entrada para execução síncrona (compatibilidade)."""
    asyncio.run(main_async())

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_ingestion()
