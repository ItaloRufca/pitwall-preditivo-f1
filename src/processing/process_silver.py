import os
import logging
import boto3
import pandas as pd
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==============================================================================
# TIPAGEM EXPLÍCITA (Strict Typing)
# ==============================================================================
SILVER_TABLE_TYPES = {
    "drivers": {
        'driver_number': 'Int64', 'session_key': 'Int64', 'meeting_key': 'Int64',
        'full_name': 'string', 'name_acronym': 'string', 'team_name': 'string',
        'team_colour': 'string', 'headshot_url': 'string', 'country_code': 'string'
    },
    "intervals": {
        'meeting_key': 'Int64', 'session_key': 'Int64', 'driver_number': 'Int64',
        'gap_to_leader': 'string', 'interval': 'string', 'date': 'string'
    },
    "laps": {
        'meeting_key': 'Int64', 'session_key': 'Int64', 'driver_number': 'Int64', 'lap_number': 'Int64',
        'st_speed': 'float64', 'lap_duration': 'float64', 
        'duration_sector_1': 'float64', 'duration_sector_2': 'float64', 'duration_sector_3': 'float64',
        'is_pit_out_lap': 'boolean', 'date_start': 'string'
    },
    "meetings": {
        'meeting_key': 'Int64', 'circuit_key': 'Int64', 'year': 'Int64',
        'meeting_name': 'string', 'location': 'string', 'country_code': 'string',
        'country_name': 'string', 'date_start': 'string'
    },
    "pit": {
        'meeting_key': 'Int64', 'session_key': 'Int64', 'driver_number': 'Int64', 'lap_number': 'Int64',
        'pit_duration': 'float64', 'date': 'string'
    },
    "position": {
        'meeting_key': 'Int64', 'session_key': 'Int64', 'driver_number': 'Int64',
        'position': 'Int64', 'date': 'string'
    },
    "race_control": {
        'meeting_key': 'Int64', 'session_key': 'Int64', 'driver_number': 'Int64', 'lap_number': 'Int64',
        'category': 'string', 'flag': 'string', 'scope': 'string', 'sector': 'string', 'message': 'string',
        'date': 'string'
    },
    "sessions": {
        'meeting_key': 'Int64', 'session_key': 'Int64', 'circuit_key': 'Int64', 'year': 'Int64',
        'session_type': 'string', 'session_name': 'string', 'location': 'string',
        'country_code': 'string', 'country_name': 'string', 'circuit_short_name': 'string',
        'date_start': 'string', 'date_end': 'string'
    },
    "stints": {
        'meeting_key': 'Int64', 'session_key': 'Int64', 'driver_number': 'Int64', 'stint_number': 'Int64',
        'lap_start': 'Int64', 'lap_end': 'Int64', 'tyre_age_at_start': 'Int64',
        'compound': 'string'
    },
    "weather": {
        'meeting_key': 'Int64', 'session_key': 'Int64',
        'humidity': 'float64', 'pressure': 'float64', 'rainfall': 'float64',
        'track_temperature': 'float64', 'wind_speed': 'float64', 'air_temperature': 'float64',
        'wind_direction': 'Int64', 'date': 'string'
    },
    "session_result": {
        'meeting_key': 'Int64', 'session_key': 'Int64', 'driver_number': 'Int64', 
        'position': 'float64', 'number_of_laps': 'Int64', 'points': 'float64', 'grid_position': 'Int64',
        'dnf': 'boolean', 'dns': 'boolean', 'dsq': 'boolean', 
        'gap_to_leader': 'string', 'duration': 'string'
    },
    "starting_grid": {
        'meeting_key': 'Int64', 'session_key': 'Int64', 'driver_number': 'Int64',
        'position': 'Int64', 'lap_duration': 'float64'
    }
}

def get_s3_client():
    return boto3.client('s3')

def get_glue_client():
    return boto3.client('glue')

def list_s3_files(bucket, prefix, suffix=''):
    """Lista arquivos no S3 de forma eficiente (paginada)."""
    s3 = get_s3_client()
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith(suffix):
                    files.append(obj['Key'])
    return files

def list_bronze_tables(bucket):
    """Lista as tabelas (pastas) disponíveis na camada Bronze."""
    s3 = get_s3_client()
    result = s3.list_objects_v2(Bucket=bucket, Prefix='bronze/', Delimiter='/')
    tables = []
    if 'CommonPrefixes' in result:
        for prefix in result['CommonPrefixes']:
            table_name = prefix['Prefix'].rstrip('/').split('/')[-1]
            tables.append(table_name)
    return tables

def process_file(bucket, bronze_key, table_name):
    """
    Processa um único arquivo Bronze -> Silver.
    Retorna o DataFrame processado se sucesso, ou None se falha.
    """
    s3 = get_s3_client()
    silver_key = bronze_key.replace('bronze/', 'silver/').replace('.csv', '.parquet')
    
    try:
        # Ler do Bronze
        obj = s3.get_object(Bucket=bucket, Key=bronze_key)
        df = pd.read_csv(obj['Body'])
        
        # Copiar por segurança
        df_filtered = df.copy()
        
        # ---- ENFORCEMENT DE SCHEMA ----
        if table_name in SILVER_TABLE_TYPES:
            type_map = SILVER_TABLE_TYPES[table_name]
            for col, dtype in type_map.items():
                if col in df_filtered.columns:
                    try:
                        if dtype == 'Int64':
                            df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').astype('Int64')
                        elif dtype == 'boolean':
                             df_filtered[col] = df_filtered[col].map({True: True, False: False, 'True': True, 'False': False, 1: True, 0: False})
                             df_filtered[col] = df_filtered[col].astype('boolean')
                        elif dtype == 'float64':
                            df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').astype('float64')
                        elif dtype == 'string':
                             df_filtered[col] = df_filtered[col].astype(str).replace('nan', None).replace('<NA>', None)
                        else:
                            df_filtered[col] = df_filtered[col].astype(dtype)
                    except Exception as e:
                        # Log warning apenas em caso real de erro, para evitar spam use debug se for frequente
                        pass
        
        # Converter para Parquet em memória
        out_buffer = BytesIO()
        df_filtered.to_parquet(out_buffer, index=False)
        
        # Salvar no Silver
        s3.put_object(Bucket=bucket, Key=silver_key, Body=out_buffer.getvalue())
        
        return df_filtered
        
    except Exception as e:
        logger.error(f"Erro ao processar {bronze_key}: {e}")
        return None

def create_glue_database(database_name):
    glue = get_glue_client()
    try:
        glue.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Database for Pitwall Silver Layer'
            }
        )
        logger.info(f"Database Glue verificado: {database_name}")
    except glue.exceptions.AlreadyExistsException:
        pass

def map_dtype_to_glue(dtype):
    dtype_str = str(dtype)
    if 'Int64' in dtype_str or 'int' in dtype_str: return 'bigint'
    elif 'float' in dtype_str: return 'double'
    elif 'bool' in dtype_str: return 'boolean'
    elif 'datetime' in dtype_str: return 'timestamp'
    else: return 'string'

def create_or_update_glue_table(database_name, table_name, s3_path, df):
    glue = get_glue_client()
    
    columns = [{'Name': c, 'Type': map_dtype_to_glue(t)} for c, t in df.dtypes.items()]
    
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_path,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            },
            'Compressed': True,
            'StoredAsSubDirectories': False
        },
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {'classification': 'parquet', 'compressionType': 'snappy'}
    }
    
    try:
        glue.update_table(DatabaseName=database_name, TableInput=table_input)
        logger.info(f"Schema atualizado no Glue: {table_name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=database_name, TableInput=table_input)
        logger.info(f"Tabela criada no Glue: {table_name}")
    except Exception as e:
        logger.error(f"Erro Glue para {table_name}: {e}")

def run_silver_processing():
    bucket = os.environ.get("S3_BUCKET_NAME")
    if not bucket: raise ValueError("S3_BUCKET_NAME não definido.")
    
    glue_db = "pitwall_silver"
    create_glue_database(glue_db)
    
    tables = list_bronze_tables(bucket)
    logger.info(f"Tabelas encontradas: {tables}")

    max_workers = 10  # Ajuste conforme necessário (S3 aguenta bem)

    for table in tables:
        logger.info(f"--- Verificando {table} ---")
        
        # 1. Listar tudo no Bronze e Silver para comparação eficiente (Batch check)
        bronze_files = set(list_s3_files(bucket, f"bronze/{table}/", '.csv'))
        silver_files = set(list_s3_files(bucket, f"silver/{table}/", '.parquet'))
        
        # Converter keys de silver para o formato bronze esperado para comparar
        # silver: silver/table/file.parquet -> bronze: bronze/table/file.csv
        silver_as_bronze = {k.replace('silver/', 'bronze/').replace('.parquet', '.csv') for k in silver_files}
        
        # Diferença: O que está no Bronze mas não no Silver
        to_process = list(bronze_files - silver_as_bronze)
        
        if not to_process:
            logger.info(f"Tabela {table}: Tudo atualizado ({len(silver_files)} arquivos).")
            continue
            
        logger.info(f"Processando {len(to_process)} novos arquivos para {table} com {max_workers} threads...")
        
        sample_df = None
        processed_count = 0
        
        # 2. Processamento Paralelo
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submete jobs
            future_to_file = {executor.submit(process_file, bucket, f, table): f for f in to_process}
            
            for future in as_completed(future_to_file):
                key = future_to_file[future]
                try:
                    df = future.result()
                    if df is not None:
                        processed_count += 1
                        sample_df = df # Guarda qualquer um para o schema
                except Exception as exc:
                    logger.error(f"Exceção no arquivo {key}: {exc}")

        # 3. Atualizar Glue (apenas uma vez por tabela)
        if sample_df is not None:
            s3_path = f"s3://{bucket}/silver/{table}/"
            create_or_update_glue_table(glue_db, table, s3_path, sample_df)
            logger.info(f"Sucesso: {processed_count} arquivos processados e Glue atualizado para {table}.")

if __name__ == "__main__":
    run_silver_processing()
