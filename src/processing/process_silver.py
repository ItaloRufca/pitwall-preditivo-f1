import os
import glob
import json
import logging
import pandas as pd
from tqdm import tqdm
from pathlib import Path

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configurações de Caminhos
BRONZE_PATH = "data/bronze"
SILVER_PATH = "data/silver"

# Tabelas de interesse
TARGET_TABLES = ["laps", "weather", "session_result", "stints"]

def ensure_dir(path):
    """Garante que o diretório existe."""
    os.makedirs(path, exist_ok=True)

def extract_metadata_from_path(file_path):
    """
    Extrai year, meeting_key e session_key do caminho do arquivo.
    Estrutura esperada: .../year={YYYY}/meeting_key={KEY}/session_key={KEY}/{DATASET}.json
    """
    path = Path(file_path)
    parts = path.parts
    
    metadata = {}
    dataset_name = path.stem # Nome do arquivo sem extensão
    
    # Percorre as partes do caminho de trás para frente para encontrar as chaves
    for part in reversed(parts[:-1]): # Ignora o nome do arquivo
        if part.startswith("year="):
            metadata["year"] = int(part.split("=")[1])
        elif part.startswith("meeting_key="):
            metadata["meeting_key"] = int(part.split("=")[1])
        elif part.startswith("session_key="):
            metadata["session_key"] = int(part.split("=")[1])
            
    return dataset_name, metadata

def process_laps(df):
    """Processamento específico para a tabela laps."""
    # Converter durações para numérico (segundos)
    numeric_cols = ['lap_duration', 'sector_1_duration', 'sector_2_duration', 'sector_3_duration']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remover voltas sem tempo (crítico para análise de ritmo)
    df = df.dropna(subset=['lap_duration'])
    
    return df

def process_weather(df):
    """Processamento específico para a tabela weather."""
    # Converter data para datetime
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        
    numeric_cols = ['air_temperature', 'track_temperature', 'humidity', 'pressure', 'wind_speed', 'rainfall']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    return df

def process_session_result(df):
    """Processamento específico para a tabela session_result."""
    numeric_cols = ['position', 'points', 'grid_position']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def process_stints(df):
    """Processamento específico para a tabela stints."""
    numeric_cols = ['lap_start', 'lap_end', 'stint_number']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def process_file(file_path):
    """Lê, transforma e retorna um DataFrame."""
    try:
        dataset_name, metadata = extract_metadata_from_path(file_path)
        
        if dataset_name not in TARGET_TABLES:
            return None, None

        with open(file_path, 'r') as f:
            data = json.load(f)
            
        if not data:
            return None, None

        df = pd.DataFrame(data)
        
        # Adicionar metadados
        for key, value in metadata.items():
            df[key] = value
            
        # Transformações Específicas
        if dataset_name == "laps":
            df = process_laps(df)
        elif dataset_name == "weather":
            df = process_weather(df)
        elif dataset_name == "session_result":
            df = process_session_result(df)
        elif dataset_name == "stints":
            df = process_stints(df)
            
        return dataset_name, df
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {file_path}: {e}")
        return None, None

def save_silver(df, dataset_name):
    """Salva o DataFrame na camada Silver em formato Parquet, particionado por ano."""
    if df is None or df.empty:
        return

    output_dir = os.path.join(SILVER_PATH, dataset_name)
    ensure_dir(output_dir)
    
    try:
        # Salvar particionado por ano
        # O pyarrow/pandas cria a estrutura de diretórios automaticamente com partition_cols
        df.to_parquet(
            output_dir,
            engine='pyarrow',
            partition_cols=['year'],
            index=False,
            existing_data_behavior='overwrite_or_ignore' # Ou 'delete_matching' dependendo da necessidade de idempotência
        )
        # logger.info(f"Dados salvos em {output_dir}") # Verbose demais se for por arquivo
    except Exception as e:
        logger.error(f"Erro ao salvar parquet para {dataset_name}: {e}")

def run_processing():
    """Função principal de processamento."""
    logger.info("Iniciando processamento Silver...")
    
    # Encontrar todos os arquivos JSON recursivamente
    search_pattern = os.path.join(BRONZE_PATH, "**", "*.json")
    files = glob.glob(search_pattern, recursive=True)
    
    logger.info(f"Encontrados {len(files)} arquivos na camada Bronze.")
    
    if not files:
        logger.warning("Nenhum arquivo encontrado. Verifique o caminho data/bronze.")
        return

    # Processar arquivos
    # Agrupar DataFrames por dataset para salvar em batches (opcional, mas melhor para parquet se couber na memória)
    # Como são muitos arquivos pequenos, processar um a um e fazer append pode ser lento ou gerar muitos arquivos pequenos.
    # Uma abordagem melhor para "Big Data" local seria acumular em memória por dataset e salvar.
    # Dado o volume de F1 (não é absurdo), vou acumular em listas e concatenar antes de salvar.
    
    data_buffer = {name: [] for name in TARGET_TABLES}
    
    for file_path in tqdm(files, desc="Processando arquivos"):
        dataset_name, df = process_file(file_path)
        if dataset_name and df is not None and not df.empty:
            data_buffer[dataset_name].append(df)
            
    # Salvar tabelas consolidadas
    for dataset_name, df_list in data_buffer.items():
        if df_list:
            logger.info(f"Consolidando e salvando tabela: {dataset_name}...")
            full_df = pd.concat(df_list, ignore_index=True)
            save_silver(full_df, dataset_name)
            logger.info(f"Tabela {dataset_name} processada com sucesso. Total de linhas: {len(full_df)}")
        else:
            logger.info(f"Nenhum dado processado para a tabela {dataset_name}.")

    logger.info("Processamento Silver finalizado.")

if __name__ == "__main__":
    run_processing()
