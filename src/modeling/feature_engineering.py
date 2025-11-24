import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

SILVER_PATH = "data/silver"

def load_silver_table(table_name):
    """Carrega uma tabela da camada Silver (Parquet)."""
    path = os.path.join(SILVER_PATH, table_name)
    if not os.path.exists(path):
        logger.warning(f"Tabela {table_name} não encontrada em {path}")
        return pd.DataFrame()
    
    # Lê o dataset particionado
    return pd.read_parquet(path)

def prepare_training_data():
    """
    Prepara o dataset para treinamento.
    Junta Laps, Weather e Session Results.
    """
    logger.info("Carregando dados da camada Silver...")
    laps = load_silver_table("laps")
    weather = load_silver_table("weather")
    # session_result = load_silver_table("session_result") # Pode ser usado para filtrar pilotos ativos
    
    if laps.empty or weather.empty:
        logger.error("Dados insuficientes para treino.")
        return pd.DataFrame()

    # Feature Engineering Simples
    
    # 1. Categorizar Clima (Simplificação: Média de chuva por sessão)
    # Agrupar weather por session_key
    weather_agg = weather.groupby(['session_key', 'meeting_key']).agg({
        'rainfall': 'mean',
        'air_temperature': 'mean',
        'track_temperature': 'mean'
    }).reset_index()
    
    weather_agg['weather_category'] = weather_agg['rainfall'].apply(
        lambda x: 'rain' if x > 0 else 'dry'
    )
    
    # 2. Juntar com Laps
    # Laps tem session_key e meeting_key
    logger.info("Unindo tabelas...")
    df = pd.merge(laps, weather_agg, on=['session_key', 'meeting_key'], how='inner')
    
    # Selecionar colunas relevantes
    # Features: meeting_key (Pista), weather_category (Clima), driver_number (Piloto)
    # Target: lap_duration
    
    # Limpeza final
    df = df.dropna(subset=['lap_duration', 'driver_number'])
    
    # Converter driver_number para string/category se necessário, ou manter int
    df['driver_number'] = df['driver_number'].astype(int)
    
    logger.info(f"Dataset de treino preparado com {len(df)} registros.")
    return df
