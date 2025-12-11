
import os
import pandas as pd
import numpy as np
import s3fs
from dotenv import load_dotenv

# 1. Configurar Ambiente
load_dotenv()
BUCKET = os.environ.get('S3_BUCKET_NAME')

if not BUCKET:
    raise ValueError("S3_BUCKET_NAME not set in .env")

print(f"Bucket: {BUCKET}")

# --- FUNÇÕES DE CARREGAMENTO ---
def load_table(table_name):
    print(f"Carregando: {table_name}...")
    try:
        path = f"s3://{BUCKET}/gold/{table_name}/"
        df = pd.read_parquet(path)
        print(f"-> Sucesso! Shape: {df.shape}")
        return df
    except Exception as e:
        print(f"-> Erro ao carregar {table_name}: {e}")
        return None

def main():
    # 2. Carregar Dados
    df_race = load_table("gold_race_widetable")
    df_practice = load_table("gold_practice_widetable")
    
    if df_race is None or df_practice is None:
        print("Erro: Tabelas não encontradas.")
        return

    # 3. Renomear Prática (Preditores)
    join_keys = ['meeting_key', 'driver_number']
    cols_to_rename = {col: f"practice_{col}" for col in df_practice.columns if col not in join_keys}
    df_practice_renamed = df_practice.rename(columns=cols_to_rename)
    
    print("Colunas de prática renomeadas.")

    # 4. Join (Consolidação)
    # Left Join: Queremos prever todas as corridas
    df_final = pd.merge(
        df_race, 
        df_practice_renamed, 
        on=['meeting_key', 'driver_number'], 
        how='left',
        suffixes=('', '_dup')
    )
    df_final = df_final.loc[:, ~df_final.columns.str.endswith('_dup')]
    print(f"Shape pós-Join: {df_final.shape}")

    # 5. Tratamento de Nulos (Prática)
    practice_cols = [c for c in df_final.columns if c.startswith('practice_') and pd.api.types.is_numeric_dtype(df_final[c])]
    
    print(f"Tratando nulos em {len(practice_cols)} colunas de prática...")
    for col in practice_cols:
        # Mediana do meeting
        meeting_median = df_final.groupby('meeting_key')[col].transform('median')
        df_final[col] = df_final[col].fillna(meeting_median)
        # Fallback global
        df_final[col] = df_final[col].fillna(0)

    # --- NOVO: FEATURES RELATIVAS & CLIMA ---
    print("\n--- Engenharia de Features: Clima e Métricas Relativas ---")
    
    # Garantir rain_flag numérico
    if 'rain_flag' in df_final.columns:
        df_final['rain_flag'] = df_final['rain_flag'].fillna(0).astype(int)
        print("rain_flag convertido para int.")
    else:
        print("AVISO: rain_flag não encontrado!")

    # Calcular médias do meeting para normalizar
    if 'avg_lap_time' in df_final.columns:
        print("Criando relativas para avg_lap_time...")
        meeting_avg = df_final.groupby('meeting_key')['avg_lap_time'].transform('mean')
        df_final['meeting_avg_lap_time'] = meeting_avg
        
        # Ratio: < 1.0 = Rápido, > 1.0 = Lento (Independente de chuva)
        df_final['ratio_avg_lap_time'] = df_final['avg_lap_time'] / (meeting_avg + 1e-6)
        
        # Diff: Negativo = Rápido
        df_final['diff_avg_lap_time'] = df_final['avg_lap_time'] - meeting_avg

    if 'best_lap_time' in df_final.columns:
        print("Criando relativas para best_lap_time...")
        meeting_best_avg = df_final.groupby('meeting_key')['best_lap_time'].transform('mean')
        df_final['meeting_best_lap_time'] = meeting_best_avg
        df_final['ratio_best_lap_time'] = df_final['best_lap_time'] / (meeting_best_avg + 1e-6)

    print("Features relativas concluídas.")

    # 6. Encoding e Split
    cat_cols = ['team_name', 'circuit_short_name', 'country_name', 'first_stint_compound']
    valid_cats = [c for c in cat_cols if c in df_final.columns]
    
    if valid_cats:
        df_final = pd.get_dummies(df_final, columns=valid_cats, drop_first=True)
        print(f"One-Hot Encoding aplicado em: {valid_cats}")

    # Split
    df_final['year'] = df_final['year'].astype(int)
    train_data = df_final[df_final['year'] <= 2024]
    test_data = df_final[df_final['year'] == 2025]
    
    if test_data.empty:
        print("AVISO: 2025 vazio. Usando 2024 como teste.")
        train_data = df_final[df_final['year'] < 2024]
        test_data = df_final[df_final['year'] == 2024]

    print(f"Treino: {train_data.shape}")
    print(f"Teste: {test_data.shape}")

    # 7. Salvar
    print("Salvando arquivos parquet...")
    train_data.to_parquet(f"s3://{BUCKET}/gold/train_data.parquet")
    test_data.to_parquet(f"s3://{BUCKET}/gold/test_data.parquet")
    print("Concluído com sucesso!")

if __name__ == "__main__":
    main()
