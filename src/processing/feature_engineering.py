import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()
BUCKET = os.environ.get('S3_BUCKET_NAME')

# --- CONFIGURAÇÃO DE TABELAS ---
RACE_TABLE = "gold_race_wt"
PRACTICE_TABLE = "gold_practice_wt"

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
    df_race = load_table(RACE_TABLE)
    df_practice = load_table(PRACTICE_TABLE)

    if df_practice is not None:
        # Colunas de identificação para o Join
        join_keys = ['meeting_key', 'driver_number']
        
        # Prefixar todas as outras colunas com 'practice_'
        cols_to_rename = {col: f"practice_{col}" for col in df_practice.columns if col not in join_keys}
        df_practice_renamed = df_practice.rename(columns=cols_to_rename)
    else:
        print("AVISO: df_practice não carregado.")
        return

    if df_race is not None and df_practice is not None:
        # Left Join: Queremos prever todas as corridas, mesmo se faltar dado de treino
        df_final = pd.merge(
            df_race, 
            df_practice_renamed, 
            on=['meeting_key', 'driver_number'], 
            how='left',
            suffixes=('', '_dup') # Caso sobre alguma duplicada não mapeada
        )
        
        # Remover duplicadas se houver
        df_final = df_final.loc[:, ~df_final.columns.str.endswith('_dup')]
        
        # Tratamento de Nulos nas Features de Prática
        practice_cols = [c for c in df_final.columns if c.startswith('practice_') and pd.api.types.is_numeric_dtype(df_final[c])]
        
        print(f"Tratando nulos em {len(practice_cols)} colunas de prática...")
        for col in practice_cols:
            # Mediana da corrida específica
            meeting_median = df_final.groupby('meeting_key')[col].transform('median')
            df_final[col] = df_final[col].fillna(meeting_median)
            # Se ainda sobrar (toda a corrida sem dados), preenche com 0
            df_final[col] = df_final[col].fillna(0)
            
        # --- TRATAMENTO DE CATEGÓRICAS ---
        cat_cols = ['team_name', 'circuit_short_name', 'country_name', 'first_stint_compound']
        valid_cats = [c for c in cat_cols if c in df_final.columns]
        
        if valid_cats:
            df_final = pd.get_dummies(df_final, columns=valid_cats, drop_first=True)
            print(f"One-Hot Encoding aplicado em: {valid_cats}")

        # --- DIVISÃO TEMPORAL ---
        # Convert 'year' column to integer to allow numerical comparison (BUG FIX)
        if 'year' in df_final.columns:
            df_final['year'] = df_final['year'].astype(int)
        
        train_data = df_final[df_final['year'] <= 2024]
        # Se houver dados de 2025, use-os como teste real. Se não, use um split de validação em 2024.
        test_data = df_final[df_final['year'] == 2025]

        if test_data.empty:
            print("AVISO: Base 2025 vazia. Usando cortes anteriores (Treino < 2024, Teste = 2024).")
            train_data = df_final[df_final['year'] < 2024]
            test_data = df_final[df_final['year'] == 2024]

        print(f"Treino: {train_data.shape}")
        print(f"Teste: {test_data.shape}")
        
        # Salvar
        print("Salvando Parquets...")
        train_data.to_parquet(f"s3://{BUCKET}/gold/train_data.parquet")
        test_data.to_parquet(f"s3://{BUCKET}/gold/test_data.parquet")
        print("Concluído!")
    else:
        print("Dados insuficientes para merge.")

if __name__ == "__main__":
    main()
