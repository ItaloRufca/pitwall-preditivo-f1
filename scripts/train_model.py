
import os
import pandas as pd
import numpy as np
import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from dotenv import load_dotenv

# 1. Setup
load_dotenv()
BUCKET = os.environ.get('S3_BUCKET_NAME')
MODEL_DIR = os.path.join(os.getcwd(), 'notebooks')
MODEL_PATH = os.path.join(MODEL_DIR, 'rf_f1_model.joblib')
FEATURES_PATH = os.path.join(MODEL_DIR, 'model_features.json')

def main():
    print("--- Treinamento do Modelo (Random Forest - Weather Aware) ---")
    
    # 2. Carregar Dados
    print("Carregando parquets...")
    try:
        # Check local/cache or S3. Assuming environment has access.
        train_df = pd.read_parquet(f"s3://{BUCKET}/gold/train_data.parquet")
        test_df = pd.read_parquet(f"s3://{BUCKET}/gold/test_data.parquet")
        print(f"Treino: {train_df.shape}, Teste: {test_df.shape}")
    except Exception as e:
        print(f"Erro ao carregar: {e}")
        return

    # 3. Preparação
    TARGET = 'final_position'
    
    # Colunas para remover (IDs, Vazamento ou não-numéricas originais)
    ignore_cols = [
        TARGET, 
        'meeting_key', 'session_key', 'driver_number', 
        'date_start', 'date_end', 
        'session_name', 'session_type', 
        'year',
        'key', 
        'country_name', 'circuit_short_name', 'team_name', 'first_stint_compound' # String columns if not dummy encoded properly or original leftovers
    ]
    
    # Drop columns that are object type (strings) to be safe for RF
    # Dummies were created in update_features, so originals might still be there or not.
    # Selecting numeric only is safest.
    
    # Filtrar features candidatos
    candidates = [c for c in train_df.columns if c not in ignore_cols]
    
    # Drop rows where target is NaN
    train_df = train_df.dropna(subset=[TARGET])
    test_df = test_df.dropna(subset=[TARGET])
    
    # Garantir apenas numéricas
    X_train = train_df[candidates].select_dtypes(include=[np.number, bool]).copy()
    y_train = train_df[TARGET]
    
    X_test = test_df[candidates].select_dtypes(include=[np.number, bool]).copy()
    y_test = test_df[TARGET]
    
    # Fill remaining NaNs with 0 (Standard RF requirement, though update_features did most work)
    X_train = X_train.fillna(0)
    X_test = X_test.fillna(0)
    
    # Alinhar colunas (Ensure same columns in test as train)
    # Get common columns
    common_cols = [c for c in X_train.columns if c in X_test.columns]
    X_train = X_train[common_cols]
    X_test = X_test[common_cols]
    
    print(f"Features finais: {len(X_train.columns)}")
    
    # Verify key features exist
    weather_in = [c for c in X_train.columns if 'rain' in c or 'temp' in c]
    relative_in = [c for c in X_train.columns if 'ratio_' in c or 'diff_' in c]
    print(f"Features de Clima presentes: {weather_in}")
    print(f"Features Relativas presentes: {relative_in}")

    # 4. Treinamento
    # Using Random Forest which is robust and handles non-linearities well
    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=10,
        random_state=42,
        n_jobs=-1  # Use all cores
    )
    
    print("Treinando Random Forest...")
    model.fit(X_train, y_train)
    
    # 5. Avaliação
    y_pred = model.predict(X_test)
    y_pred = np.clip(y_pred, 1, 20)
    
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    
    print(f"\n--- Resultados ---")
    print(f"MAE: {mae:.4f}")
    print(f"RMSE: {rmse:.4f}")
    
    # Feature Importance (Top 10)
    print("\n--- Top 10 Features ---")
    importances = model.feature_importances_
    indices = np.argsort(importances)[-10:]
    for i in indices:
        print(f"{X_train.columns[i]}: {importances[i]:.4f}")

    # 6. Salvar Modelo e Metadados
    print(f"\nSalvando modelo em: {MODEL_PATH}")
    joblib.dump(model, MODEL_PATH)
    
    # Save feature names for inference alignment
    import json
    with open(FEATURES_PATH, 'w') as f:
        json.dump(list(X_train.columns), f)
        
    print("Modelo e lista de features salvos.")

if __name__ == "__main__":
    main()
