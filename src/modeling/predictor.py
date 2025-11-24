import pandas as pd
import numpy as np
import logging
import sys
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error
from src.modeling.feature_engineering import prepare_training_data

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class F1Predictor:
    def __init__(self):
        self.model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        self.le_weather = LabelEncoder()
        self.is_trained = False
        self.driver_list = [] # Lista de pilotos conhecidos

    def train(self):
        df = prepare_training_data()
        if df.empty:
            logger.error("Sem dados para treinar.")
            return

        # Preprocessing
        # Codificar Clima: dry=0, rain=1 (ou via LabelEncoder)
        df['weather_encoded'] = self.le_weather.fit_transform(df['weather_category'])
        
        # Features e Target
        # Usamos meeting_key como proxy para a Pista. 
        # Idealmente teríamos 'circuit_key', mas meeting_key varia por ano.
        # Para simplificar este MVP, vamos assumir que o usuário insere o meeting_key de referência 
        # OU vamos treinar apenas com dados genéricos de piloto/clima (o que seria ruim).
        # MELHOR ABORDAGEM MVP: Usar meeting_key como categórico é perigoso se o ID mudar.
        # Vamos tentar usar 'location' se tivéssemos, mas não temos fácil aqui.
        # Vamos usar meeting_key mesmo, assumindo que o modelo aprende "pistas históricas".
        
        X = df[['meeting_key', 'weather_encoded', 'driver_number']]
        y = df['lap_duration']
        
        self.driver_list = df['driver_number'].unique().tolist()
        
        logger.info("Treinando modelo (Random Forest)...")
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        self.model.fit(X_train, y_train)
        self.is_trained = True
        
        # Avaliação
        predictions = self.model.predict(X_test)
        mae = mean_absolute_error(y_test, predictions)
        logger.info(f"Modelo treinado. MAE (Erro Médio Absoluto): {mae:.3f} segundos")

    def predict_grid(self, meeting_key, weather_category):
        if not self.is_trained:
            logger.warning("Modelo não treinado. Treinando agora...")
            self.train()
            if not self.is_trained:
                return None

        try:
            weather_encoded = self.le_weather.transform([weather_category])[0]
        except ValueError:
            logger.error(f"Clima '{weather_category}' desconhecido. Use: {self.le_weather.classes_}")
            return None

        # Gerar previsões para todos os pilotos conhecidos
        # Criar DataFrame de input
        input_data = []
        for driver in self.driver_list:
            input_data.append({
                'meeting_key': meeting_key,
                'weather_encoded': weather_encoded,
                'driver_number': driver
            })
        
        X_pred = pd.DataFrame(input_data)
        
        # Prever tempos
        predicted_times = self.model.predict(X_pred)
        
        # Montar Grid
        grid = pd.DataFrame({
            'driver_number': self.driver_list,
            'predicted_lap_time': predicted_times
        })
        
        # Ordenar por tempo (menor é melhor)
        grid = grid.sort_values('predicted_lap_time').reset_index(drop=True)
        grid['position'] = grid.index + 1
        
        return grid

def interactive_mode():
    predictor = F1Predictor()
    predictor.train()
    
    if not predictor.is_trained:
        return

    print("\n--- Pitwall Preditivo F1 ---")
    print("Dica: Use meeting_key=1208 (Interlagos 2023) ou procure chaves na API.")
    
    while True:
        try:
            meeting_input = input("\nInsira o ID da Pista (meeting_key) ou 'sair': ")
            if meeting_input.lower() == 'sair':
                break
            
            meeting_key = int(meeting_input)
            
            weather_input = input("Insira o Clima (dry/rain): ").lower()
            if weather_input not in ['dry', 'rain']:
                print("Clima inválido. Use 'dry' ou 'rain'.")
                continue
                
            print(f"\nCalculando Grid para Meeting {meeting_key} com clima {weather_input}...")
            grid = predictor.predict_grid(meeting_key, weather_input)
            
            if grid is not None:
                print("\n--- Grid de Largada Previsto ---")
                print(grid[['position', 'driver_number', 'predicted_lap_time']].head(20).to_string(index=False))
                
        except ValueError:
            print("Entrada inválida.")
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    interactive_mode()
