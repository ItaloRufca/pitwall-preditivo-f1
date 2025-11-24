import logging
import sys
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from src.modeling.feature_engineering import prepare_training_data, get_spark_session

# Configuração de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class F1PredictorSpark:
    def __init__(self):
        self.spark = get_spark_session()
        self.model = None
        self.pipeline = None
        self.driver_list = []

    def train(self):
        logger.info("Preparando dados com Spark...")
        df = prepare_training_data(self.spark)
        
        if df is None or df.rdd.isEmpty():
            logger.error("Sem dados para treinar.")
            return

        # Guardar lista de pilotos para predição
        self.driver_list = [row['driver_number'] for row in df.select('driver_number').distinct().collect()]

        # Pipeline MLlib
        # 1. Indexar string 'weather_category' -> numero
        indexer = StringIndexer(inputCol="weather_category", outputCol="weather_index", handleInvalid="keep")
        
        # 2. Vetorizar Features
        assembler = VectorAssembler(
            inputCols=["meeting_key", "driver_number", "weather_index"],
            outputCol="features"
        )
        
        # 3. Modelo Random Forest
        rf = RandomForestRegressor(featuresCol="features", labelCol="lap_duration", numTrees=50)
        
        self.pipeline = Pipeline(stages=[indexer, assembler, rf])
        
        # Split
        train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)
        
        logger.info("Treinando modelo...")
        self.model = self.pipeline.fit(train_data)
        
        # Avaliação
        predictions = self.model.transform(test_data)
        evaluator = RegressionEvaluator(labelCol="lap_duration", predictionCol="prediction", metricName="mae")
        mae = evaluator.evaluate(predictions)
        logger.info(f"Modelo treinado. MAE: {mae:.3f}")

    def predict_grid(self, meeting_key, weather_category):
        if not self.model:
            logger.warning("Modelo não treinado.")
            return None

        # Criar DataFrame de input para todos os pilotos
        input_data = []
        for driver in self.driver_list:
            input_data.append((int(meeting_key), int(driver), weather_category))
            
        df_pred = self.spark.createDataFrame(input_data, ["meeting_key", "driver_number", "weather_category"])
        
        # Prever
        predictions = self.model.transform(df_pred)
        
        # Coletar resultados e ordenar (Spark -> Python List -> Pandas/Print)
        results = predictions.select("driver_number", "prediction").orderBy("prediction").collect()
        
        # Formatar output
        grid = []
        for i, row in enumerate(results):
            grid.append({
                "position": i + 1,
                "driver_number": row["driver_number"],
                "predicted_time": row["prediction"]
            })
            
        return grid

def interactive_mode():
    predictor = F1PredictorSpark()
    predictor.train()
    
    if not predictor.model:
        return

    print("\n--- Pitwall Preditivo F1 (Spark Engine) ---")
    
    while True:
        try:
            meeting_input = input("\nInsira o ID da Pista (meeting_key) ou 'sair': ")
            if meeting_input.lower() == 'sair':
                break
            
            meeting_key = int(meeting_input)
            weather_input = input("Insira o Clima (dry/rain): ").lower()
            
            print(f"\nCalculando Grid...")
            grid = predictor.predict_grid(meeting_key, weather_input)
            
            if grid:
                print(f"{'Pos':<5} {'Driver':<10} {'Time':<10}")
                print("-" * 30)
                for row in grid[:20]:
                    print(f"{row['position']:<5} {row['driver_number']:<10} {row['predicted_time']:.3f}")
                    
        except Exception as e:
            print(f"Erro: {e}")

if __name__ == "__main__":
    interactive_mode()
