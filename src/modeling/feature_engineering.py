import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg

BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
if BUCKET_NAME:
    SILVER_PATH = f"s3a://{BUCKET_NAME}/silver"
else:
    SILVER_PATH = "data/silver"

def get_spark_session():
    return SparkSession.builder \
        .appName("PitwallPreditivoF1-Modeling") \
        .getOrCreate()

def prepare_training_data(spark):
    """
    Prepara o dataset para treinamento usando Spark.
    """
    # Ler tabelas Silver (Parquet)
    try:
        laps = spark.read.parquet(f"{SILVER_PATH}/laps")
        weather = spark.read.parquet(f"{SILVER_PATH}/weather")
    except Exception as e:
        print(f"Erro ao ler dados: {e}")
        return None

    # Feature Engineering
    # Categorizar Clima
    weather_agg = weather.groupBy("session_key", "meeting_key").agg(
        avg("rainfall").alias("avg_rainfall"),
        avg("track_temperature").alias("avg_track_temp")
    )
    
    weather_agg = weather_agg.withColumn(
        "weather_category",
        when(col("avg_rainfall") > 0, "rain").otherwise("dry")
    )
    
    # Join
    df = laps.join(weather_agg, on=["session_key", "meeting_key"], how="inner")
    
    # Selecionar colunas e limpar
    df = df.select(
        col("meeting_key").cast("integer"),
        col("driver_number").cast("integer"),
        col("weather_category"),
        col("lap_duration").cast("double")
    ).dropna()
    
    return df
