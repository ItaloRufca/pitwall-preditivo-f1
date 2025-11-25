import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType

# Configuração de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações de Caminhos (S3 ou Local)
# Se S3_BUCKET_NAME estiver definido, usa S3, senão usa local data/
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
if BUCKET_NAME:
    BRONZE_PATH = f"s3a://{BUCKET_NAME}/bronze"
    SILVER_PATH = f"s3a://{BUCKET_NAME}/silver"
else:
    BRONZE_PATH = "data/bronze"
    SILVER_PATH = "data/silver"

def get_spark_session():
    """Cria e retorna uma SparkSession configurada para S3."""
    builder = SparkSession.builder \
        .appName("PitwallPreditivoF1-Silver") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") # Necessário para S3
    
    return builder.getOrCreate()

def process_laps(df):
    """Processamento específico para a tabela laps."""
    numeric_cols = ['lap_duration', 'sector_1_duration', 'sector_2_duration', 'sector_3_duration', 'lap_number']
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
    
    df = df.filter(col("lap_duration").isNotNull())
    df = df.dropDuplicates(['session_key', 'driver_number', 'lap_number'])
    return df

def process_weather(df):
    """Processamento específico para a tabela weather."""
    if 'date' in df.columns:
        df = df.withColumn('date', to_timestamp(col('date')))
        
    numeric_cols = ['air_temperature', 'track_temperature', 'humidity', 'pressure', 'wind_speed', 'rainfall']
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
            
    df = df.dropDuplicates(['session_key', 'date'])
    return df

def process_drivers(df):
    """Processamento específico para a tabela drivers."""
    if 'driver_number' in df.columns:
        df = df.withColumn('driver_number', col('driver_number').cast(IntegerType()))
    
    df = df.dropDuplicates(['session_key', 'driver_number'])
    return df

def process_pit(df):
    """Processamento específico para a tabela pit."""
    numeric_cols = ['lap_number', 'duration', 'pit_duration']
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
            
    df = df.dropDuplicates(['session_key', 'driver_number', 'lap_number'])
    return df

def run_silver_processing():
    spark = get_spark_session()
    logger.info("Spark Session criada.")
    logger.info("Iniciando processamento Silver (Spark)...")

    # Tabelas de interesse
    tables = ["laps", "weather", "drivers", "pit", "session_result", "stints"]

    for table in tables:
        logger.info(f"Processando tabela: {table}")
        
        # Ler Bronze (assumindo CSV do ingestão anterior, ou JSON se mudarmos)
        # Ler Bronze
        # Estrutura: bronze/{table}/year=*/meeting_key=*/session_key=*.csv
        input_path = f"{BRONZE_PATH}/{table}/*/*/*.csv"
        
        try:
            # Ler CSV com header
            df = spark.read.option("header", "true").csv(input_path)
            
            if df.rdd.isEmpty():
                logger.warning(f"Sem dados para {table}")
                continue

            # Transformações
            if table == "laps":
                df = process_laps(df)
            elif table == "weather":
                df = process_weather(df)
            elif table == "drivers":
                df = process_drivers(df)
            elif table == "pit":
                df = process_pit(df)
            
            # Adicionar coluna de partição 'year' se não existir (extrair do path é complexo no Spark read direto)
            # Mas o Spark read particionado (se a estrutura fosse hive-style perfeita) já traria.
            # Como nossa estrutura é customizada (dataset_name=...), o Spark pode não inferir o 'year' automaticamente como coluna
            # a menos que leiamos da raiz com basePath.
            # O ingestão salvou em year={YYYY}. Vamos tentar ler usando basePath para ganhar a coluna year.
            
            # Tentativa de leitura particionada
            # O path é complexo. Vamos ler tudo e salvar overwrite.
            # Para performance real, o ideal seria ler particionado.
            
            output_path = f"{SILVER_PATH}/{table}"
            
            df.write.mode("overwrite").parquet(output_path)
            logger.info(f"Salvo em {output_path}")
            
        except Exception as e:
            logger.error(f"Erro ao processar {table}: {e}")

    spark.stop()

if __name__ == "__main__":
    process_silver()
