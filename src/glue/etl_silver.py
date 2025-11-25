import sys
import os
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import DoubleType
import os

# 1. Inicialização do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET_NAME'])

# CREDENCIAIS AWS
# Em produção, use IAM Roles ou variáveis de ambiente.
# Se necessário hardcodar para testes, substitua abaixo.
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "INSIRA_SUA_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "INSIRA_SUA_SECRET_KEY")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configurar Credenciais no Hadoop Conf (para S3A e S3)
if AWS_ACCESS_KEY != "INSIRA_SUA_ACCESS_KEY":
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", AWS_SECRET_KEY)

# Configurações
BUCKET_NAME = args["handson-datalake-prd"]
BRONZE_PATH = f"s3://{BUCKET_NAME}/bronze"
SILVER_PATH = f"s3://{BUCKET_NAME}/silver"

def process_laps(df):
    """Processamento específico para a tabela laps."""
    numeric_cols = ['lap_duration', 'sector_1_duration', 'sector_2_duration', 'sector_3_duration']
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
    
    df = df.filter(col("lap_duration").isNotNull())
    return df

def process_weather(df):
    """Processamento específico para a tabela weather."""
    if 'date' in df.columns:
        df = df.withColumn('date', to_timestamp(col('date')))
        
    numeric_cols = ['air_temperature', 'track_temperature', 'humidity', 'pressure', 'wind_speed', 'rainfall']
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(DoubleType()))
    return df

# 2. Execução do ETL
tables = ["laps", "weather", "session_result", "stints"]

for table in tables:
    print(f"Processando tabela: {table}")
    
    # Leitura do Bronze (CSV)
    # Nova estrutura: bronze/{table}/year=*/meeting_key=*/session_key=*.csv
    input_path = f"{BRONZE_PATH}/{table}/*/*/*.csv"
    
    try:
        df = spark.read.option("header", "true").csv(input_path)
        
        if df.rdd.isEmpty():
            print(f"Sem dados para {table}")
            continue

        # Transformações
        if table == "laps":
            df = process_laps(df)
        elif table == "weather":
            df = process_weather(df)
        
        # Escrita no Silver (Parquet)
        output_path = f"{SILVER_PATH}/{table}"
        
        # Usando Spark write padrão. 
        # Em produção Glue, as vezes converte-se para DynamicFrame para usar o Glue Catalog,
        # mas para escrita direta em S3, o Spark write é eficiente e simples.
        df.write.mode("overwrite").parquet(output_path)
        print(f"Salvo em {output_path}")
        
    except Exception as e:
        print(f"Erro ao processar {table}: {e}")

# 3. Finalização
job.commit()
