import sys
import os
import json
import urllib.request
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

# Configuração de Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

BUCKET_NAME = args['S3_BUCKET_NAME']
BASE_URL = "https://api.openf1.org/v1"
ENDPOINTS = ["drivers", "laps", "pit", "weather", "session_result", "stints"]

def fetch_data(endpoint, params=None):
    """Busca dados da API usando urllib (Standard Lib)."""
    url = f"{BASE_URL}/{endpoint}"
    if params:
        query_string = urllib.parse.urlencode(params)
        url = f"{url}?{query_string}"
    
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            if response.status == 200:
                return json.loads(response.read().decode())
    except Exception as e:
        print(f"Erro API {url}: {e}")
    return []

def run_ingestion():
    print(">>> INICIANDO SCRIPT GLUE - DEBUG <<<")
    print(f"Bucket configurado: {BUCKET_NAME}")
    
    # Iterar anos (Isso roda no Driver, o que é ok para orquestração leve)
    # Para paralelismo massivo, poderíamos criar um RDD de anos/sessões, 
    # mas a API tem rate limits, então loop sequencial no driver é mais seguro.
    years = range(2020, 2026)

    for year in years:
        print(f"Processando Ano: {year}")
        sessions = fetch_data("sessions", params={"year": year})
        
        if not sessions:
            continue

        for session in sessions:
            meeting_key = session.get('meeting_key')
            session_key = session.get('session_key')
            
            if not meeting_key or not session_key:
                continue

            print(f"Sessão: {session_key}")

            for dataset_name in ENDPOINTS:
                data = fetch_data(dataset_name, params={"session_key": session_key})
                
                if not data:
                    continue

                # Criar DataFrame Spark
                # Spark infere schema, mas para JSON vazio ou complexo pode falhar.
                # Como os dados vêm em memória (lista de dicts), createDataFrame funciona bem.
                try:
                    # RDD parallelize distribui os dados para os workers para escrita
                    rdd = sc.parallelize(data)
                    
                    # Se a lista for vazia, o createDataFrame falha sem schema.
                    if rdd.isEmpty():
                        continue
                        
                    # Deixar o Spark inferir o schema dos dicts
                    df = spark.read.json(rdd)
                    
                    # Caminho de saída
                    # bronze/{dataset_name}/year=.../meeting_key=.../session_key=...
                    output_path = f"s3://{BUCKET_NAME}/bronze/{dataset_name}/year={year}/meeting_key={meeting_key}/session_key={session_key}"
                    
                    # Escrever CSV (com header para facilitar leitura no Silver)
                    df.write.mode("overwrite").option("header", "true").csv(output_path)
                    
                except Exception as e:
                    print(f"Erro ao processar {dataset_name} na sessão {session_key}: {e}")

    job.commit()

if __name__ == "__main__":
    run_ingestion()
