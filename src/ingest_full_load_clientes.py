import psycopg2
from minio import Minio
from io import BytesIO
from minio.error import S3Error
import sys
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

# Objetivo: apenas FULL LOAD da tabela 'cliente' em CSV no caminho:
#   raw/full/data=YYYYMMDD/cliente_full_YYYYMMDD_HHMMSS.csv  (bucket precisa ser minúsculo)


def ensure_bucket_exists(minio_client: Minio, bucket_name: str) -> None:
    """Garante que o bucket exista: se existir, não faz nada; se não existir, cria."""
    try:
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' criado.")
        else:
            print(f"Bucket '{bucket_name}' já existe.")
    except Exception as e:
        print(f"Erro ao verificar/criar bucket '{bucket_name}': {e}")
        sys.exit(1)


def extract_cliente_full_csv(
    engine,
    minio_client: Minio,
    bucket_name: str,
    raw_full_base_path: str,
    db_schema: str,
) -> None:
    """Extrai *somente* a tabela db_schema.cliente e salva como CSV em RAW/full/."""
    print("Iniciando FULL load (CSV) da tabela 'cliente'...")

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM {db_schema}.cliente"), conn)

            if df.empty:
                print("Tabela 'cliente' vazia.")
                return

            now = datetime.now()
            date_partition_str = now.strftime("data=%Y%m%d")
            file_timestamp_str = now.strftime("%Y%m%d_%H%M%S")
            file_name = f"cliente_full_{file_timestamp_str}.csv"
            object_name = f"{raw_full_base_path}{date_partition_str}/{file_name}"

            csv_bytes = df.to_csv(index=False).encode("utf-8")
            csv_buffer = BytesIO(csv_bytes)

            minio_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=csv_buffer,
                length=len(csv_bytes),
                content_type="text/csv",
            )
            print(f"Tabela 'cliente' salva em: '{object_name}' ({len(df)} linhas)")

        print("Ingestão FULL (CSV) da tabela 'cliente' concluída.")

    except Exception as e:
        print(f"Erro durante ingestão da tabela 'cliente': {e}")


def main() -> None:
    """Executa conexão e FULL LOAD da tabela cliente (CSV) em RAW/full/."""
    MINIO_ENDPOINT = "minio:9000"  # ajuste se o host do serviço MinIO for diferente no Codespace
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"

    BUCKET_NAME = "raw"  # ajuste para o bucket desejado

    POSTGRES_HOST = "db"
    POSTGRES_DB = "mydb"
    POSTGRES_USER = "myuser"
    POSTGRES_PASS = "mypassword"
    POSTGRES_PORT = "5432"
    POSTGRES_SCHEMA = "db_loja"

    RAW_FULL_BASE_PATH = "full/"  # destino raiz

    try:
        minio_client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        minio_client.list_buckets()  # sanity check
        ensure_bucket_exists(minio_client, BUCKET_NAME)
    except Exception as e:
        print(f"Erro ao configurar MinIO: {e}")
        return

    try:
        db_string = (
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )
        engine = create_engine(db_string)

        with engine.connect() as _:
            print("Conexão com PostgreSQL bem-sucedida.")

        extract_cliente_full_csv(
            engine=engine,
            minio_client=minio_client,
            bucket_name=BUCKET_NAME,
            raw_full_base_path=RAW_FULL_BASE_PATH,
            db_schema=POSTGRES_SCHEMA,
        )
    except Exception as e:
        print(f"Erro ao conectar ou executar ETL no PostgreSQL: {e}")
    finally:
        print("Script finalizado.")


if __name__ == "__main__":
    main()
