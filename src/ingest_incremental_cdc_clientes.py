"""
Ingestão incremental (CDC) para a tabela db_loja.cliente
- Consome alterações do slot lógico (padrão: data_sync_slot) via test_decoding
- Filtra apenas a tabela alvo (schema.tabela)
- Persiste em CSV no MinIO no padrão:
    raw/inc/data=YYYYMMDD/cliente_cdc_YYYYMMDD_HHMMSS.csv

Observações:
- Bucket 'raw' já deve existir — este script não cria nenhum bucket.
- Por padrão usa test_decoding (legível). Se seu slot estiver com pgoutput, você
  não conseguirá ler mudanças com as funções *_get_changes; nesse caso, crie um slot
  com plugin test_decoding ou adapte para wal2json.
"""

import os
import re
import json
from io import BytesIO
from datetime import datetime
from typing import List, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from minio import Minio

_TBL_RE = re.compile(r"^table\s+([^.]+)\.([:A-Za-z0-9_]+):\s+(INSERT|UPDATE|DELETE):\s+(.*)$")
_KV_RE = re.compile(r"([A-Za-z0-9_]+)\[[^\]]+\]:([^\s].*?)(?=\s+[A-Za-z0-9_]+\[|\s*$)")

def parse_test_decoding_row(raw: str) -> Dict:
    m = _TBL_RE.match(raw.strip())
    if not m:
        return {}
    schema, table, op, rest = m.groups()
    cols = {}
    for km in _KV_RE.finditer(rest):
        k, v = km.groups()
        v = v.strip()
        if len(v) >= 2 and v[0] == "'" and v[-1] == "'":
            v = v[1:-1]
        cols[k] = v
    return {"schema": schema, "table": table, "op": op, "cols": cols}

def fetch_cdc_changes(engine, slot_name: str, limit: int) -> List[Dict]:
    rows = []
    with engine.connect() as conn:
        q = text(
            "SELECT lsn, xid, data FROM pg_logical_slot_get_changes(:slot, NULL, :limit, 'include-xids', '0', 'skip-empty-xacts', '1')"
        )
        res = conn.execute(q, {"slot": slot_name, "limit": limit})
        for lsn, xid, data in res:
            rows.append({"lsn": lsn, "xid": xid, "data": data})
    return rows

def process_cdc_for_cliente(raw_rows: List[Dict], target_schema: str, target_table: str) -> pd.DataFrame:
    out = []
    capture_ts = datetime.now().isoformat()
    for r in raw_rows:
        parsed = parse_test_decoding_row(r.get("data", ""))
        if not parsed:
            continue
        if parsed.get("schema") == target_schema and parsed.get("table") == target_table:
            out.append({
                "capture_ts": capture_ts,
                "lsn": r["lsn"],
                "op": parsed["op"],
                "columns": json.dumps(parsed["cols"], ensure_ascii=False)
            })
    return pd.DataFrame(out) if out else pd.DataFrame()

def write_csv_to_minio(minio_client: Minio, bucket: str, base_path: str, df: pd.DataFrame, prefix: str) -> str:
    now = datetime.now()
    date_partition_str = now.strftime("data=%Y%m%d")
    file_timestamp_str = now.strftime("%Y%m%d_%H%M%S")
    file_name = f"{prefix}_{file_timestamp_str}.csv"
    object_name = f"{base_path}{date_partition_str}/{file_name}"
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    buf = BytesIO(csv_bytes)
    minio_client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=buf,
        length=len(csv_bytes),
        content_type="text/csv",
    )
    return object_name

def main() -> None:
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    BUCKET_NAME = "raw"  # bucket existente

    PGHOST = os.getenv("PGHOST", "db")
    PGDATABASE = os.getenv("PGDATABASE", "mydb")
    PGUSER = os.getenv("PGUSER", "myuser")
    PGPASSWORD = os.getenv("PGPASSWORD", "mypassword")
    PGPORT = os.getenv("PGPORT", "5432")
    PGSCHEMA = os.getenv("PGSCHEMA", "db_loja")

    CDC_SLOT_NAME = os.getenv("CDC_SLOT_NAME", "data_sync_slot")
    CDC_BATCH_LIMIT = int(os.getenv("CDC_BATCH_LIMIT", "1000"))

    RAW_INC_BASE_PATH = "inc/"
    TARGET_TABLE = "cliente"

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    db_string = f"postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{PGDATABASE}"
    engine = create_engine(db_string)

    print(f"Consumindo CDC do slot '{CDC_SLOT_NAME}' (limite={CDC_BATCH_LIMIT})...")
    raw_rows = fetch_cdc_changes(engine, CDC_SLOT_NAME, CDC_BATCH_LIMIT)
    if not raw_rows:
        print("Nenhuma mudança para processar.")
        return

    df = process_cdc_for_cliente(raw_rows, PGSCHEMA, TARGET_TABLE)
    if df.empty:
        print("Sem mudanças da tabela alvo db_loja.cliente nesta leitura.")
        return

    dest = write_csv_to_minio(minio_client, BUCKET_NAME, RAW_INC_BASE_PATH, df, prefix="cliente_cdc")
    print(f"Arquivo incremental gravado em: '{dest}' ({len(df)} registros)")

if __name__ == "__main__":
    main()