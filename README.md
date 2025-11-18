# my-pyspark-postgres

Pipeline de exemplo em **PySpark** integrando com **PostgreSQL** via JDBC. O objetivo é demonstrar um fluxo de ingestão, transformação e carga (ETL/ELT) usando Spark, SQLs versionadas e um ambiente reprodutível (devcontainer) para desenvolvimento local ou no Codespaces.

> **Status**: projeto educacional / de laboratório. Ajuste os exemplos de tabelas, esquemas e caminhos ao seu caso real.

---

## Sumário

- [Arquitetura & Fluxo](#arquitetura--fluxo)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Pré‑requisitos](#pré-requisitos)
- [Configuração](#configuração)
  - [Variáveis de ambiente](#variáveis-de-ambiente)
  - [Instalação local](#instalação-local)
  - [Devcontainer / Codespaces](#devcontainer--codespaces)
- [Como executar](#como-executar)
  - [Usando ](#usando-spark-submit)[`spark-submit`](#usando-spark-submit)
  - [Usando ](#usando-pythonpyspark-interativo)[`python`](#usando-pythonpyspark-interativo)[/](#usando-pythonpyspark-interativo)[`pyspark`](#usando-pythonpyspark-interativo)[ interativo](#usando-pythonpyspark-interativo)
- [Esquemas e Tabelas](#esquemas-e-tabelas)
- [Pasta ](#pasta-query-sqls)[`query/`](#pasta-query-sqls)[ (SQLs)](#pasta-query-sqls)
- [Boas práticas de escrita no PostgreSQL](#boas-práticas-de-escrita-no-postgresql)
- [Testes rápidos](#testes-rápidos)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)
- [Licença](#licença)

---

## Arquitetura & Fluxo

```text
+-----------+      +-----------+      +--------------+
|  Fonte(s) | ---> |  PySpark  | ---> |   Postgres   |
|  (CSV/..) |      | (ETL/ELT) |      | (DW/DataMart)|
+-----------+      +-----------+      +--------------+
        ingestão             transformação             carga via JDBC
```

1. **Ingestão**: leitura de arquivos (ex.: CSV/Parquet) ou tabelas JDBC.
2. **Transformação**: limpeza, cast de tipos, normalização e regras de negócio em DataFrames Spark.
3. **Carga**: escrita em tabelas PostgreSQL via **driver JDBC** (modo `append`, `overwrite`, etc.).

## Estrutura do Projeto

```
my-pyspark-postgres/
├─ .devcontainer/           # Ambiente de desenvolvimento reproduzível (VS Code/Codespaces)
├─ query/                   # SQLs versionadas (criação/DDL, índices, checks, views)
├─ src/                     # Scripts PySpark (ingestão, transform, load)
├─ requirements.txt         # Dependências Python
└─ README.md                # Este guia
```

> Observação: a árvore acima reflete o layout básico exposto no repositório. Renomeie/adapte as subpastas conforme evoluir a solução (ex.: `src/jobs`, `src/utils`).

## Pré‑requisitos

- **Python 3.10+** (recomendado)
- **Apache Spark 3.x**
- **Java 8+** (JRE/JDK) – Spark depende de JVM
- **PostgreSQL 13+**
- **Driver JDBC do Postgres** (ex.: `org.postgresql:postgresql:42.x`) disponível no Spark

> Em ambientes com devcontainer/Codespaces, a maior parte já vem configurada. Localmente, garanta que `JAVA_HOME` esteja setado e que o driver JDBC seja resolvido por `--packages` ou via `spark.jars`.

## Configuração

### Variáveis de ambiente

Crie um arquivo `.env` na raiz (ou exporte no seu shell/CI) com as credenciais do banco:

```bash
PG_HOST=localhost
PG_PORT=5432
PG_DB=mydb
PG_USER=myuser
PG_PASSWORD=mypassword
PG_SCHEMA=public
```

Você pode consumir isso nos scripts Python com `os.getenv`.

### Instalação local

```bash
# 1) Crie e ative um ambiente virtual
python -m venv .venv && source .venv/bin/activate

# 2) Instale dependências
pip install -r requirements.txt

# 3) (Opcional) Baixe o driver JDBC do Postgres se não usar --packages
#   ex.: wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

### Devcontainer / Codespaces

- Abra o projeto no **VS Code** e aceite a proposta de reabrir em **Dev Container**; ou crie um Codespace do repositório.
- O diretório `.devcontainer/` define imagem base e extensões para um ambiente replicável (Python, Java, Spark, etc.).

## Como executar

### Usando `spark-submit`

```bash
spark-submit \
  --packages org.postgresql:postgresql:42.7.3 \
  --conf spark.sql.session.timeZone=UTC \
  src/main.py \
  --input "data/input/*.csv" \
  --table "${PG_SCHEMA}.fato_exemplo" \
  --mode append
```

- `--packages` garante o driver JDBC em runtime (alternativa: `--jars path/do/postgresql.jar`).
- Ajuste `--input` para sua fonte. Caso o job leia do próprio Postgres, use `spark.read.format("jdbc")`.

### Usando `python`/`pyspark` interativo

```bash
python src/main.py --input data/input/*.csv --table ${PG_SCHEMA}.fato_exemplo --mode overwrite
```

Exemplo (dentro do script):

```python
from pyspark.sql import SparkSession
import os

spark = (SparkSession.builder
    .appName("my-pyspark-postgres")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
    .getOrCreate())

pg_url = f"jdbc:postgresql://{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
props = {"user": os.getenv("PG_USER"), "password": os.getenv("PG_PASSWORD"), "driver": "org.postgresql.Driver"}

# Leitura de CSV (exemplo)
df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv("data/input/*.csv"))

# Transformações
# df = ... (casts, trims, deduplicações, normalizações etc.)

# Escrita no Postgres
df.write.mode("append").jdbc(pg_url, f"{os.getenv('PG_SCHEMA')}.fato_exemplo", properties=props)
```

## Esquemas e Tabelas

> Dica: mantenha **DDL** e artefatos de schema dentro de `query/` para versionar a evolução do banco. Exemplos úteis:

- `query/001_create_schema.sql`
- `query/010_create_dim_clientes.sql`
- `query/020_create_fato_vendas.sql`
- `query/030_indexes.sql`
- `query/040_views_analiticas.sql`

**Boas práticas:**

- Prefira **tipos adequados** (NUMERIC p/ valores monetários; TIMESTAMP p/ datas e horas; TEXT vs VARCHAR…)
- Crie **índices** com parcimônia (avaliar `EXPLAIN ANALYZE`).
- Use **constraints** (PK/FK/UNIQUE/CHECK) para garantir qualidade de dados.

## Pasta `query/` (SQLs)

- Scripts que preparam o banco e/ou materializam views/tabelas auxiliares para leitura pelo Spark.
- Podem ser executados manualmente (psql/pgAdmin) ou por um orquestrador/Makefile (ex.: `make db-setup`).

Exemplo de execução:

```bash
psql "host=$PG_HOST port=$PG_PORT dbname=$PG_DB user=$PG_USER password=$PG_PASSWORD" \
  -f query/001_create_schema.sql
```

## Boas práticas de escrita no PostgreSQL

- **Modo de escrita**: `append` (inserções), `overwrite` (substitui a tabela), `errorifexists` ou `ignore`.
- **Particionamento lógico**: se necessário, particione por data/ano-mês para cargas incrementais.
- **Idempotência**: prefira tabelas *staging* + *MERGE/UPSERT* (ex.: `INSERT ... ON CONFLICT`) para evitar duplicidades.
- **Transações curtas** e **lotes** pequenos reduzem lock contention.
- **Tipos corretos** e **timezone** coerente (UTC) evitam bugs sutis.

## Testes rápidos

- **Smoke test** de conexão JDBC:

```python
spark.read \
  .format("jdbc") \
  .option("url", pg_url) \
  .option("dbtable", "information_schema.tables") \
  .options(**props) \
  .load() \
  .show(10, truncate=False)
```

- **Validação de schema**: compare `df.dtypes` com o esperado e falhe cedo.
- **Qualidade**: conte nulos, verifique chaves únicas e integridade referencial (via SQLs em `query/`).

## Troubleshooting

- **java.lang.ClassNotFoundException: org.postgresql.Driver**: adicione o driver com `--packages org.postgresql:postgresql:42.7.3` ou `--jars`.
- **Timezone/Data**: configure `spark.sql.session.timeZone=UTC` e normalize entradas.
- **Performance**: use `partitionColumn`/`lowerBound`/`upperBound`/`numPartitions` em leituras JDBC grandes.
- **Memória**: ajuste `--executor-memory`/`--driver-memory` e partições (`repartition`) conforme o volume.

## Roadmap

-

## Licença

Este repositório segue a licença do upstream original (se aplicável). Caso não conste, considere **MIT** para fins educacionais e ajuste conforme sua necessidade.

