# Northwind ELT Pipeline

An end-to-end ELT pipeline that extracts data from a PostgreSQL database (Northwind) and a CSV file, stages it as JSONL on disk, loads it into a destination PostgreSQL database, and produces a final dataset by joining orders with their order details in SQL. The pipeline is orchestrated with Apache Airflow, uses Meltano for extraction, and runs entirely in Docker.

Built as a data engineering code challenge.

## Architecture

```
                 ┌─────────────────────┐
 Northwind DB ──▶│ extract_postgres    │──┐
 (PostgreSQL)    │ Meltano tap-postgres│  │
                 └─────────────────────┘  │   JSONL     ┌────────────────┐    ┌──────────────────┐
                                          ├──files────▶ │ load_to_final  │──▶ │ PostgreSQL (final)│
                 ┌─────────────────────┐  │             │ Python/psycopg2│    │ SQL JOIN          │
 order_details ─▶│ extract_csv         │──┘             └────────────────┘    └──────────────────┘
 .csv            │ Meltano tap-csv     │
                 └─────────────────────┘
```

The DAG `northwind_pipeline` runs the two extractions in parallel, then loads both results into the destination database:

```
start >> [extract_postgres, extract_csv] >> load_to_final_db >> end
```

- **Schedule:** daily, with `max_active_runs=1` so the same pipeline never runs twice at once
- **Retries:** 1 retry with a 5-minute delay
- **Logical date:** each run receives the Airflow logical date, so a specific day can be reprocessed with a backfill

## Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 3.1.8 |
| Extraction | Meltano 3.3.0 (`tap-postgres`, `tap-csv`, `target-jsonl`) |
| Load and processing | Python 3.12, pandas 2.3.3, psycopg2 2.9.11 |
| Storage | PostgreSQL (source and destination) |
| Infrastructure | Docker and Docker Compose |

## Design decisions

- **Meltano in its own virtualenv.** It runs in a separate environment (`/opt/meltano-venv`) to avoid dependency conflicts with Airflow.
- **Pinned versions.** Dependency versions are fixed in the Dockerfile, so builds are reproducible in a 100% Docker setup.
- **No secrets in the repository.** All credentials come from `.env` files (git-ignored). Only `.env.example` templates with placeholders are versioned, and Docker Compose fails fast if a required variable is missing.
- **Runtime artifacts are not versioned.** The JSONL files in `data/` are generated at run time. Only `output/orders_with_details.csv` is versioned, as evidence of the final join.
- **Final transformation in SQL.** The result is a `JOIN` between `orders` and `order_details`, executed in the destination database.

## Project structure

```
.
├── source/                 # Northwind source database (Docker Compose)
│   └── .env.example        # source DB credentials template
└── airflow-docker/
    ├── dags/
    │   ├── pipeline.py     # northwind_pipeline DAG
    │   └── tasks/          # extraction scripts and load script
    ├── meltano/            # Meltano project (taps and target)
    ├── data/               # JSONL staging files (generated)
    ├── output/             # final CSV evidence
    ├── .env.example        # Airflow, destination DB and Meltano credentials template
    ├── Dockerfile
    └── docker-compose.yaml
```

## Configuration

Credentials are never stored in the code. Create your own `.env` files from the templates and choose your own passwords:

```bash
cp source/.env.example source/.env
cp airflow-docker/.env.example airflow-docker/.env
```

Then edit both files and replace every `change_me`. Notes:

- `TAP_POSTGRES_USER` / `TAP_POSTGRES_PASSWORD` (Airflow `.env`) must match `NORTHWIND_USER` / `NORTHWIND_PASSWORD` (source `.env`).
- Passwords that go inside connection URLs should use only letters, digits, `-` and `_`.
- Generate the Airflow API secret with `openssl rand -hex 32`.
- **Linux and Codespaces only:** also set `AIRFLOW_UID` in `airflow-docker/.env` (uncomment the line and use the output of `id -u`).

## How to run

**Requirements:** Docker and Docker Compose, with ports 5432, 5433 and 8080 free.
Tested on macOS and Linux (GitHub Codespaces). Configured for Windows with Docker Desktop, but not tested.

Complete the [Configuration](#configuration) step first.

**1. Start the source database**

```bash
cd source
docker compose up -d
docker compose ps        # wait until the source database is Up
```

**2. Build and initialize Airflow**

```bash
cd ../airflow-docker
docker compose build
docker compose up airflow-init     # wait for: exited 0
```

**3. Start all services**

```bash
docker compose up -d
docker compose ps        # wait until all services are (healthy)
```

**4. Run the pipeline**

Open http://localhost:8080 and sign in with the `_AIRFLOW_WWW_USER_*` credentials from your `.env`. Find the `northwind_pipeline` DAG, enable it and trigger a run. Wait until all tasks show `success`.

Or from the command line:

```bash
docker compose exec airflow-scheduler airflow dags unpause northwind_pipeline
docker compose exec airflow-scheduler airflow dags list-runs northwind_pipeline
```

**5. Check the result**

Export the joined dataset (the commands below use the default user and database names from `.env.example`; adjust them if you changed `FINAL_DB_USER` or `FINAL_DB_NAME`):

```bash
mkdir -p output && docker compose exec -T postgres-final \
  psql -U final_user -d northwind_final -c "COPY (
    SELECT o.order_id, o.customer_id, o.employee_id, o.order_date,
           od.product_id, od.unit_price, od.quantity, od.discount
    FROM orders o
    INNER JOIN order_details od ON o.order_id = od.order_id
    ORDER BY o.order_id, od.product_id
  ) TO STDOUT CSV HEADER" > output/orders_with_details.csv
```

Query a single order (replace the `order_id`):

```bash
docker compose exec postgres-final \
  psql -U final_user -d northwind_final -c "
    SELECT o.order_id, o.customer_id, o.employee_id, o.order_date,
           od.product_id, od.unit_price, od.quantity, od.discount
    FROM orders o
    INNER JOIN order_details od ON o.order_id = od.order_id
    WHERE o.order_id = 10248
    ORDER BY od.product_id;"
```

## Reprocessing a specific date

```bash
docker compose exec airflow-scheduler \
  airflow backfill create --dag-id northwind_pipeline \
  --from-date 2024-01-15 \
  --to-date 2024-01-15
```

## Troubleshooting

Check the logs of the failing service:

```bash
docker compose logs <service-name> --tail 50
```

Examples: `airflow-scheduler`, `airflow-apiserver`, `airflow-dag-processor`, `postgres-final`.

If Docker Compose reports `Set ... in .env`, a required variable is missing from your `.env` file.
