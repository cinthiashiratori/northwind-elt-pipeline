import glob
import json
import os
import sys
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

DATA_DIR = "/data"


def build_connection_url() -> URL:
    """Build the destination database URL from environment variables.

    Required: FINAL_DB_USER, FINAL_DB_PASSWORD, FINAL_DB_NAME
    Optional: FINAL_DB_HOST (default: postgres-final), FINAL_DB_PORT (default: 5432)
    """
    required = ["FINAL_DB_USER", "FINAL_DB_PASSWORD", "FINAL_DB_NAME"]
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise RuntimeError(
            f"Missing environment variables: {', '.join(missing)}. "
            "Set them in airflow-docker/.env (see .env.example)."
        )
    return URL.create(
        drivername="postgresql+psycopg2",
        username=os.environ["FINAL_DB_USER"],
        password=os.environ["FINAL_DB_PASSWORD"],
        host=os.environ.get("FINAL_DB_HOST", "postgres-final"),
        port=int(os.environ.get("FINAL_DB_PORT", "5432")),
        database=os.environ["FINAL_DB_NAME"],
    )


def load(ds: str) -> None:
    engine = create_engine(build_connection_url())

    postgres_files = glob.glob(os.path.join(DATA_DIR, "postgres", "*", ds, "*.jsonl"))
    csv_files = glob.glob(os.path.join(DATA_DIR, "csv", ds, "*.jsonl"))
    all_files = postgres_files + csv_files

    if not all_files:
        raise FileNotFoundError(
            f"No .jsonl files found for date {ds}. "
            "Please check if extract_postgres and extract_csv completed successfully."
        )

    for file_path in all_files:
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        records = []
        with open(file_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        if records:
            df = pd.DataFrame(records)
            if table_name == "order_details":
                df["order_id"] = pd.to_numeric(df["order_id"], errors="coerce").astype("Int64")
            df.to_sql(
                table_name,
                engine,
                if_exists="replace",
                index=False,
                schema="public",
            )
            print(f"Table '{table_name}' loaded with {len(records)} records.")

    engine.dispose()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: execution date not provided.")
        print("Usage: python load_to_final_db.py <YYYY-MM-DD>")
        sys.exit(1)

    load(ds=sys.argv[1])
