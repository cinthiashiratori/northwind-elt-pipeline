FROM apache/airflow:3.1.8
USER root
# git: tap-csv só existe no GitHub (não está no PyPI)
# gcc + libc6-dev: compilar psutil do fonte (sem wheel para aarch64 + cpython-3.12)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git gcc libc6-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# Meltano em venv isolado — evita conflito de structlog com Airflow
RUN python -m venv /opt/meltano-venv \
    && /opt/meltano-venv/bin/pip install --no-cache-dir meltano==3.3.0
# printf (não echo) para criar wrapper com quebra de linha real
RUN printf '#!/bin/bash\nexec /opt/meltano-venv/bin/meltano "$@"\n' \
    > /usr/local/bin/meltano \
    && chmod +x /usr/local/bin/meltano
USER airflow
# Versões fixadas para garantir reprodutibilidade — testadas com Airflow 3.1.8 + Python 3.12
RUN pip install --no-cache-dir pandas==2.3.3 psycopg2-binary==2.9.11