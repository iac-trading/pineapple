FROM apache/airflow:2.10.2-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    iputils-ping \
    telnet \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-ssh \
    apache-airflow-providers-postgres \
    psycopg2-binary \
    nats-py \
    yfinance \
    pandas \
    pyarrow \
    ccxt \
    yahoo_fin