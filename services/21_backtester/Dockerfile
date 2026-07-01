FROM python:3.11

WORKDIR /app
ENV PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir \
  nats-py==2.7.2 \
  psycopg2-binary==2.9.9 \
  python-dateutil==2.9.0.post0 \
  pandas==2.2.1 \
  numpy==1.26.4 \
  scipy==1.12.0 \
  matplotlib==3.8.3 \
  seaborn==0.13.2 \
  tabulate==0.9.0 \
  ipython==8.22.2 \
  quantstats==0.0.62 \
  yfinance==0.2.37 \
  statsmodels==0.14.4 \
  scikit-learn==1.4.1.post1

COPY ./app /app/app

CMD ["python", "-m", "app.main"]