FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir flask gunicorn yfinance pandas
COPY proxy/main.py fetch_data.py ./
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 300 main:app
