FROM python:3.12-slim
WORKDIR /app
# MALLOC_ARENA_MAX：限制 glibc 為每個執行緒各開一塊記憶體區（多執行緒服務記憶體只增不減的常見原因）
ENV PYTHONUNBUFFERED=1 MALLOC_ARENA_MAX=2
RUN pip install --no-cache-dir flask gunicorn yfinance pandas
COPY proxy/main.py fetch_data.py ./
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 300 main:app
