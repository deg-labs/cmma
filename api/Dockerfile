FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir pip --upgrade
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

# コンテナ起動時にUvicornサーバーを起動
# --host 0.0.0.0: コンテナ外部からアクセスできるようにする
# --port 8000: EXPOSEで公開したポートでリッスン
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
