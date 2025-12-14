FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# コンテナ起動時に実行するコマンド
# -u: Pythonの標準出力をバッファリングしない（ログがリアルタイムで表示される）
CMD ["python", "-u", "main.py"]
