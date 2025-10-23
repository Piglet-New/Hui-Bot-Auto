FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY hui_bot_fresh.py .
ENV PORT=8080
CMD ["python", "hui_bot_fresh.py"]
