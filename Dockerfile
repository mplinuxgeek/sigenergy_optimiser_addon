FROM python:3.12-alpine

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apk add --no-cache tzdata

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY optimizer ./optimizer
COPY config.example.yaml ./config.example.yaml

EXPOSE 8000

CMD ["uvicorn", "optimizer.web:app", "--host", "0.0.0.0", "--port", "8000"]
