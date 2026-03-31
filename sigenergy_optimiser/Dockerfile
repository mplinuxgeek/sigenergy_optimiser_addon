ARG BUILD_FROM=python:3.12-alpine
FROM ${BUILD_FROM}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apk add --no-cache tzdata

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY optimizer ./optimizer
COPY config.example.yaml ./config.example.yaml
COPY sigenergy_optimiser/run.sh /run.sh
RUN chmod +x /run.sh

EXPOSE 8000
CMD ["/run.sh"]
