FROM python:3.12-slim as builder

WORKDIR /build

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libc6-dev \
    libffi-dev \
    libssl-dev \
    python3-dev \
    librdkafka-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY app/requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# Runtime
FROM python:3.12-slim

RUN useradd -m -u 1000 appuser

WORKDIR /app

RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    librdkafka1 \
    ca-certificates \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

ENV FLYWAY_VERSION=11.10.3
RUN curl -L "https://github.com/flyway/flyway/releases/download/flyway-${FLYWAY_VERSION}/flyway-commandline-${FLYWAY_VERSION}-linux-x64.tar.gz" \
    | tar xz -C /opt && \
    ln -s /opt/flyway-${FLYWAY_VERSION}/flyway /usr/local/bin/flyway

COPY --from=builder /install /usr/local

COPY app/ ./
COPY flyway/ ./flyway/
COPY k8s/scripts/istio.sh ./entrypoint.sh
COPY k8s/scripts/probe.sh ./probe.sh
RUN chmod +x /app/entrypoint.sh
RUN chmod +x /app/probe.sh
RUN chown -R appuser:appuser /app

USER appuser

CMD ["sh", "-c", "/app/entrypoint.sh"]
