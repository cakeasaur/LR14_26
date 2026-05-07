# ─── Stage 1: build ─────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

# git нужен Go-модулям для приватных зависимостей и vcs-stamp; ca-certificates
# понадобятся в runtime, копируем их в финальный образ.
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Кэшируем слои модулей: сначала только манифесты, потом исходники.
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
COPY regions.json ./

# CGO отключён — получаем полностью статический бинарь, который
# запустится на голом alpine без glibc. GOARCH берём от --platform
# (мульти-арх через TARGETARCH), что позволяет собирать и под ARM.
ARG TARGETARCH=amd64
ENV CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH
RUN go build -trimpath -ldflags="-s -w" -o /out/collector .

# ─── Stage 2: runtime ───────────────────────────────────────────────────────
FROM alpine:3.19

# Только минимум: ca-certs для grpc/http к etcd. Probe выполняет kubelet
# снаружи через httpGet, ничего внутри контейнера не требуется.
# Фиксируем UID/GID = 10001, чтобы k8s securityContext.fsGroup мог
# совпасть и emptyDir/PVC мог быть group-writable.
RUN apk add --no-cache ca-certificates && \
    addgroup -g 10001 app && \
    adduser -D -u 10001 -G app app && \
    mkdir -p /app /data && chown -R app:app /app /data

WORKDIR /app
COPY --from=builder /out/collector /app/collector
COPY --from=builder /src/regions.json /app/regions.json

USER app

# 8080 — /health, 9090 — /metrics (Prometheus), 8815 — Arrow Flight gRPC
EXPOSE 8080 9090 8815

ENV ETCD_ENDPOINTS="etcd:2379" \
    OUTPUT_PATH="/data/raw.ndjson" \
    WINDOW_SECONDS="10s" \
    BATCH_SIZE="50"

# Передаём env как аргументы CLI через shell-форму ENTRYPOINT.
ENTRYPOINT ["/bin/sh", "-c", "exec /app/collector \
  --etcd=$ETCD_ENDPOINTS \
  --output=$OUTPUT_PATH \
  --regions-file=/app/regions.json \
  --window=$WINDOW_SECONDS \
  --batch=$BATCH_SIZE"]
