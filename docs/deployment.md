# Docker Deployment

Barrel VectorDB provides official Docker images for easy deployment. Images are built for both `linux/amd64` and `linux/arm64` architectures.

## Quick Start

### Standalone Mode

```bash
docker run -d \
  --name barrel-vectordb \
  -p 8080:8080 \
  -v barrel-data:/app/data \
  registry.gitlab.enki.io/barrel-db/barrel_vectordb:latest
```

### Cluster Mode

```bash
docker run -d \
  --name barrel-node1 \
  -e BARREL_ENABLE_CLUSTER=true \
  -e BARREL_NODE_NAME=barrel@node1 \
  -e RELEASE_COOKIE=my_secret_cookie \
  -p 8080:8080 \
  -p 4369:4369 \
  -p 9100-9200:9100-9200 \
  -v barrel-data:/app/data \
  registry.gitlab.enki.io/barrel-db/barrel_vectordb:latest
```

## Image Tags

| Tag | Description |
|-----|-------------|
| `latest` | Latest stable release |
| `x.y.z` | Specific version (e.g., `1.0.0`) |
| `main` | Latest main branch build |
| `main-<sha>` | Specific main branch commit |

## Configuration

All configuration is done via environment variables.

### Node Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_NODE_NAME` | `barrel_vectordb@<hostname>` | Erlang node name |
| `RELEASE_COOKIE` | `barrel_vectordb_secret` | Erlang distribution cookie |

!!! warning "Security"
    Always set a strong `RELEASE_COOKIE` in production. All nodes in a cluster must use the same cookie.

### Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_DATA_PATH` | `/app/data` | Data directory path |

### HTTP API

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_HTTP_PORT` | `8080` | HTTP API port |
| `BARREL_HTTP_IP` | `0.0.0.0` | HTTP bind address |

### Clustering

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_ENABLE_CLUSTER` | `false` | Enable cluster mode |
| `BARREL_SEED_NODES` | - | Comma-separated seed nodes |
| `BARREL_CLUSTER_NAME` | `barrel_vectordb` | Cluster name |

**Seed nodes format:**

```bash
BARREL_SEED_NODES="'barrel@node1.example.com','barrel@node2.example.com'"
```

### Sharding

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_DEFAULT_SHARDS` | `1` | Default shard count for new collections |
| `BARREL_REPLICATION_FACTOR` | `1` | Default replication factor |

### Embedding

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_EMBEDDER_PROVIDER` | - | Provider: `ollama`, `openai`, `fastembed`, `local` |
| `BARREL_EMBEDDER_MODEL` | (provider default) | Model name |
| `BARREL_EMBEDDER_URL` | - | URL for ollama/openai |
| `OPENAI_API_KEY` | - | OpenAI API key |

### Performance

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_SCHEDULER_COUNT` | (auto) | Erlang scheduler count |
| `BARREL_ASYNC_THREADS` | `64` | Async thread pool size |

### Logging

| Variable | Default | Description |
|----------|---------|-------------|
| `BARREL_LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warning`, `error` |

## Docker Compose

### Single Node

```yaml
version: '3.8'

services:
  vectordb:
    image: registry.gitlab.enki.io/barrel-db/barrel_vectordb:latest
    ports:
      - "8080:8080"
    environment:
      BARREL_LOG_LEVEL: info
    volumes:
      - vectordb-data:/app/data
    restart: unless-stopped

volumes:
  vectordb-data:
```

### 3-Node Cluster

```yaml
version: '3.8'

x-vectordb-common: &vectordb-common
  image: registry.gitlab.enki.io/barrel-db/barrel_vectordb:latest
  restart: unless-stopped
  networks:
    - vectordb-net

services:
  node1:
    <<: *vectordb-common
    hostname: node1
    environment:
      RELEASE_COOKIE: my_production_cookie
      BARREL_NODE_NAME: barrel@node1
      BARREL_ENABLE_CLUSTER: "true"
      BARREL_SEED_NODES: ""  # First node, no seeds
      BARREL_REPLICATION_FACTOR: "2"
    ports:
      - "8081:8080"
    volumes:
      - node1-data:/app/data

  node2:
    <<: *vectordb-common
    hostname: node2
    environment:
      RELEASE_COOKIE: my_production_cookie
      BARREL_NODE_NAME: barrel@node2
      BARREL_ENABLE_CLUSTER: "true"
      BARREL_SEED_NODES: "'barrel@node1'"
      BARREL_REPLICATION_FACTOR: "2"
    ports:
      - "8082:8080"
    volumes:
      - node2-data:/app/data
    depends_on:
      - node1

  node3:
    <<: *vectordb-common
    hostname: node3
    environment:
      RELEASE_COOKIE: my_production_cookie
      BARREL_NODE_NAME: barrel@node3
      BARREL_ENABLE_CLUSTER: "true"
      BARREL_SEED_NODES: "'barrel@node1'"
      BARREL_REPLICATION_FACTOR: "2"
    ports:
      - "8083:8080"
    volumes:
      - node3-data:/app/data
    depends_on:
      - node1

networks:
  vectordb-net:
    driver: bridge

volumes:
  node1-data:
  node2-data:
  node3-data:
```

### With Ollama Embeddings

```yaml
version: '3.8'

services:
  ollama:
    image: ollama/ollama:latest
    volumes:
      - ollama-data:/root/.ollama
    ports:
      - "11434:11434"

  vectordb:
    image: registry.gitlab.enki.io/barrel-db/barrel_vectordb:latest
    depends_on:
      - ollama
    environment:
      BARREL_EMBEDDER_PROVIDER: ollama
      BARREL_EMBEDDER_MODEL: nomic-embed-text
      BARREL_EMBEDDER_URL: http://ollama:11434
    ports:
      - "8080:8080"
    volumes:
      - vectordb-data:/app/data

volumes:
  ollama-data:
  vectordb-data:
```

## Kubernetes

### Basic Deployment

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: barrel-vectordb
spec:
  serviceName: barrel-vectordb
  replicas: 3
  selector:
    matchLabels:
      app: barrel-vectordb
  template:
    metadata:
      labels:
        app: barrel-vectordb
    spec:
      containers:
        - name: vectordb
          image: registry.gitlab.enki.io/barrel-db/barrel_vectordb:latest
          ports:
            - containerPort: 8080
              name: http
            - containerPort: 4369
              name: epmd
            - containerPort: 9100
              name: dist
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: BARREL_NODE_NAME
              value: "barrel@$(POD_NAME).barrel-vectordb"
            - name: BARREL_ENABLE_CLUSTER
              value: "true"
            - name: BARREL_SEED_NODES
              value: "'barrel@barrel-vectordb-0.barrel-vectordb'"
            - name: RELEASE_COOKIE
              valueFrom:
                secretKeyRef:
                  name: barrel-secrets
                  key: erlang-cookie
          volumeMounts:
            - name: data
              mountPath: /app/data
          livenessProbe:
            httpGet:
              path: /vectordb/cluster/status
              port: 8080
            initialDelaySeconds: 60
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /vectordb/cluster/status
              port: 8080
            initialDelaySeconds: 30
            periodSeconds: 10
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
---
apiVersion: v1
kind: Service
metadata:
  name: barrel-vectordb
spec:
  clusterIP: None  # Headless service for StatefulSet
  selector:
    app: barrel-vectordb
  ports:
    - port: 8080
      name: http
    - port: 4369
      name: epmd
    - port: 9100
      name: dist
---
apiVersion: v1
kind: Service
metadata:
  name: barrel-vectordb-lb
spec:
  type: LoadBalancer
  selector:
    app: barrel-vectordb
  ports:
    - port: 8080
      targetPort: 8080
```

## Health Checks

The container includes a built-in health check:

```bash
curl http://localhost:8080/vectordb/cluster/status
```

Response when healthy:

```json
{
  "state": "member",
  "is_leader": true,
  "nodes": ["barrel@node1", "barrel@node2", "barrel@node3"]
}
```

## Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 8080 | TCP | HTTP API |
| 4369 | TCP | EPMD (Erlang Port Mapper Daemon) |
| 9100-9200 | TCP | Erlang distribution |
| 9090 | TCP | Prometheus metrics (optional) |

## Volumes

| Path | Description |
|------|-------------|
| `/app/data` | Persistent data (RocksDB, Ra logs) |

!!! warning "Data Persistence"
    Always mount `/app/data` to a persistent volume in production to avoid data loss.

## Building Custom Images

```bash
# Clone repository
git clone https://gitlab.enki.io/barrel-db/barrel_vectordb.git
cd barrel_vectordb

# Build for current architecture
docker build -t my-barrel-vectordb .

# Build for multiple architectures
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t my-registry/barrel-vectordb:latest \
  --push \
  .
```
