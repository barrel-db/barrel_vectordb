# Build stage
FROM erlang:27-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    cmake \
    libsnappy-dev \
    liblz4-dev \
    libzstd-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Setup git to use CI_JOB_TOKEN for private GitLab repos
ARG CI_JOB_TOKEN=""
RUN if [ -n "$CI_JOB_TOKEN" ]; then \
    git config --global url."https://gitlab-ci-token:${CI_JOB_TOKEN}@gitlab.enki.io/".insteadOf "https://gitlab.enki.io/"; \
    fi

# RocksDB build settings
ENV ERLANG_ROCKSDB_BUILDOPTS="-j 2"
ENV ERLANG_ROCKSDB_OPTS="-DWITH_SNAPPY=ON -DWITH_LZ4=ON -DWITH_ZSTD=ON"
ENV CXXFLAGS="-std=c++20"

# Copy rebar config first for better caching
COPY rebar.config rebar.lock* ./

# Fetch dependencies
RUN rebar3 get-deps

# Copy source
COPY src/ src/
COPY include/ include/
COPY priv/ priv/
COPY config/ config/

# Initialize git repo (needed by rocksdb hooks)
RUN git init && git config user.email "build@local" && git config user.name "build"

# Build release
RUN rebar3 as prod compile && rebar3 as prod release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssl \
    libncurses6 \
    libstdc++6 \
    libsnappy1v5 \
    liblz4-1 \
    libzstd1 \
    curl \
    jq \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy release from builder
COPY --from=builder /app/_build/prod/rel/barrel_vectordb ./

# Copy entrypoint script
COPY scripts/docker-entrypoint.sh /app/

# Create directories
RUN mkdir -p /app/data /app/log && chmod +x /app/docker-entrypoint.sh

# Environment defaults
ENV RELEASE_COOKIE=barrel_vectordb_cluster
ENV BARREL_DATA_PATH=/app/data
ENV BARREL_HTTP_PORT=8080
ENV BARREL_ENABLE_CLUSTER=true

# HTTP API port
EXPOSE 8080

# EPMD port
EXPOSE 4369

# Erlang distribution ports
EXPOSE 9100-9200

# Volume for persistent data
VOLUME ["/app/data"]

# Health check using cluster status endpoint
HEALTHCHECK --interval=10s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -sf http://localhost:${BARREL_HTTP_PORT}/vectordb/cluster/status || exit 1

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["foreground"]
