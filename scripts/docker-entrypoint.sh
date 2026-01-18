#!/bin/bash
set -e

# ============================================
# Barrel VectorDB Docker Entrypoint
# ============================================
#
# Generates configuration from environment variables
# and starts the Erlang release.
#

# Colors for output (if terminal supports it)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    NC=''
fi

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Node name from environment or generate from hostname
NODE_NAME="${BARREL_NODE_NAME:-barrel_vectordb@$(hostname -f)}"

# Find the release version directory
RELEASE_DIR=$(ls -d /app/releases/*/ 2>/dev/null | head -1)
if [ -z "$RELEASE_DIR" ]; then
    log_error "No release directory found"
    exit 1
fi

log_info "Configuring Barrel VectorDB"
log_info "  Node name: ${NODE_NAME}"
log_info "  Data path: ${BARREL_DATA_PATH:-/app/data}"
log_info "  HTTP port: ${BARREL_HTTP_PORT:-8080}"
log_info "  Cluster enabled: ${BARREL_ENABLE_CLUSTER:-false}"

if [ "${BARREL_ENABLE_CLUSTER}" = "true" ]; then
    log_info "  Cluster name: ${BARREL_CLUSTER_NAME:-barrel_vectordb}"
    log_info "  Seed nodes: ${BARREL_SEED_NODES:-none}"
    log_info "  Default shards: ${BARREL_DEFAULT_SHARDS:-1}"
    log_info "  Replication factor: ${BARREL_REPLICATION_FACTOR:-1}"
fi

if [ "${BARREL_GATEWAY_ENABLED}" = "true" ]; then
    log_info "  Gateway enabled: true"
    log_info "  Gateway port: ${BARREL_GATEWAY_PORT:-8080}"
    log_info "  Gateway default RPM: ${BARREL_GATEWAY_DEFAULT_RPM:-100}"
fi

# Parse HTTP bind IP
HTTP_IP="${BARREL_HTTP_IP:-0.0.0.0}"
if [[ "$HTTP_IP" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    # Convert dotted notation to Erlang tuple
    HTTP_IP_TUPLE="{$(echo $HTTP_IP | sed 's/\./,/g')}"
else
    HTTP_IP_TUPLE="{0,0,0,0}"
fi

# Parse log level
case "${BARREL_LOG_LEVEL:-info}" in
    debug)   LOG_LEVEL="debug" ;;
    info)    LOG_LEVEL="info" ;;
    warning) LOG_LEVEL="warning" ;;
    warn)    LOG_LEVEL="warning" ;;
    error)   LOG_LEVEL="error" ;;
    *)       LOG_LEVEL="info" ;;
esac

# Build embedder configuration if provider is set
EMBEDDER_CONFIG=""
if [ -n "$BARREL_EMBEDDER_PROVIDER" ]; then
    log_info "  Embedder provider: ${BARREL_EMBEDDER_PROVIDER}"

    case "$BARREL_EMBEDDER_PROVIDER" in
        ollama)
            EMBEDDER_CONFIG="{embedder, #{
                provider => ollama,
                model => <<\"${BARREL_EMBEDDER_MODEL:-nomic-embed-text}\">>,
                url => <<\"${BARREL_EMBEDDER_URL:-http://localhost:11434}\">>
            }}"
            ;;
        openai)
            EMBEDDER_CONFIG="{embedder, #{
                provider => openai,
                model => <<\"${BARREL_EMBEDDER_MODEL:-text-embedding-3-small}\">>
            }}"
            ;;
        fastembed)
            EMBEDDER_CONFIG="{embedder, #{
                provider => fastembed,
                model => <<\"${BARREL_EMBEDDER_MODEL:-BAAI/bge-small-en-v1.5}\">>
            }}"
            ;;
        local)
            EMBEDDER_CONFIG="{embedder, #{
                provider => local,
                model => <<\"${BARREL_EMBEDDER_MODEL:-all-MiniLM-L6-v2}\">>
            }}"
            ;;
    esac

    if [ -n "$EMBEDDER_CONFIG" ]; then
        EMBEDDER_CONFIG="        $EMBEDDER_CONFIG,"
    fi
fi

# Build Gateway configuration if enabled
GATEWAY_CONFIG=""
if [ "${BARREL_GATEWAY_ENABLED:-false}" = "true" ]; then
    if [ -z "$BARREL_GATEWAY_MASTER_KEY" ]; then
        log_error "BARREL_GATEWAY_MASTER_KEY is required when gateway is enabled"
        exit 1
    fi
    GATEWAY_CONFIG="        {gateway, #{
            enabled => true,
            port => ${BARREL_GATEWAY_PORT:-8080},
            master_api_key => <<\"${BARREL_GATEWAY_MASTER_KEY}\">>,
            system_db_path => \"${BARREL_DATA_PATH:-/app/data}/system\",
            default_rate_limit => ${BARREL_GATEWAY_DEFAULT_RPM:-100},
            default_quotas => #{
                max_storage_mb => ${BARREL_GATEWAY_MAX_STORAGE_MB:-1024},
                max_vectors => ${BARREL_GATEWAY_MAX_VECTORS:-100000},
                max_collections => ${BARREL_GATEWAY_MAX_COLLECTIONS:-10}
            }
        }},"
fi

# Build Prometheus configuration if enabled
PROMETHEUS_CONFIG=""
if [ "${BARREL_PROMETHEUS_ENABLED:-false}" = "true" ]; then
    log_info "  Prometheus metrics: enabled on port ${BARREL_PROMETHEUS_PORT:-9090}"
    PROMETHEUS_CONFIG="    {prometheus, [
        {default_metrics, true}
    ]},"
fi

# Generate sys.config
cat > "${RELEASE_DIR}sys.config" << EOF
[
    {barrel_vectordb, [
        {path, "${BARREL_DATA_PATH:-/app/data}"},
        {enable_cluster, ${BARREL_ENABLE_CLUSTER:-false}},
$EMBEDDER_CONFIG
$GATEWAY_CONFIG
        {cluster_options, #{
            cluster_name => '${BARREL_CLUSTER_NAME:-barrel_vectordb}',
            seed_nodes => [${BARREL_SEED_NODES:-}],
            discovery_mode => seed,
            http => #{
                ip => ${HTTP_IP_TUPLE},
                port => ${BARREL_HTTP_PORT:-8080}
            },
            sharding => #{
                default_shards => ${BARREL_DEFAULT_SHARDS:-1},
                replication_factor => ${BARREL_REPLICATION_FACTOR:-1}
            }
        }}
    ]},
    {ra, [
        {data_dir, "${BARREL_DATA_PATH:-/app/data}/ra"}
    ]},
$PROMETHEUS_CONFIG
    {kernel, [
        {logger_level, ${LOG_LEVEL}},
        {logger, [
            {handler, default, logger_std_h, #{
                level => ${LOG_LEVEL},
                formatter => {logger_formatter, #{
                    single_line => true,
                    template => [time, " [", level, "] ", msg, "\n"]
                }}
            }}
        ]},
        {inet_dist_listen_min, 9100},
        {inet_dist_listen_max, 9200}
    ]},
    {hackney, [
        {max_connections, 100}
    ]}
].
EOF

# Generate vm.args
# Use -sname for short names (Docker hostnames) or -name for FQDNs
if [[ "$NODE_NAME" == *"."* ]]; then
    NAME_FLAG="-name"
else
    NAME_FLAG="-sname"
fi

# Scheduler count
SCHEDULER_OPTS=""
if [ -n "$BARREL_SCHEDULER_COUNT" ]; then
    SCHEDULER_OPTS="+S ${BARREL_SCHEDULER_COUNT}"
fi

cat > "${RELEASE_DIR}vm.args" << EOF
${NAME_FLAG} ${NODE_NAME}
-setcookie ${RELEASE_COOKIE:-barrel_vectordb_secret}
+K true
+A ${BARREL_ASYNC_THREADS:-64}
${SCHEDULER_OPTS}
-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9200
-heart
EOF

log_info "Configuration written to ${RELEASE_DIR}"
log_info "Starting Barrel VectorDB..."

# Execute the release
exec /app/bin/barrel_vectordb "$@"
