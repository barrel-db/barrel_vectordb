#!/bin/bash
set -e

# Node name from environment or generate from hostname
NODE_NAME="${BARREL_NODE_NAME:-barrel_vectordb@$(hostname -f)}"

# Find the release version directory
RELEASE_DIR=$(ls -d /app/releases/*/ 2>/dev/null | head -1)
if [ -z "$RELEASE_DIR" ]; then
    echo "ERROR: No release directory found"
    exit 1
fi

echo "Configuring node: ${NODE_NAME}"
echo "Cluster enabled: ${BARREL_ENABLE_CLUSTER:-false}"
echo "Seed nodes: ${BARREL_SEED_NODES:-none}"

# Generate sys.config for cluster mode
cat > "${RELEASE_DIR}sys.config" << EOF
[
    {barrel_vectordb, [
        {path, "${BARREL_DATA_PATH:-/app/data}"},
        {enable_cluster, ${BARREL_ENABLE_CLUSTER:-false}},
        {cluster_options, #{
            cluster_name => barrel_vectors,
            seed_nodes => [${BARREL_SEED_NODES:-}],
            discovery_mode => seed,
            http => #{
                ip => {0, 0, 0, 0},
                port => ${BARREL_HTTP_PORT:-8080}
            }
        }}
    ]},
    {ra, [
        {data_dir, "${BARREL_DATA_PATH:-/app/data}/ra"}
    ]},
    {kernel, [
        {logger_level, info},
        {logger, [
            {handler, default, logger_std_h, #{
                level => info,
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
        {max_connections, 50}
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
cat > "${RELEASE_DIR}vm.args" << EOF
${NAME_FLAG} ${NODE_NAME}
-setcookie ${RELEASE_COOKIE:-barrel_vectordb_cluster}
+K true
+A 64
-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9200
EOF

echo "Configuration written to ${RELEASE_DIR}"
echo "Starting barrel_vectordb..."

exec /app/bin/barrel_vectordb "$@"
