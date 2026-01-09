# Cluster Testing

Barrel VectorDB includes a comprehensive Docker-based testing infrastructure for validating cluster behavior.

## Overview

The testing infrastructure provides:

- **5-node Docker cluster** with configurable seed nodes
- **Dynamic node addition** via Docker Compose profiles
- **Network partition simulation** using Linux traffic control (tc)
- **Comprehensive test scenarios** for cluster operations

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Docker Network: vectordb-net                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                      │
│  │  node1   │  │  node2   │  │  node3   │   Initial cluster    │
│  │ (seed)   │◄─►│          │◄─►│          │   (Ra quorum)       │
│  │ :8081    │  │ :8082    │  │ :8083    │                      │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                      │
│       │             │             │                             │
│       └─────────────┼─────────────┘                             │
│                     │                                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐                      │
│  │  node4   │  │  node5   │  │  node6   │   Additional nodes   │
│  │ :8084    │  │ :8085    │  │ :8086    │   (node6 = dynamic)  │
│  └──────────┘  └──────────┘  └──────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Build and Start Cluster

```bash
# Build the Docker image
docker-compose -f docker-compose.cluster.yml build

# Start 5-node cluster
docker-compose -f docker-compose.cluster.yml up -d

# Wait for cluster formation
sleep 30

# Verify cluster health
curl http://localhost:8081/vectordb/cluster/nodes
```

### Run Tests

```bash
# Run all basic tests
./scripts/cluster_test.sh all

# Run advanced tests (includes failover, node addition, etc.)
./scripts/cluster_test.sh advanced

# Run specific test
./scripts/cluster_test.sh search
./scripts/cluster_test.sh reshard
./scripts/cluster_test.sh partition
```

### Cleanup

```bash
# Stop cluster and remove volumes
docker-compose -f docker-compose.cluster.yml down -v
```

## Test Scenarios

### Basic Tests (`all`)

| Test | Description |
|------|-------------|
| `status` | All 5 nodes responding to HTTP requests |
| `nodes` | Cluster discovers all 5 nodes |
| `collection` | Create and delete collections |
| `documents` | Add documents via different nodes |
| `search` | Scatter-gather search across shards |
| `leader` | Leader election and failover |
| `failure` | Node failure and recovery |

### Advanced Tests (`advanced`)

| Test | Description |
|------|-------------|
| `consistency` | Data visible across all nodes |
| `replication` | Documents accessible from replicas |
| `concurrent` | Parallel writes from multiple nodes |
| `failover` | Shard leader failover on node loss |
| `nodeadd` | Dynamic node addition |
| `leave` | Graceful node leave |

### Specialized Tests

| Test | Description |
|------|-------------|
| `partition` | Network partition simulation |
| `reshard` | Collection resharding |

## Running Individual Tests

```bash
# Cluster status
./scripts/cluster_test.sh status

# Node discovery
./scripts/cluster_test.sh nodes

# Document operations
./scripts/cluster_test.sh documents

# Search functionality
./scripts/cluster_test.sh search

# Node addition
./scripts/cluster_test.sh nodeadd

# Graceful leave
./scripts/cluster_test.sh leave

# Network partition
./scripts/cluster_test.sh partition

# Resharding
./scripts/cluster_test.sh reshard
```

## Test Details

### Network Partition Test

Simulates a network partition by blocking traffic to a specific node using Linux traffic control (`tc`):

```bash
# Inside container, block all traffic to node3
tc qdisc add dev eth0 root handle 1: prio
tc filter add dev eth0 parent 1: protocol ip prio 1 u32 \
    match ip dst 192.168.x.x/32 flowid 1:1
tc qdisc add dev eth0 parent 1:1 handle 10: netem loss 100%
```

The test verifies:

1. Majority partition continues operating
2. Writes succeed on majority partition
3. Search works on majority partition
4. Cluster recovers when partition heals

!!! note "Requires NET_ADMIN"
    The Docker containers run with `NET_ADMIN` capability to allow traffic control commands.

### Dynamic Node Addition Test

Tests adding a 6th node to the running cluster:

1. Start node6 with Docker Compose profile
2. Wait for node to join cluster (state: member)
3. Verify node appears in cluster membership
4. Test read/write operations via new node
5. Stop node6 and verify cluster continues

```bash
# Start node6
docker-compose -f docker-compose.cluster.yml --profile dynamic up -d node6

# Verify join
curl http://localhost:8086/vectordb/cluster/status | jq '.state'
# => "member"
```

### Graceful Leave Test

Tests the graceful node removal process:

1. Start node6 and wait for cluster membership
2. Call graceful leave API on node6
3. Verify node is removed from cluster
4. Verify remaining nodes continue operating

```bash
# Leave cluster
curl -X POST http://localhost:8086/vectordb/cluster/leave

# Verify removal
curl http://localhost:8081/vectordb/cluster/nodes
```

### Resharding Test

Tests changing the shard count for a collection:

1. Create collection with 2 shards
2. Add test documents
3. Reshard to 4 shards
4. Verify all documents preserved
5. Verify writes work after reshard

```bash
# Create collection
curl -X PUT http://localhost:8081/vectordb/collections/test \
  -H "Content-Type: application/json" \
  -d '{"dimensions": 128, "num_shards": 2}'

# Add documents...

# Reshard
curl -X POST http://localhost:8081/vectordb/collections/test/reshard \
  -H "Content-Type: application/json" \
  -d '{"num_shards": 4}'
```

## Port Mapping

| Node | HTTP Port | Erlang Node Name |
|------|-----------|------------------|
| node1 | 8081 | barrel_vectordb@node1 |
| node2 | 8082 | barrel_vectordb@node2 |
| node3 | 8083 | barrel_vectordb@node3 |
| node4 | 8084 | barrel_vectordb@node4 |
| node5 | 8085 | barrel_vectordb@node5 |
| node6 | 8086 | barrel_vectordb@node6 |

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BARREL_NODE_NAME` | Erlang node name | Required |
| `BARREL_SEED_NODES` | Comma-separated seed nodes | Required |
| `BARREL_ENABLE_CLUSTER` | Enable clustering | `true` |
| `BARREL_HTTP_PORT` | HTTP API port | `8080` |
| `RELEASE_COOKIE` | Erlang distribution cookie | Required |

### Docker Compose Profiles

| Profile | Nodes | Usage |
|---------|-------|-------|
| (default) | node1-5 | Standard 5-node cluster |
| `dynamic` | node6 | Additional node for testing |

```bash
# Start with dynamic node
docker-compose -f docker-compose.cluster.yml --profile dynamic up -d
```

## Troubleshooting

### Node Won't Join Cluster

Check node logs:

```bash
docker logs vectordb-node6 --tail 50
```

Common issues:

- **Cookie mismatch**: Ensure `RELEASE_COOKIE` matches across all nodes
- **Network isolation**: Verify nodes are on same Docker network
- **Seed node down**: Ensure at least one seed node is healthy

### Test Timeouts

Increase wait times in `cluster_test.sh`:

```bash
# Default is 120s for node join
local max_wait=180
```

### Stale Node Data

Clean up node data before restarting:

```bash
docker-compose -f docker-compose.cluster.yml --profile dynamic stop node6
docker-compose -f docker-compose.cluster.yml --profile dynamic rm -f node6
docker volume rm barrel_vectordb_node6-data
```

## Writing Custom Tests

The test script provides helper functions:

```bash
# Source the helpers
source scripts/cluster_test.sh

# API call helper
api_call GET "http://localhost:8081/vectordb/cluster/status"
api_call POST "http://localhost:8081/vectordb/collections/test" '{"dimensions": 128}'

# Generate random vector
vector=$(random_vector 128)

# Logging helpers
log_info "Information message"
log_pass "Test passed"
log_fail "Test failed"
log_section "Test: My Custom Test"
```
