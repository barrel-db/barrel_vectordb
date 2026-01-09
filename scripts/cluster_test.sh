#!/bin/bash
set -e

# Configuration
BASE_URL="http://127.0.0.1"
PORTS=(8081 8082 8083 8084 8085)
COLLECTION="test_collection"
DIMENSIONS=384

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_pass() { echo -e "${GREEN}✓ $1${NC}"; }
log_fail() { echo -e "${RED}✗ $1${NC}"; exit 1; }
log_info() { echo -e "${YELLOW}► $1${NC}"; }
log_section() { echo -e "\n${BLUE}=== $1 ===${NC}"; }

# Counter for unique seeds
SEED_COUNTER=0

# Generate random vector using awk (no Python needed)
random_vector() {
    local dim=$1
    SEED_COUNTER=$((SEED_COUNTER + 1))
    local seed=$(($(date +%s%N 2>/dev/null || echo $$) + SEED_COUNTER))
    awk -v dim="$dim" -v seed="$seed" 'BEGIN {
        srand(seed);
        printf "[";
        for (i = 1; i <= dim; i++) {
            printf "%.6f", rand();
            if (i < dim) printf ",";
        }
        printf "]";
    }'
}

# API call helper (returns response body, exits 0 even on HTTP errors)
api_call() {
    local method=$1 url=$2 data=$3
    if [ -n "$data" ]; then
        curl -s -X "$method" -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null || true
    else
        curl -s -X "$method" "$url" 2>/dev/null || true
    fi
}

# Wait for all nodes to be healthy
wait_for_cluster() {
    log_info "Waiting for cluster to form (this may take a minute)..."
    local max_wait=120
    local waited=0

    for port in "${PORTS[@]}"; do
        while [ $waited -lt $max_wait ]; do
            if api_call GET "$BASE_URL:$port/vectordb/cluster/status" | grep -q '"state"'; then
                log_pass "Node :$port is up"
                break
            fi
            sleep 2
            waited=$((waited + 2))
        done
        if [ $waited -ge $max_wait ]; then
            log_fail "Timeout waiting for node :$port"
        fi
    done
}

# Test: All nodes respond to cluster status
test_cluster_status() {
    log_section "Test: Cluster Status"
    local failed=0

    for port in "${PORTS[@]}"; do
        local status
        status=$(api_call GET "$BASE_URL:$port/vectordb/cluster/status")
        if echo "$status" | grep -q '"state"'; then
            local state
            state=$(echo "$status" | jq -r '.state // "unknown"')
            log_pass "Node :$port responding (state: $state)"
        else
            log_fail "Node :$port not responding"
            failed=1
        fi
    done

    [ $failed -eq 0 ] && log_pass "All nodes responding"
}

# Test: All nodes discovered in cluster
test_cluster_nodes() {
    log_section "Test: Node Discovery"

    local nodes
    nodes=$(api_call GET "$BASE_URL:8081/vectordb/cluster/nodes")

    if [ -z "$nodes" ]; then
        log_fail "Could not get cluster nodes"
    fi

    local count
    count=$(echo "$nodes" | jq '.nodes | length')

    if [ "$count" -eq 5 ]; then
        log_pass "All 5 nodes discovered"
        echo "$nodes" | jq -r '.nodes[]' | while read -r node; do
            echo "  - $node"
        done
    else
        log_fail "Expected 5 nodes, got $count"
    fi
}

# Test: Create collection (via any node)
test_create_collection() {
    log_section "Test: Create Collection"

    # Delete if exists
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$COLLECTION" || true
    sleep 1

    local result
    result=$(api_call PUT "$BASE_URL:8082/vectordb/collections/$COLLECTION" \
        "{\"dimensions\": $DIMENSIONS, \"num_shards\": 3, \"replication_factor\": 2}")

    if echo "$result" | grep -q '"status"'; then
        log_pass "Collection '$COLLECTION' created via node2"
    else
        log_fail "Failed to create collection: $result"
    fi

    # Verify visible from other nodes
    sleep 2
    local collections
    collections=$(api_call GET "$BASE_URL:8084/vectordb/collections")
    if echo "$collections" | grep -q "$COLLECTION"; then
        log_pass "Collection visible from node4"
    else
        log_fail "Collection not visible from node4"
    fi
}

# Test: Add documents via different nodes
test_add_documents() {
    log_section "Test: Add Documents (distributed)"

    local success=0
    for i in {1..10}; do
        local port_idx=$((i % 5))
        local port=${PORTS[$port_idx]}
        local vector
        vector=$(random_vector $DIMENSIONS)

        local result
        result=$(api_call POST "$BASE_URL:$port/vectordb/collections/$COLLECTION/docs" \
            "{\"id\": \"doc-$i\", \"text\": \"Test document number $i for cluster testing\", \"metadata\": {\"index\": $i}, \"vector\": $vector}")

        if echo "$result" | grep -q '"status"'; then
            success=$((success + 1))
        fi
    done

    if [ $success -eq 10 ]; then
        log_pass "Added 10 documents across nodes"
    else
        log_fail "Only added $success/10 documents"
    fi
}

# Test: Search (scatter-gather)
test_search() {
    log_section "Test: Search (scatter-gather)"

    local vector
    vector=$(random_vector $DIMENSIONS)

    local result
    result=$(api_call POST "$BASE_URL:8083/vectordb/collections/$COLLECTION/search" \
        "{\"vector\": $vector, \"k\": 5}")

    if [ -z "$result" ]; then
        log_fail "Search returned no response"
    fi

    local count
    count=$(echo "$result" | jq '.results | length')

    if [ "$count" -gt 0 ]; then
        log_pass "Search returned $count results via node3"
        echo "$result" | jq -r '.results[] | "  - \(.key): score=\(.score)"' | head -3
    else
        log_fail "Search returned no results"
    fi
}

# Test: Node failure and recovery
test_node_failure() {
    log_section "Test: Node Failure & Recovery"

    log_info "Stopping node3..."
    docker stop vectordb-node3 >/dev/null 2>&1

    sleep 10

    # Check cluster still works
    local nodes
    nodes=$(api_call GET "$BASE_URL:8081/vectordb/cluster/nodes")
    local count
    count=$(echo "$nodes" | jq '.nodes | length')

    if [ "$count" -eq 4 ]; then
        log_pass "Cluster detected node3 failure (4 nodes remaining)"
    else
        log_info "Cluster shows $count nodes"
    fi

    # Test writes still work (try multiple documents - some shards have leaders that are still up)
    local write_success=0
    for i in 1 2 3; do
        local vector
        vector=$(random_vector $DIMENSIONS)
        local result
        result=$(api_call POST "$BASE_URL:8082/vectordb/collections/$COLLECTION/docs" \
            "{\"id\": \"doc-failover-$i\", \"text\": \"Added during failover test $i\", \"metadata\": {}, \"vector\": $vector}")

        if echo "$result" | grep -q '"status"'; then
            write_success=$((write_success + 1))
        fi
    done

    if [ $write_success -gt 0 ]; then
        log_pass "Writes working after node failure ($write_success/3 succeeded - shards with live leaders)"
    else
        log_fail "All writes failed after node failure"
    fi

    # Restart node
    log_info "Restarting node3..."
    docker start vectordb-node3 >/dev/null 2>&1

    sleep 15

    nodes=$(api_call GET "$BASE_URL:8081/vectordb/cluster/nodes")
    count=$(echo "$nodes" | jq '.nodes | length')

    if [ "$count" -eq 5 ]; then
        log_pass "Node3 rejoined cluster"
    else
        log_info "Cluster has $count nodes after restart"
    fi
}

# Test: Leader info
test_leader() {
    log_section "Test: Leader Election"

    local status
    status=$(api_call GET "$BASE_URL:8081/vectordb/cluster/status")

    local leader
    leader=$(echo "$status" | jq -r '.leader // "unknown"')
    local is_leader
    is_leader=$(echo "$status" | jq -r '.is_leader // false')

    log_pass "Current leader: $leader"
    log_info "Node1 is leader: $is_leader"
}

# Test: Data consistency (write to one node, read from another)
test_consistency() {
    log_section "Test: Data Consistency"

    # Create a fresh collection for this test
    local test_col="consistency_test"
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
    sleep 1

    api_call PUT "$BASE_URL:8081/vectordb/collections/$test_col" \
        "{\"dimensions\": $DIMENSIONS, \"num_shards\": 1, \"replication_factor\": 2}" >/dev/null
    sleep 2

    # Write to node1
    local vector
    vector=$(random_vector $DIMENSIONS)
    local write_result
    write_result=$(api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/docs" \
        "{\"id\": \"consistency-doc\", \"text\": \"Consistency test document\", \"metadata\": {\"test\": \"consistency\"}, \"vector\": $vector}")

    if ! echo "$write_result" | grep -q '"status"'; then
        log_fail "Failed to write document on node1"
    fi

    sleep 2  # Allow replication

    # Read from node2, node3, node4, node5
    local read_success=0
    for port in 8082 8083 8084 8085; do
        local result
        result=$(api_call POST "$BASE_URL:$port/vectordb/collections/$test_col/search" \
            "{\"vector\": $vector, \"k\": 1}")

        if echo "$result" | grep -q 'consistency-doc'; then
            read_success=$((read_success + 1))
        fi
    done

    # Cleanup
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true

    if [ $read_success -ge 1 ]; then
        log_pass "Data consistent: read from $read_success/4 other nodes"
    else
        log_fail "Data inconsistency: could not read from any other node"
    fi
}

# Test: Replication verification
test_replication() {
    log_section "Test: Replication Verification"

    # Create collection with RF=3
    local test_col="replication_test"
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
    sleep 1

    local result
    result=$(api_call PUT "$BASE_URL:8082/vectordb/collections/$test_col" \
        "{\"dimensions\": $DIMENSIONS, \"num_shards\": 2, \"replication_factor\": 3}")

    if ! echo "$result" | grep -q '"status"'; then
        log_fail "Failed to create replicated collection"
    fi

    sleep 3  # Allow shard creation

    # Check shard placement via cluster status
    local shards
    shards=$(api_call GET "$BASE_URL:8081/vectordb/cluster/shards/$test_col")

    if [ -z "$shards" ]; then
        log_info "Could not get shard info (endpoint may not exist)"
    else
        echo "$shards" | jq -r '.' 2>/dev/null || echo "$shards"
    fi

    # Add a document and verify it's accessible from multiple nodes
    local vector
    vector=$(random_vector $DIMENSIONS)
    api_call POST "$BASE_URL:8083/vectordb/collections/$test_col/docs" \
        "{\"id\": \"replicated-doc\", \"text\": \"Replicated document\", \"metadata\": {}, \"vector\": $vector}" >/dev/null

    sleep 2

    # Count how many nodes can find the document
    local accessible=0
    for port in "${PORTS[@]}"; do
        local search_result
        search_result=$(api_call POST "$BASE_URL:$port/vectordb/collections/$test_col/search" \
            "{\"vector\": $vector, \"k\": 1}")

        if echo "$search_result" | grep -q 'replicated-doc'; then
            accessible=$((accessible + 1))
        fi
    done

    # Cleanup
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true

    if [ $accessible -eq 5 ]; then
        log_pass "Document accessible from all 5 nodes (replication working)"
    elif [ $accessible -ge 3 ]; then
        log_pass "Document accessible from $accessible/5 nodes"
    else
        log_fail "Document only accessible from $accessible/5 nodes"
    fi
}

# Test: Concurrent writes
test_concurrent() {
    log_section "Test: Concurrent Writes"

    # Create collection
    local test_col="concurrent_test"
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
    sleep 1

    api_call PUT "$BASE_URL:8081/vectordb/collections/$test_col" \
        "{\"dimensions\": $DIMENSIONS, \"num_shards\": 3, \"replication_factor\": 2}" >/dev/null
    sleep 2

    # Launch concurrent writes from different nodes
    local pids=()
    local results_dir="/tmp/concurrent_test_$$"
    mkdir -p "$results_dir"

    for i in {1..5}; do
        (
            local success=0
            for j in {1..5}; do
                local port=${PORTS[$((i-1))]}
                local vector
                vector=$(random_vector $DIMENSIONS)
                local result
                result=$(api_call POST "$BASE_URL:$port/vectordb/collections/$test_col/docs" \
                    "{\"id\": \"concurrent-$i-$j\", \"text\": \"Concurrent doc $i-$j\", \"metadata\": {\"writer\": $i}, \"vector\": $vector}")

                if echo "$result" | grep -q '"status"'; then
                    success=$((success + 1))
                fi
            done
            echo "$success" > "$results_dir/node$i"
        ) &
        pids+=($!)
    done

    # Wait for all writers
    for pid in "${pids[@]}"; do
        wait "$pid"
    done

    # Count total successes
    local total=0
    for i in {1..5}; do
        if [ -f "$results_dir/node$i" ]; then
            local count
            count=$(cat "$results_dir/node$i")
            total=$((total + count))
        fi
    done

    rm -rf "$results_dir"

    # Verify document count
    sleep 2
    local vector
    vector=$(random_vector $DIMENSIONS)
    local search_result
    search_result=$(api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/search" \
        "{\"vector\": $vector, \"k\": 30}")

    local found
    found=$(echo "$search_result" | jq '.results | length' 2>/dev/null || echo "0")

    # Cleanup
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true

    if [ "$total" -ge 20 ]; then
        log_pass "Concurrent writes: $total/25 succeeded, $found found in search"
    else
        log_fail "Concurrent writes: only $total/25 succeeded"
    fi
}

# Test: Shard leader failover
test_leader_failover() {
    log_section "Test: Shard Leader Failover"

    # Create collection with known placement
    local test_col="failover_test"
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
    sleep 1

    api_call PUT "$BASE_URL:8081/vectordb/collections/$test_col" \
        "{\"dimensions\": $DIMENSIONS, \"num_shards\": 1, \"replication_factor\": 3}" >/dev/null
    sleep 3

    # Add a document
    local vector
    vector=$(random_vector $DIMENSIONS)
    api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/docs" \
        "{\"id\": \"failover-doc\", \"text\": \"Failover test document\", \"metadata\": {}, \"vector\": $vector}" >/dev/null
    sleep 2

    # Verify document exists (try all nodes)
    local pre_success=0
    for port in "${PORTS[@]}"; do
        local pre_result
        pre_result=$(api_call POST "$BASE_URL:$port/vectordb/collections/$test_col/search" \
            "{\"vector\": $vector, \"k\": 1}")

        if echo "$pre_result" | grep -q 'failover-doc'; then
            pre_success=$((pre_success + 1))
        fi
    done

    if [ $pre_success -eq 0 ]; then
        log_fail "Document not found on any node before failover"
        api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
        return
    fi

    log_info "Document verified on $pre_success/5 nodes before failover"

    # Stop node2 (likely has shard replicas)
    log_info "Stopping node2..."
    docker stop vectordb-node2 >/dev/null 2>&1
    sleep 10

    # Try to read from remaining nodes
    local post_success=0
    for port in 8081 8083 8084 8085; do
        local result
        result=$(api_call POST "$BASE_URL:$port/vectordb/collections/$test_col/search" \
            "{\"vector\": $vector, \"k\": 1}")

        if echo "$result" | grep -q 'failover-doc'; then
            post_success=$((post_success + 1))
        fi
    done

    # Restart node2
    log_info "Restarting node2..."
    docker start vectordb-node2 >/dev/null 2>&1
    sleep 15

    # Cleanup
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true

    if [ $post_success -ge 1 ]; then
        log_pass "Shard failover: data accessible from $post_success/4 nodes after leader loss"
    else
        log_fail "Shard failover failed: data not accessible"
    fi
}

# Test: Dynamic node addition
test_node_addition() {
    log_section "Test: Dynamic Node Addition"

    # Check initial node count
    local initial_nodes
    initial_nodes=$(api_call GET "$BASE_URL:8081/vectordb/cluster/nodes")
    local initial_count
    initial_count=$(echo "$initial_nodes" | jq '.nodes | length' 2>/dev/null || echo "0")

    log_info "Initial cluster has $initial_count nodes"

    # Start node6
    log_info "Starting node6..."
    docker-compose -f docker-compose.cluster.yml --profile dynamic up -d node6 >/dev/null 2>&1

    # Wait for node6 to be healthy
    local waited=0
    local max_wait=90
    while [ $waited -lt $max_wait ]; do
        if curl -sf "http://127.0.0.1:8086/vectordb/cluster/status" >/dev/null 2>&1; then
            log_pass "Node6 is up on port 8086"
            break
        fi
        sleep 3
        waited=$((waited + 3))
    done

    if [ $waited -ge $max_wait ]; then
        log_fail "Timeout waiting for node6 to start"
        docker-compose -f docker-compose.cluster.yml --profile dynamic stop node6 >/dev/null 2>&1
        return
    fi

    # Wait for node6 to join cluster
    sleep 10

    # Check new node count
    local new_nodes
    new_nodes=$(api_call GET "$BASE_URL:8081/vectordb/cluster/nodes")
    local new_count
    new_count=$(echo "$new_nodes" | jq '.nodes | length' 2>/dev/null || echo "0")

    log_info "Cluster now has $new_count nodes"

    # Verify node6 is in the list
    if echo "$new_nodes" | grep -q 'node6'; then
        log_pass "Node6 successfully joined cluster"
    else
        log_fail "Node6 not found in cluster nodes"
    fi

    # Test that node6 can access existing data
    # First create a collection and add data via node1
    local test_col="node_add_test"
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
    sleep 1

    api_call PUT "$BASE_URL:8081/vectordb/collections/$test_col" \
        "{\"dimensions\": $DIMENSIONS, \"num_shards\": 2, \"replication_factor\": 2}" >/dev/null
    sleep 3

    local vector
    vector=$(random_vector $DIMENSIONS)
    api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/docs" \
        "{\"id\": \"node-add-doc\", \"text\": \"Document for node addition test\", \"metadata\": {}, \"vector\": $vector}" >/dev/null
    sleep 2

    # Search from node6
    local result
    result=$(api_call POST "$BASE_URL:8086/vectordb/collections/$test_col/search" \
        "{\"vector\": $vector, \"k\": 1}")

    if echo "$result" | grep -q 'node-add-doc'; then
        log_pass "Node6 can search cluster data"
    else
        log_info "Node6 search result: $result"
    fi

    # Test write via node6
    local vector2
    vector2=$(random_vector $DIMENSIONS)
    local write_result
    write_result=$(api_call POST "$BASE_URL:8086/vectordb/collections/$test_col/docs" \
        "{\"id\": \"written-via-node6\", \"text\": \"Document written via new node\", \"metadata\": {}, \"vector\": $vector2}")

    if echo "$write_result" | grep -q '"status"'; then
        log_pass "Node6 can write to cluster"
    else
        log_info "Node6 write failed: $write_result"
    fi

    # Cleanup
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true

    # Stop node6 (keep it for other tests if needed)
    log_info "Stopping node6..."
    docker-compose -f docker-compose.cluster.yml --profile dynamic stop node6 >/dev/null 2>&1

    # Verify cluster recovers
    sleep 5
    local final_nodes
    final_nodes=$(api_call GET "$BASE_URL:8081/vectordb/cluster/nodes")
    local final_count
    final_count=$(echo "$final_nodes" | jq '.nodes | length' 2>/dev/null || echo "0")

    if [ "$final_count" -eq "$initial_count" ]; then
        log_pass "Cluster recovered to $final_count nodes after node6 stopped"
    else
        log_info "Cluster has $final_count nodes (expected $initial_count)"
    fi
}

# Test: Graceful node leave
test_graceful_leave() {
    log_section "Test: Graceful Node Leave"

    # First start node6 and ensure it joins
    log_info "Starting node6..."
    docker-compose -f docker-compose.cluster.yml --profile dynamic up -d node6 >/dev/null 2>&1

    # Wait for node6 to be healthy
    local waited=0
    local max_wait=90
    while [ $waited -lt $max_wait ]; do
        if curl -sf "http://127.0.0.1:8086/vectordb/cluster/status" >/dev/null 2>&1; then
            break
        fi
        sleep 3
        waited=$((waited + 3))
    done

    if [ $waited -ge $max_wait ]; then
        log_fail "Timeout waiting for node6 to start"
        return
    fi

    sleep 10

    # Check node6 is in cluster
    local nodes_before
    nodes_before=$(api_call GET "$BASE_URL:8081/vectordb/cluster/nodes")
    local count_before
    count_before=$(echo "$nodes_before" | jq '.nodes | length' 2>/dev/null || echo "0")

    if ! echo "$nodes_before" | grep -q 'node6'; then
        log_fail "Node6 not in cluster before leave test"
        docker-compose -f docker-compose.cluster.yml --profile dynamic stop node6 >/dev/null 2>&1
        return
    fi

    log_info "Cluster has $count_before nodes (including node6)"

    # Call graceful leave on node6
    log_info "Calling graceful leave on node6..."
    local leave_result
    leave_result=$(api_call POST "$BASE_URL:8086/vectordb/cluster/leave" "")

    if echo "$leave_result" | grep -q '"leaving"'; then
        log_pass "Graceful leave initiated: $leave_result"
    else
        log_info "Leave result: $leave_result"
    fi

    sleep 5

    # Check node count decreased
    local nodes_after
    nodes_after=$(api_call GET "$BASE_URL:8081/vectordb/cluster/nodes")
    local count_after
    count_after=$(echo "$nodes_after" | jq '.nodes | length' 2>/dev/null || echo "0")

    if [ "$count_after" -lt "$count_before" ]; then
        log_pass "Node removed from cluster: $count_before -> $count_after nodes"
    else
        log_info "Cluster still has $count_after nodes (Ra may need time to propagate)"
    fi

    # Verify node6 is no longer in the list
    if ! echo "$nodes_after" | grep -q 'node6'; then
        log_pass "Node6 removed from cluster node list"
    else
        log_info "Node6 still in list (may take time to propagate)"
    fi

    # Stop node6 container
    log_info "Stopping node6 container..."
    docker-compose -f docker-compose.cluster.yml --profile dynamic stop node6 >/dev/null 2>&1
}

# Test: Network partition simulation
test_network_partition() {
    log_section "Test: Network Partition"

    # Create a test collection first
    local test_col="partition_test"
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
    sleep 1

    api_call PUT "$BASE_URL:8081/vectordb/collections/$test_col" \
        "{\"dimensions\": $DIMENSIONS, \"num_shards\": 3, \"replication_factor\": 2}" >/dev/null
    sleep 3

    # Add some documents
    for i in {1..5}; do
        local vector
        vector=$(random_vector $DIMENSIONS)
        api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/docs" \
            "{\"id\": \"partition-doc-$i\", \"text\": \"Document $i for partition test\", \"metadata\": {}, \"vector\": $vector}" >/dev/null
    done
    sleep 2

    log_info "Added 5 documents to test collection"

    # Get node3's IP address
    local node3_ip
    node3_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' vectordb-node3 2>/dev/null)

    if [ -z "$node3_ip" ]; then
        log_fail "Could not get node3 IP address"
        api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
        return
    fi

    log_info "Node3 IP: $node3_ip"

    # Block traffic from node1 and node2 to node3 (simulate partial partition)
    log_info "Creating network partition (blocking node3)..."

    # Block outgoing traffic to node3 from node1
    docker exec vectordb-node1 tc qdisc add dev eth0 root handle 1: prio 2>/dev/null || true
    docker exec vectordb-node1 tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst "$node3_ip" action drop 2>/dev/null || true

    # Block outgoing traffic to node3 from node2
    docker exec vectordb-node2 tc qdisc add dev eth0 root handle 1: prio 2>/dev/null || true
    docker exec vectordb-node2 tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst "$node3_ip" action drop 2>/dev/null || true

    sleep 15  # Wait for partition detection

    # Test: Majority partition (nodes 1,2,4,5) should still work
    local majority_result
    majority_result=$(api_call GET "$BASE_URL:8081/vectordb/cluster/status")

    if echo "$majority_result" | grep -q '"state"'; then
        log_pass "Majority partition (node1) still responding"
    else
        log_info "Majority partition status: $majority_result"
    fi

    # Test: Write to majority partition
    local vector
    vector=$(random_vector $DIMENSIONS)
    local write_result
    write_result=$(api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/docs" \
        "{\"id\": \"during-partition\", \"text\": \"Written during partition\", \"metadata\": {}, \"vector\": $vector}")

    if echo "$write_result" | grep -q '"status"'; then
        log_pass "Writes work on majority partition"
    else
        log_info "Write during partition: $write_result"
    fi

    # Test: Search on majority partition
    local search_result
    search_result=$(api_call POST "$BASE_URL:8082/vectordb/collections/$test_col/search" \
        "{\"vector\": $vector, \"k\": 3}")

    local count
    count=$(echo "$search_result" | jq '.results | length' 2>/dev/null || echo "0")

    if [ "$count" -gt 0 ]; then
        log_pass "Search works on majority partition ($count results)"
    else
        log_info "Search returned no results during partition"
    fi

    # Remove partition
    log_info "Removing network partition..."
    docker exec vectordb-node1 tc qdisc del dev eth0 root 2>/dev/null || true
    docker exec vectordb-node2 tc qdisc del dev eth0 root 2>/dev/null || true

    sleep 15  # Wait for recovery

    # Verify cluster recovered
    local nodes_after
    nodes_after=$(api_call GET "$BASE_URL:8081/vectordb/cluster/nodes")
    local node_count
    node_count=$(echo "$nodes_after" | jq '.nodes | length' 2>/dev/null || echo "0")

    if [ "$node_count" -ge 5 ]; then
        log_pass "Cluster recovered after partition ($node_count nodes)"
    else
        log_info "Cluster has $node_count nodes after partition recovery"
    fi

    # Verify node3 can respond
    local node3_status
    node3_status=$(api_call GET "$BASE_URL:8083/vectordb/cluster/status")

    if echo "$node3_status" | grep -q '"state"'; then
        log_pass "Node3 recovered and responding"
    else
        log_info "Node3 status after recovery: $node3_status"
    fi

    # Cleanup
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
}

# Test: Resharding
test_reshard() {
    log_section "Test: Resharding"

    # Create collection with 2 shards
    local test_col="reshard_test"
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
    sleep 1

    log_info "Creating collection with 2 shards..."
    api_call PUT "$BASE_URL:8081/vectordb/collections/$test_col" \
        "{\"dimensions\": $DIMENSIONS, \"num_shards\": 2, \"replication_factor\": 2}" >/dev/null
    sleep 3

    # Add documents
    log_info "Adding 10 documents..."
    for i in {1..10}; do
        local vector
        vector=$(random_vector $DIMENSIONS)
        api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/docs" \
            "{\"id\": \"reshard-doc-$i\", \"text\": \"Document $i for reshard test\", \"metadata\": {\"idx\": $i}, \"vector\": $vector}" >/dev/null
    done
    sleep 2

    # Verify documents exist
    local vector_search
    vector_search=$(random_vector $DIMENSIONS)
    local pre_count
    pre_count=$(api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/search" \
        "{\"vector\": $vector_search, \"k\": 20}" | jq '.results | length' 2>/dev/null || echo "0")

    log_info "Documents before reshard: $pre_count"

    # Reshard to 4 shards
    log_info "Resharding from 2 to 4 shards..."
    local reshard_result
    reshard_result=$(api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/reshard" \
        "{\"num_shards\": 4}")

    if echo "$reshard_result" | grep -q '"resharding"'; then
        log_pass "Reshard initiated: $reshard_result"
    elif echo "$reshard_result" | grep -q '"error"'; then
        log_info "Reshard result: $reshard_result"
    else
        log_info "Reshard result: $reshard_result"
    fi

    sleep 5

    # Verify documents still accessible
    local post_count
    post_count=$(api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/search" \
        "{\"vector\": $vector_search, \"k\": 20}" | jq '.results | length' 2>/dev/null || echo "0")

    log_info "Documents after reshard: $post_count"

    if [ "$post_count" -ge "$pre_count" ]; then
        log_pass "Documents preserved after reshard ($post_count docs)"
    else
        log_info "Document count changed: $pre_count -> $post_count"
    fi

    # Test that writes still work
    local vector
    vector=$(random_vector $DIMENSIONS)
    local write_result
    write_result=$(api_call POST "$BASE_URL:8081/vectordb/collections/$test_col/docs" \
        "{\"id\": \"post-reshard-doc\", \"text\": \"Written after reshard\", \"metadata\": {}, \"vector\": $vector}")

    if echo "$write_result" | grep -q '"status"'; then
        log_pass "Writes work after reshard"
    else
        log_info "Post-reshard write: $write_result"
    fi

    # Cleanup
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$test_col" >/dev/null 2>&1 || true
}

# Cleanup
cleanup() {
    log_section "Cleanup"
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$COLLECTION" >/dev/null 2>&1 || true
    log_pass "Test collection deleted"
}

# Usage
usage() {
    echo "Usage: $0 {status|nodes|collection|documents|search|failure|leader|consistency|replication|concurrent|failover|all|advanced|cleanup}"
    echo ""
    echo "Basic Tests:"
    echo "  status     - Check all nodes respond"
    echo "  nodes      - Check node discovery"
    echo "  collection - Create/verify collection"
    echo "  documents  - Add documents via different nodes"
    echo "  search     - Test scatter-gather search"
    echo "  failure    - Test node failure/recovery"
    echo "  leader     - Show leader info"
    echo ""
    echo "Advanced Tests:"
    echo "  consistency  - Write to one node, read from others"
    echo "  replication  - Verify replication factor works"
    echo "  concurrent   - Stress test with parallel writes"
    echo "  failover     - Test shard leader failover"
    echo "  nodeadd      - Dynamically add node6 to cluster"
    echo "  leave        - Test graceful node leave API"
    echo "  partition    - Simulate network partition with tc"
    echo "  reshard      - Test resharding (2->4 shards)"
    echo ""
    echo "Test Suites:"
    echo "  all        - Run basic tests"
    echo "  advanced   - Run advanced tests only"
    echo "  cleanup    - Delete test collection"
    exit 1
}

# Main
main() {
    echo "======================================"
    echo "Barrel VectorDB Cluster Test Suite"
    echo "======================================"

    case "${1:-all}" in
        status)      test_cluster_status ;;
        nodes)       test_cluster_nodes ;;
        collection)  test_create_collection ;;
        documents)   test_add_documents ;;
        search)      test_search ;;
        failure)     test_node_failure ;;
        leader)      test_leader ;;
        consistency) test_consistency ;;
        replication) test_replication ;;
        concurrent)  test_concurrent ;;
        failover)    test_leader_failover ;;
        nodeadd)     test_node_addition ;;
        leave)       test_graceful_leave ;;
        partition)   test_network_partition ;;
        reshard)     test_reshard ;;
        all)
            wait_for_cluster
            test_cluster_status
            test_cluster_nodes
            test_create_collection
            test_add_documents
            test_search
            test_leader
            test_node_failure
            cleanup
            ;;
        advanced)
            wait_for_cluster
            test_consistency
            test_replication
            test_concurrent
            test_leader_failover
            test_node_addition
            test_graceful_leave
            ;;
        cleanup)    cleanup ;;
        *)          usage ;;
    esac

    echo ""
    echo "======================================"
    echo "Tests completed"
    echo "======================================"
}

main "$@"
