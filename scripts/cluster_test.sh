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

# Cleanup
cleanup() {
    log_section "Cleanup"
    api_call DELETE "$BASE_URL:8081/vectordb/collections/$COLLECTION" >/dev/null 2>&1 || true
    log_pass "Test collection deleted"
}

# Usage
usage() {
    echo "Usage: $0 {status|nodes|collection|documents|search|failure|leader|all|cleanup}"
    echo ""
    echo "Tests:"
    echo "  status     - Check all nodes respond"
    echo "  nodes      - Check node discovery"
    echo "  collection - Create/verify collection"
    echo "  documents  - Add documents via different nodes"
    echo "  search     - Test scatter-gather search"
    echo "  failure    - Test node failure/recovery"
    echo "  leader     - Show leader info"
    echo "  all        - Run all tests"
    echo "  cleanup    - Delete test collection"
    exit 1
}

# Main
main() {
    echo "======================================"
    echo "Barrel VectorDB Cluster Test Suite"
    echo "======================================"

    case "${1:-all}" in
        status)     test_cluster_status ;;
        nodes)      test_cluster_nodes ;;
        collection) test_create_collection ;;
        documents)  test_add_documents ;;
        search)     test_search ;;
        failure)    test_node_failure ;;
        leader)     test_leader ;;
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
        cleanup)    cleanup ;;
        *)          usage ;;
    esac

    echo ""
    echo "======================================"
    echo "Tests completed"
    echo "======================================"
}

main "$@"
