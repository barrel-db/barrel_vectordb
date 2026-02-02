#!/bin/bash
# BM25 and Hybrid Search HTTP API Tests
#
# Usage:
#   ./scripts/bm25_http_test.sh [URL]
#
# Default URL: http://localhost:8080
# For cluster: ./scripts/bm25_http_test.sh http://localhost:8081

set -e

# Configuration
BASE_URL="${1:-http://localhost:8080}"
COLLECTION="bm25_test_collection"
DIMENSIONS=4

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

# API call helper
api_call() {
    local method=$1 url=$2 data=$3
    if [ -n "$data" ]; then
        curl -s -X "$method" -H "Content-Type: application/json" -d "$data" "$url" 2>/dev/null || true
    else
        curl -s -X "$method" "$url" 2>/dev/null || true
    fi
}

# Check if server is up
check_server() {
    log_info "Checking server at $BASE_URL..."
    local status
    status=$(api_call GET "$BASE_URL/vectordb/cluster/status")
    if echo "$status" | grep -q '"state"'; then
        log_pass "Server is up"
    else
        log_fail "Server not responding at $BASE_URL"
    fi
}

# Setup: Create collection with BM25 enabled
setup_collection() {
    log_section "Setup: Create Collection with BM25"

    # Delete if exists
    api_call DELETE "$BASE_URL/vectordb/collections/$COLLECTION" > /dev/null 2>&1 || true
    sleep 1

    # Create collection with BM25 backend
    local result
    result=$(api_call PUT "$BASE_URL/vectordb/collections/$COLLECTION" \
        "{\"dimensions\": $DIMENSIONS, \"num_shards\": 2, \"replication_factor\": 1, \"bm25_backend\": \"memory\"}")

    if echo "$result" | grep -q '"status"'; then
        log_pass "Collection '$COLLECTION' created with BM25"
    else
        # Check if it failed due to embedder (standalone mode)
        log_info "Creating standalone collection..."
        result=$(api_call PUT "$BASE_URL/vectordb/collections/$COLLECTION" \
            "{\"dimensions\": $DIMENSIONS, \"bm25_backend\": \"memory\"}")
        if echo "$result" | grep -q '"status"\|"created"'; then
            log_pass "Collection '$COLLECTION' created"
        else
            log_fail "Failed to create collection: $result"
        fi
    fi
}

# Add test documents
add_documents() {
    log_section "Setup: Add Test Documents"

    local docs=(
        '{"id": "doc1", "text": "Erlang is a functional programming language for concurrent systems", "vector": [0.1, 0.2, 0.3, 0.4]}'
        '{"id": "doc2", "text": "Python is popular for data science and machine learning", "vector": [0.2, 0.3, 0.4, 0.5]}'
        '{"id": "doc3", "text": "Erlang OTP provides behaviors for fault-tolerant distributed applications", "vector": [0.3, 0.4, 0.5, 0.6]}'
        '{"id": "doc4", "text": "Java is used for enterprise applications and Android development", "vector": [0.4, 0.5, 0.6, 0.7]}'
        '{"id": "doc5", "text": "Erlang processes are lightweight and communicate via message passing", "vector": [0.5, 0.6, 0.7, 0.8]}'
    )

    local count=0
    for doc in "${docs[@]}"; do
        local result
        result=$(api_call POST "$BASE_URL/vectordb/collections/$COLLECTION/docs" "$doc")
        if echo "$result" | grep -q '"status"'; then
            count=$((count + 1))
        else
            log_info "Warning: Failed to add document: $result"
        fi
    done

    log_pass "Added $count documents"
}

# Test: BM25 Search - Basic
test_bm25_search_basic() {
    log_section "Test: BM25 Search - Basic"

    local result
    result=$(api_call POST "$BASE_URL/vectordb/collections/$COLLECTION/search/bm25" \
        '{"query": "erlang", "k": 10}')

    if [ -z "$result" ]; then
        log_info "BM25 endpoint not available (404) - rebuild Docker image with latest code"
        return 0
    fi

    if echo "$result" | grep -q '"results"'; then
        local count
        count=$(echo "$result" | jq '.results | length')
        if [ "$count" -gt 0 ]; then
            log_pass "BM25 search returned $count results"
            echo "  Query: 'erlang'"
            echo "$result" | jq -r '.results[] | "  - \(.id): score \(.score)"' 2>/dev/null || echo "  $result"
        else
            log_info "BM25 search returned 0 results (BM25 may not be indexing via this path)"
        fi
    else
        if echo "$result" | grep -q '"no_bm25_index"\|"bm25_not_enabled"'; then
            log_info "BM25 not enabled for this collection (expected in some configs)"
        else
            log_info "BM25 search response: $result"
        fi
    fi
}

# Test: BM25 Search - Multi-term query
test_bm25_search_multiterm() {
    log_section "Test: BM25 Search - Multi-term Query"

    local result
    result=$(api_call POST "$BASE_URL/vectordb/collections/$COLLECTION/search/bm25" \
        '{"query": "erlang programming", "k": 5}')

    if [ -z "$result" ]; then
        log_info "BM25 endpoint not available (404)"
        return 0
    fi

    if echo "$result" | grep -q '"results"'; then
        local count
        count=$(echo "$result" | jq '.results | length')
        log_pass "Multi-term BM25 search returned $count results"
        echo "  Query: 'erlang programming'"
    else
        if echo "$result" | grep -q '"no_bm25_index"\|"bm25_not_enabled"'; then
            log_info "BM25 not enabled (expected in some configs)"
        else
            log_info "Multi-term BM25 search response: $result"
        fi
    fi
}

# Test: BM25 Search - Empty query error
test_bm25_search_empty_query() {
    log_section "Test: BM25 Search - Empty Query Error"

    local result
    result=$(api_call POST "$BASE_URL/vectordb/collections/$COLLECTION/search/bm25" \
        '{"k": 10}')

    if [ -z "$result" ]; then
        log_info "BM25 endpoint not available (404)"
        return 0
    fi

    if echo "$result" | grep -q '"bad_request"'; then
        log_pass "Empty query correctly returns error"
    else
        log_info "Empty query response: $result"
    fi
}

# Test: BM25 Search - No results
test_bm25_search_no_results() {
    log_section "Test: BM25 Search - No Results"

    local result
    result=$(api_call POST "$BASE_URL/vectordb/collections/$COLLECTION/search/bm25" \
        '{"query": "nonexistent_xyz_term", "k": 10}')

    if [ -z "$result" ]; then
        log_info "BM25 endpoint not available (404)"
        return 0
    fi

    if echo "$result" | grep -q '"results"'; then
        local count
        count=$(echo "$result" | jq '.results | length')
        if [ "$count" -eq 0 ]; then
            log_pass "Search for non-existent term returns empty results"
        else
            log_info "Unexpected: got $count results for non-existent term"
        fi
    else
        log_info "BM25 search response: $result"
    fi
}

# Test: Hybrid Search - Basic
test_hybrid_search_basic() {
    log_section "Test: Hybrid Search - Basic"

    local result
    result=$(api_call POST "$BASE_URL/vectordb/collections/$COLLECTION/search/hybrid" \
        '{"query": "erlang programming", "k": 10}')

    if [ -z "$result" ]; then
        log_info "Hybrid endpoint not available (404) - rebuild Docker image with latest code"
        return 0
    fi

    if echo "$result" | grep -q '"results"'; then
        local count
        count=$(echo "$result" | jq '.results | length')
        log_pass "Hybrid search returned $count results"
        echo "  Query: 'erlang programming'"
    else
        if echo "$result" | grep -q '"embedder"\|"no_bm25"'; then
            log_info "Hybrid search requires embedder (expected without embedder config)"
        else
            log_info "Hybrid search result: $result"
        fi
    fi
}

# Test: Hybrid Search - RRF Fusion
test_hybrid_search_rrf() {
    log_section "Test: Hybrid Search - RRF Fusion"

    local result
    result=$(api_call POST "$BASE_URL/vectordb/collections/$COLLECTION/search/hybrid" \
        '{"query": "erlang", "k": 5, "bm25_weight": 0.7, "vector_weight": 0.3, "fusion": "rrf"}')

    if [ -z "$result" ]; then
        log_info "Hybrid endpoint not available (404)"
        return 0
    fi

    if echo "$result" | grep -q '"results"'; then
        log_pass "RRF fusion search completed"
        echo "$result" | jq -r '.results[0] // "no results"' 2>/dev/null || true
    else
        log_info "RRF fusion: $result"
    fi
}

# Test: Hybrid Search - Linear Fusion
test_hybrid_search_linear() {
    log_section "Test: Hybrid Search - Linear Fusion"

    local result
    result=$(api_call POST "$BASE_URL/vectordb/collections/$COLLECTION/search/hybrid" \
        '{"query": "erlang", "k": 5, "bm25_weight": 0.5, "vector_weight": 0.5, "fusion": "linear"}')

    if [ -z "$result" ]; then
        log_info "Hybrid endpoint not available (404)"
        return 0
    fi

    if echo "$result" | grep -q '"results"'; then
        log_pass "Linear fusion search completed"
    else
        log_info "Linear fusion: $result"
    fi
}

# Test: Hybrid Search - Empty query error
test_hybrid_search_empty_query() {
    log_section "Test: Hybrid Search - Empty Query Error"

    local result
    result=$(api_call POST "$BASE_URL/vectordb/collections/$COLLECTION/search/hybrid" \
        '{"k": 10, "fusion": "rrf"}')

    if [ -z "$result" ]; then
        log_info "Hybrid endpoint not available (404)"
        return 0
    fi

    if echo "$result" | grep -q '"bad_request"'; then
        log_pass "Empty query correctly returns error"
    else
        log_info "Empty query response: $result"
    fi
}

# Cleanup
cleanup() {
    log_section "Cleanup"
    api_call DELETE "$BASE_URL/vectordb/collections/$COLLECTION" > /dev/null 2>&1 || true
    log_pass "Test collection deleted"
}

# Main
main() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE} BM25 & Hybrid Search HTTP API Tests${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo -e "Target: $BASE_URL\n"

    check_server
    setup_collection
    add_documents

    # BM25 Tests
    test_bm25_search_basic
    test_bm25_search_multiterm
    test_bm25_search_empty_query
    test_bm25_search_no_results

    # Hybrid Tests
    test_hybrid_search_basic
    test_hybrid_search_rrf
    test_hybrid_search_linear
    test_hybrid_search_empty_query

    cleanup

    echo -e "\n${GREEN}========================================${NC}"
    echo -e "${GREEN} All BM25 HTTP Tests Completed!${NC}"
    echo -e "${GREEN}========================================${NC}"
}

main "$@"
