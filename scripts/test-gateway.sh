#!/bin/bash
# ============================================
# Barrel VectorDB Gateway Test Script
# ============================================
#
# Tests the multi-tenant gateway functionality.
#
# Usage:
#   ./scripts/test-gateway.sh [GATEWAY_URL] [MASTER_KEY]
#
# Arguments:
#   GATEWAY_URL - Gateway endpoint (default: http://localhost:8080)
#   MASTER_KEY  - Master API key (default: test-master-key)
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

GATEWAY_URL="${1:-http://localhost:8080}"
MASTER_KEY="${2:-test-master-key}"

PASSED=0
FAILED=0

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_pass() {
    echo -e "${GREEN}[PASS]${NC} $1"
    ((PASSED++))
}

log_fail() {
    echo -e "${RED}[FAIL]${NC} $1"
    ((FAILED++))
}

log_section() {
    echo ""
    echo -e "${YELLOW}=== $1 ===${NC}"
}

# Make HTTP request and return response
do_request() {
    local method="$1"
    local path="$2"
    local key="$3"
    local data="${4:-}"

    if [ -n "$data" ]; then
        curl -s -X "$method" \
            -H "X-Api-Key: $key" \
            -H "Content-Type: application/json" \
            -d "$data" \
            "${GATEWAY_URL}${path}"
    else
        curl -s -X "$method" \
            -H "X-Api-Key: $key" \
            "${GATEWAY_URL}${path}"
    fi
}

# Make HTTP request and return status code
get_status() {
    local method="$1"
    local path="$2"
    local key="$3"
    local data="${4:-}"

    if [ -n "$data" ]; then
        curl -s -o /dev/null -w "%{http_code}" -X "$method" \
            -H "X-Api-Key: $key" \
            -H "Content-Type: application/json" \
            -d "$data" \
            "${GATEWAY_URL}${path}"
    else
        curl -s -o /dev/null -w "%{http_code}" -X "$method" \
            -H "X-Api-Key: $key" \
            "${GATEWAY_URL}${path}"
    fi
}

# ============================================
# Wait for gateway to be ready
# ============================================

log_section "Waiting for gateway to be ready"

MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    STATUS=$(get_status GET "/v1/collections" "$MASTER_KEY" 2>/dev/null || echo "000")
    if [ "$STATUS" = "200" ]; then
        log_info "Gateway is ready!"
        break
    fi
    log_info "Waiting for gateway... (attempt $((RETRY_COUNT+1))/$MAX_RETRIES)"
    sleep 2
    ((RETRY_COUNT++))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    log_fail "Gateway did not become ready in time"
    exit 1
fi

# ============================================
# Test: Authentication
# ============================================

log_section "Authentication Tests"

# Test missing API key
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X GET "${GATEWAY_URL}/v1/collections")
if [ "$STATUS" = "401" ]; then
    log_pass "Missing API key returns 401"
else
    log_fail "Missing API key should return 401, got $STATUS"
fi

# Test invalid API key
STATUS=$(get_status GET "/v1/collections" "invalid-key")
if [ "$STATUS" = "401" ]; then
    log_pass "Invalid API key returns 401"
else
    log_fail "Invalid API key should return 401, got $STATUS"
fi

# Test master key works for admin
STATUS=$(get_status GET "/admin/tenants/test/usage" "$MASTER_KEY")
if [ "$STATUS" = "200" ] || [ "$STATUS" = "404" ]; then
    log_pass "Master key can access admin endpoints"
else
    log_fail "Master key should access admin endpoints, got $STATUS"
fi

# ============================================
# Test: Tenant Management
# ============================================

log_section "Tenant Management Tests"

# Create tenant
RESPONSE=$(do_request POST "/admin/tenants" "$MASTER_KEY" '{"tenant_id": "test_tenant", "rpm_limit": 200, "max_vectors": 10000, "max_collections": 5}')
TENANT_KEY=$(echo "$RESPONSE" | grep -o '"api_key":"[^"]*"' | cut -d'"' -f4)

if [ -n "$TENANT_KEY" ] && [[ "$TENANT_KEY" == bvdb_* ]]; then
    log_pass "Created tenant with API key: ${TENANT_KEY:0:20}..."
else
    log_fail "Failed to create tenant. Response: $RESPONSE"
    TENANT_KEY=""
fi

# Try to create duplicate tenant
if [ -n "$TENANT_KEY" ]; then
    STATUS=$(get_status POST "/admin/tenants" "$MASTER_KEY" '{"tenant_id": "test_tenant"}')
    if [ "$STATUS" = "409" ]; then
        log_pass "Duplicate tenant returns 409"
    else
        log_fail "Duplicate tenant should return 409, got $STATUS"
    fi
fi

# List tenant keys
if [ -n "$TENANT_KEY" ]; then
    RESPONSE=$(do_request GET "/admin/tenants/test_tenant/keys" "$MASTER_KEY")
    if echo "$RESPONSE" | grep -q "keys"; then
        log_pass "List tenant keys works"
    else
        log_fail "Failed to list tenant keys. Response: $RESPONSE"
    fi
fi

# Get tenant usage
if [ -n "$TENANT_KEY" ]; then
    RESPONSE=$(do_request GET "/admin/tenants/test_tenant/usage" "$MASTER_KEY")
    if echo "$RESPONSE" | grep -q "usage"; then
        log_pass "Get tenant usage works"
    else
        log_fail "Failed to get tenant usage. Response: $RESPONSE"
    fi
fi

# ============================================
# Test: Collection Operations
# ============================================

log_section "Collection Operations Tests"

if [ -n "$TENANT_KEY" ]; then
    # Create collection
    RESPONSE=$(do_request POST "/v1/collections" "$TENANT_KEY" '{"name": "test_docs", "dimension": 384}')
    if echo "$RESPONSE" | grep -q "created\|test_docs"; then
        log_pass "Created collection 'test_docs'"
    else
        log_fail "Failed to create collection. Response: $RESPONSE"
    fi

    # List collections
    RESPONSE=$(do_request GET "/v1/collections" "$TENANT_KEY")
    if echo "$RESPONSE" | grep -q "test_docs"; then
        log_pass "Collection appears in list"
    else
        log_fail "Collection not in list. Response: $RESPONSE"
    fi

    # Get collection info
    RESPONSE=$(do_request GET "/v1/collections/test_docs" "$TENANT_KEY")
    if echo "$RESPONSE" | grep -q "test_docs\|dimension"; then
        log_pass "Get collection info works"
    else
        log_fail "Failed to get collection info. Response: $RESPONSE"
    fi

    # Try to create duplicate collection
    STATUS=$(get_status POST "/v1/collections" "$TENANT_KEY" '{"name": "test_docs", "dimension": 384}')
    if [ "$STATUS" = "409" ]; then
        log_pass "Duplicate collection returns 409"
    else
        log_fail "Duplicate collection should return 409, got $STATUS"
    fi
fi

# ============================================
# Test: Document Operations
# ============================================

log_section "Document Operations Tests"

if [ -n "$TENANT_KEY" ]; then
    # Add document with vector
    VECTOR=$(python3 -c "import json; print(json.dumps([0.1] * 384))" 2>/dev/null || echo "[$(seq -s, 0.001 0.001 0.384)]")
    RESPONSE=$(do_request POST "/v1/collections/test_docs/documents" "$TENANT_KEY" "{\"id\": \"doc1\", \"text\": \"Hello world test document\", \"vector\": $VECTOR, \"metadata\": {\"type\": \"test\"}}")
    if echo "$RESPONSE" | grep -q "ok\|doc1\|created"; then
        log_pass "Added document 'doc1'"
    else
        log_fail "Failed to add document. Response: $RESPONSE"
    fi

    # Add batch documents
    RESPONSE=$(do_request POST "/v1/collections/test_docs/documents" "$TENANT_KEY" "{\"documents\": [{\"id\": \"doc2\", \"text\": \"Second document\", \"vector\": $VECTOR}, {\"id\": \"doc3\", \"text\": \"Third document\", \"vector\": $VECTOR}]}")
    if echo "$RESPONSE" | grep -q "ok\|created\|doc"; then
        log_pass "Added batch documents"
    else
        log_fail "Failed to add batch documents. Response: $RESPONSE"
    fi

    # Get document
    RESPONSE=$(do_request GET "/v1/collections/test_docs/documents/doc1" "$TENANT_KEY")
    if echo "$RESPONSE" | grep -q "doc1\|Hello"; then
        log_pass "Get document works"
    else
        log_fail "Failed to get document. Response: $RESPONSE"
    fi

    # Delete document
    STATUS=$(get_status DELETE "/v1/collections/test_docs/documents/doc3" "$TENANT_KEY")
    if [ "$STATUS" = "200" ] || [ "$STATUS" = "204" ]; then
        log_pass "Delete document works"
    else
        log_fail "Delete document failed, got $STATUS"
    fi
fi

# ============================================
# Test: Search Operations
# ============================================

log_section "Search Operations Tests"

if [ -n "$TENANT_KEY" ]; then
    # Vector search
    VECTOR=$(python3 -c "import json; print(json.dumps([0.1] * 384))" 2>/dev/null || echo "[$(seq -s, 0.001 0.001 0.384)]")
    RESPONSE=$(do_request POST "/v1/collections/test_docs/search" "$TENANT_KEY" "{\"vector\": $VECTOR, \"k\": 5}")
    if echo "$RESPONSE" | grep -q "hits\|doc"; then
        log_pass "Vector search works"
    else
        log_fail "Vector search failed. Response: $RESPONSE"
    fi
fi

# ============================================
# Test: Tenant Isolation
# ============================================

log_section "Tenant Isolation Tests"

# Create second tenant
RESPONSE=$(do_request POST "/admin/tenants" "$MASTER_KEY" '{"tenant_id": "other_tenant", "max_vectors": 10000}')
OTHER_KEY=$(echo "$RESPONSE" | grep -o '"api_key":"[^"]*"' | cut -d'"' -f4)

if [ -n "$OTHER_KEY" ]; then
    # Other tenant should not see first tenant's collections
    RESPONSE=$(do_request GET "/v1/collections" "$OTHER_KEY")
    if echo "$RESPONSE" | grep -q "test_docs"; then
        log_fail "Other tenant can see first tenant's collection!"
    else
        log_pass "Tenant collections are isolated"
    fi

    # Other tenant cannot access first tenant's documents
    STATUS=$(get_status GET "/v1/collections/test_docs/documents/doc1" "$OTHER_KEY")
    if [ "$STATUS" = "404" ]; then
        log_pass "Tenant documents are isolated"
    else
        log_fail "Other tenant can access first tenant's documents, got $STATUS"
    fi
fi

# ============================================
# Test: Rate Limiting
# ============================================

log_section "Rate Limiting Tests"

if [ -n "$TENANT_KEY" ]; then
    # Make many requests quickly (should eventually get rate limited)
    RATE_LIMITED=0
    for i in $(seq 1 250); do
        STATUS=$(get_status GET "/v1/collections" "$TENANT_KEY")
        if [ "$STATUS" = "429" ]; then
            RATE_LIMITED=1
            break
        fi
    done

    if [ $RATE_LIMITED -eq 1 ]; then
        log_pass "Rate limiting works (got 429 after $i requests)"
    else
        log_info "Rate limiting not triggered (may need more requests or higher rate limit)"
    fi
fi

# ============================================
# Test: Quota Enforcement
# ============================================

log_section "Quota Enforcement Tests"

# Create tenant with low limits
RESPONSE=$(do_request POST "/admin/tenants" "$MASTER_KEY" '{"tenant_id": "limited_tenant", "max_collections": 2, "max_vectors": 10}')
LIMITED_KEY=$(echo "$RESPONSE" | grep -o '"api_key":"[^"]*"' | cut -d'"' -f4)

if [ -n "$LIMITED_KEY" ]; then
    # Create collections up to limit
    do_request POST "/v1/collections" "$LIMITED_KEY" '{"name": "col1", "dimension": 384}' > /dev/null
    do_request POST "/v1/collections" "$LIMITED_KEY" '{"name": "col2", "dimension": 384}' > /dev/null

    # Third collection should fail
    STATUS=$(get_status POST "/v1/collections" "$LIMITED_KEY" '{"name": "col3", "dimension": 384}')
    if [ "$STATUS" = "403" ]; then
        log_pass "Collection quota enforced"
    else
        log_fail "Collection quota not enforced, got $STATUS"
    fi
fi

# ============================================
# Cleanup
# ============================================

log_section "Cleanup"

if [ -n "$TENANT_KEY" ]; then
    # Delete collection
    STATUS=$(get_status DELETE "/v1/collections/test_docs" "$TENANT_KEY")
    if [ "$STATUS" = "200" ] || [ "$STATUS" = "204" ]; then
        log_pass "Deleted test collection"
    else
        log_fail "Failed to delete collection, got $STATUS"
    fi
fi

# ============================================
# Summary
# ============================================

echo ""
echo "============================================"
echo -e "Test Results: ${GREEN}$PASSED passed${NC}, ${RED}$FAILED failed${NC}"
echo "============================================"

if [ $FAILED -gt 0 ]; then
    exit 1
fi

exit 0
