# Multi-Tenant HTTP Gateway

The barrel_vectordb gateway provides a REST API with multi-tenancy support, enabling multiple tenants to share a single barrel_vectordb deployment with complete data isolation.

## Features

- **Tenant Isolation**: Each tenant's data is transparently prefixed and isolated
- **API Key Authentication**: Secure API keys for tenant identification
- **Rate Limiting**: Configurable requests-per-minute limits per tenant
- **Quota Enforcement**: Limits on vectors, collections, and storage per tenant
- **Backend Abstraction**: Works with both standalone and clustered deployments

## Architecture

```
Tenant Request                     barrel_vectordb
     │                                   │
     ▼                                   │
┌─────────────────────────────────┐      │
│  barrel_vectordb_gateway        │      │
│  ┌───────────────────────────┐  │      │
│  │ Auth: X-Api-Key → tenant  │  │      │
│  └───────────────────────────┘  │      │
│  ┌───────────────────────────┐  │      │
│  │ Rate Limit (token bucket) │  │      │
│  └───────────────────────────┘  │      │
│  ┌───────────────────────────┐  │      │
│  │ Prefix: {hash}_{tenant}_  │──┼──────┘
│  └───────────────────────────┘  │
└─────────────────────────────────┘
```

### Collection Naming

Collections are automatically prefixed with a hash and tenant ID:

```
{4-char-hash}_{tenant_id}_{collection_name}
```

Example: `a3f2_acme_documents`

- `a3f2`: 4-character hex hash of tenant ID (enables RocksDB prefix locality)
- `acme`: tenant ID
- `documents`: collection name

This prefixing is completely transparent to the tenant - they only see their collection name.

## Gateway vs Normal HTTP API

barrel_vectordb has two HTTP interfaces:

| Feature | Normal HTTP API | Gateway |
|---------|-----------------|---------|
| Authentication | None | API key required |
| Multi-tenancy | No | Yes (tenant isolation) |
| Rate limiting | No | Yes (per tenant) |
| Quotas | No | Yes (vectors, collections, storage) |
| Use case | Internal/trusted access | Public/multi-tenant access |

**When to use the Gateway:**
- Multi-tenant SaaS deployments
- Public-facing APIs requiring authentication
- When you need rate limiting and quotas

**When to use Normal HTTP API:**
- Single-tenant deployments
- Internal/trusted network access
- Direct cluster management

**Important:** When the gateway is enabled, you **don't need** the normal HTTP API. The gateway provides:

- **Admin operations** (`/admin/*`) - Create tenants, manage API keys, view usage (requires master key)
- **Tenant operations** (`/v1/*`) - Collections, documents, search (requires tenant API key)

The normal HTTP API can be disabled by not configuring `cluster_options.http`.

### Standalone vs Clustered Mode

The gateway works with both deployment modes:

- **Standalone mode** (`enable_cluster = false`): Gateway manages stores locally. Only the gateway HTTP port is needed.
- **Clustered mode** (`enable_cluster = true`): Gateway routes operations through the cluster. You can disable the normal HTTP API and only expose the gateway.

## Configuration

Enable the gateway in your `sys.config`:

```erlang
{barrel_vectordb, [
    {path, "/var/lib/barrel_vectordb"},
    {gateway, #{
        enabled => true,
        port => 8080,
        master_api_key => <<"your-secret-master-key">>,
        system_db_path => "/var/lib/barrel_vectordb/system",
        default_rate_limit => 100,  % requests per minute
        default_quotas => #{
            max_storage_mb => 1024,
            max_vectors => 100000,
            max_collections => 10
        }
    }}
]}.
```

### Environment Variables (Docker)

| Variable | Description | Default |
|----------|-------------|---------|
| `BARREL_GATEWAY_ENABLED` | Enable gateway | `false` |
| `BARREL_GATEWAY_PORT` | HTTP port | `8080` |
| `BARREL_GATEWAY_MASTER_KEY` | Master API key | (required) |
| `BARREL_GATEWAY_DEFAULT_RPM` | Default rate limit | `100` |
| `BARREL_GATEWAY_MAX_VECTORS` | Default vector quota | `100000` |
| `BARREL_GATEWAY_MAX_COLLECTIONS` | Default collection quota | `10` |
| `BARREL_GATEWAY_MAX_STORAGE_MB` | Default storage quota | `1024` |

## REST API

### Authentication

All requests require an `X-Api-Key` header:

```bash
curl -H "X-Api-Key: bvdb_your_api_key" http://localhost:8080/v1/collections
```

### Admin Endpoints

Admin endpoints require the master API key.

#### Create Tenant

```bash
POST /admin/tenants
Content-Type: application/json
X-Api-Key: <master_key>

{
    "tenant_id": "acme",
    "rpm_limit": 200,
    "max_vectors": 50000,
    "max_collections": 5,
    "max_storage_mb": 512
}
```

Response:
```json
{
    "tenant_id": "acme",
    "api_key": "bvdb_abc123...",
    "status": "created"
}
```

#### Create Additional API Key

```bash
POST /admin/tenants/:tenant_id/keys
Content-Type: application/json
X-Api-Key: <master_key>

{
    "rpm_limit": 100
}
```

#### List API Keys

```bash
GET /admin/tenants/:tenant_id/keys
X-Api-Key: <master_key>
```

#### Revoke API Key

```bash
DELETE /admin/tenants/:tenant_id/keys?key=bvdb_abc123...
X-Api-Key: <master_key>
```

#### Get Tenant Usage

```bash
GET /admin/tenants/:tenant_id/usage
X-Api-Key: <master_key>
```

Response:
```json
{
    "usage": {
        "tenant_id": "acme",
        "vector_count": 1234,
        "collection_count": 3,
        "storage_bytes": 5242880
    },
    "limits": {
        "rpm_limit": 200,
        "max_vectors": 50000,
        "max_collections": 5,
        "max_storage_mb": 512
    }
}
```

### Collection Endpoints

#### List Collections

```bash
GET /v1/collections
X-Api-Key: <tenant_key>
```

Response:
```json
{
    "collections": [
        {"name": "documents"},
        {"name": "products"}
    ]
}
```

#### Create Collection

```bash
POST /v1/collections
Content-Type: application/json
X-Api-Key: <tenant_key>

{
    "name": "documents",
    "dimension": 768,
    "embedder": {
        "type": "ollama",
        "url": "http://ollama:11434",
        "model": "nomic-embed-text"
    }
}
```

#### Get Collection Info

```bash
GET /v1/collections/:name
X-Api-Key: <tenant_key>
```

#### Delete Collection

```bash
DELETE /v1/collections/:name
X-Api-Key: <tenant_key>
```

### Document Endpoints

#### Add Document

```bash
POST /v1/collections/:name/documents
Content-Type: application/json
X-Api-Key: <tenant_key>

{
    "id": "doc-1",
    "text": "This is the document content",
    "metadata": {"category": "tutorial"}
}
```

#### Add Document with Pre-computed Vector

```bash
POST /v1/collections/:name/documents
Content-Type: application/json
X-Api-Key: <tenant_key>

{
    "id": "doc-1",
    "text": "Document content",
    "vector": [0.1, 0.2, ...],
    "metadata": {"category": "tutorial"}
}
```

#### Batch Add Documents

```bash
POST /v1/collections/:name/documents
Content-Type: application/json
X-Api-Key: <tenant_key>

{
    "documents": [
        {"id": "doc-1", "text": "First document"},
        {"id": "doc-2", "text": "Second document"}
    ]
}
```

#### Get Document

```bash
GET /v1/collections/:name/documents/:id
X-Api-Key: <tenant_key>
```

#### Delete Document

```bash
DELETE /v1/collections/:name/documents/:id
X-Api-Key: <tenant_key>
```

### Search Endpoint

#### Text Search

```bash
POST /v1/collections/:name/search
Content-Type: application/json
X-Api-Key: <tenant_key>

{
    "query": "search text",
    "k": 10,
    "filter": {"category": "tutorial"}
}
```

#### Vector Search

```bash
POST /v1/collections/:name/search
Content-Type: application/json
X-Api-Key: <tenant_key>

{
    "vector": [0.1, 0.2, ...],
    "k": 10
}
```

Response:
```json
{
    "hits": [
        {
            "id": "doc-1",
            "score": 0.95,
            "text": "Document content",
            "metadata": {"category": "tutorial"}
        }
    ]
}
```

## Error Responses

| Status | Code | Description |
|--------|------|-------------|
| 401 | `missing_api_key` | No X-Api-Key header |
| 401 | `invalid_api_key` | Invalid API key |
| 401 | `invalid_master_key` | Invalid master key for admin endpoint |
| 403 | `quota_exceeded` | Vector or collection quota exceeded |
| 404 | `collection_not_found` | Collection does not exist |
| 404 | `document_not_found` | Document does not exist |
| 409 | `collection_already_exists` | Collection name already used |
| 429 | `rate_limit_exceeded` | Too many requests |

## Rate Limiting

The gateway uses a token bucket algorithm for rate limiting:

- Each tenant has a configurable RPM (requests per minute) limit
- Tokens refill continuously at `RPM / 60` tokens per second
- Bucket capacity equals the RPM limit
- When tokens are exhausted, requests return `429 Too Many Requests`

Rate limits are per-node (not globally coordinated across cluster nodes).

## Quota Enforcement

Quotas are enforced before operations:

- **Vectors**: Checked before adding documents
- **Collections**: Checked before creating collections
- **Storage**: Tracked but not strictly enforced (approximate)

In clustered mode, quota updates are propagated through Raft consensus.

## Storage

The gateway maintains a separate system RocksDB at the configured `system_db_path`:

```
/var/lib/barrel_vectordb/system/
├── cf_keys           → API keys
├── cf_keys_by_tenant → Secondary index {tenant_id, key} → <<>>
└── cf_quotas         → Tenant quota usage tracking
```

## Clustered Mode

When running with clustering enabled:

1. **API Key Management**: Key creation/deletion goes through Raft consensus
2. **Quota Updates**: Quota changes are replicated to all nodes
3. **Reads**: API key validation and quota checks read from local system DB
4. **Collection Operations**: Routed through cluster APIs

## Security Considerations

1. **Master Key**: Store securely, use for admin operations only
2. **API Keys**: 32-byte random keys with `bvdb_` prefix
3. **Key Rotation**: Create new keys, then revoke old ones
4. **Network**: Use TLS termination proxy in production

## Monitoring

Monitor gateway health via:

```bash
# Check if gateway is accepting requests
curl -sf http://localhost:8080/v1/collections -H "X-Api-Key: $KEY"

# Get tenant usage
curl -H "X-Api-Key: $MASTER_KEY" http://localhost:8080/admin/tenants/acme/usage
```

## Example: Complete Workflow

```bash
# Set master key
MASTER_KEY="your-master-key"
GATEWAY="http://localhost:8080"

# Create a tenant
RESPONSE=$(curl -s -X POST "$GATEWAY/admin/tenants" \
  -H "X-Api-Key: $MASTER_KEY" \
  -H "Content-Type: application/json" \
  -d '{"tenant_id": "acme", "max_vectors": 10000}')

TENANT_KEY=$(echo $RESPONSE | jq -r '.api_key')

# Create a collection
curl -X POST "$GATEWAY/v1/collections" \
  -H "X-Api-Key: $TENANT_KEY" \
  -H "Content-Type: application/json" \
  -d '{"name": "docs", "dimension": 768}'

# Add documents
curl -X POST "$GATEWAY/v1/collections/docs/documents" \
  -H "X-Api-Key: $TENANT_KEY" \
  -H "Content-Type: application/json" \
  -d '{"documents": [
    {"id": "1", "text": "Introduction to vector databases"},
    {"id": "2", "text": "How embeddings work"},
    {"id": "3", "text": "Similarity search algorithms"}
  ]}'

# Search
curl -X POST "$GATEWAY/v1/collections/docs/search" \
  -H "X-Api-Key: $TENANT_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "vector search", "k": 2}'

# Check usage
curl "$GATEWAY/admin/tenants/acme/usage" \
  -H "X-Api-Key: $MASTER_KEY"
```
