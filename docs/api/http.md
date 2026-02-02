# HTTP API

Barrel VectorDB provides an optional HTTP API for cluster deployments. The API works in both clustered and standalone modes.

## Endpoints

### Collections

#### List Collections

```
GET /vectordb/collections
```

Returns list of all collections.

**Response:**

```json
{
  "collection_name": {
    "dimension": 768,
    "shards": 4,
    "replication_factor": 2
  }
}
```

#### Get Collection

```
GET /vectordb/collections/:collection
```

Returns collection metadata.

**Response:**

```json
{
  "name": "my_collection",
  "dimension": 768,
  "count": 1000
}
```

#### Create Collection

```
PUT /vectordb/collections/:collection
```

**Request Body:**

```json
{
  "dimensions": 768,
  "num_shards": 4,
  "replication_factor": 2
}
```

**Response:**

```json
{
  "status": "created"
}
```

#### Delete Collection

```
DELETE /vectordb/collections/:collection
```

**Response:**

```json
{
  "status": "deleted"
}
```

### Documents

#### Add Document

```
POST /vectordb/collections/:collection/docs
```

**Request Body (with text - requires embedder):**

```json
{
  "id": "doc-123",
  "text": "Hello world",
  "metadata": {
    "category": "greeting"
  }
}
```

**Request Body (with vector):**

```json
{
  "id": "doc-123",
  "text": "Hello world",
  "metadata": {},
  "vector": [0.1, 0.2, 0.3, ...]
}
```

**Response:**

```json
{
  "status": "created",
  "id": "doc-123"
}
```

#### Get Document

```
GET /vectordb/collections/:collection/docs/:id
```

**Response:**

```json
{
  "id": "doc-123",
  "text": "Hello world",
  "metadata": {
    "category": "greeting"
  }
}
```

#### Delete Document

```
DELETE /vectordb/collections/:collection/docs/:id
```

**Response:**

```json
{
  "status": "deleted"
}
```

### Search

#### Search Collection

```
POST /vectordb/collections/:collection/search
```

**Request Body (text query - requires embedder):**

```json
{
  "query": "hi there",
  "k": 10
}
```

**Request Body (vector query):**

```json
{
  "vector": [0.1, 0.2, 0.3, ...],
  "k": 10
}
```

**Response:**

```json
{
  "results": [
    {
      "id": "doc-123",
      "text": "Hello world",
      "score": 0.89,
      "metadata": {
        "category": "greeting"
      }
    }
  ]
}
```

#### BM25 Search

Keyword-based search using BM25 ranking. Requires `bm25_backend` to be configured for the collection.

```
POST /vectordb/collections/:collection/search/bm25
```

**Request Body:**

```json
{
  "query": "erlang programming",
  "k": 10
}
```

**Response:**

```json
{
  "results": [
    {
      "id": "doc-456",
      "score": 2.45
    },
    {
      "id": "doc-789",
      "score": 1.82
    }
  ]
}
```

| Status | Error Code | Description |
|--------|------------|-------------|
| 200 | - | Search completed successfully |
| 400 | `bad_request` | Missing or invalid query |
| 400 | `no_bm25_index` | BM25 not configured for collection |

#### Hybrid Search

Combined BM25 keyword search and vector semantic search with score fusion.

```
POST /vectordb/collections/:collection/search/hybrid
```

**Request Body:**

```json
{
  "query": "erlang concurrency",
  "k": 10,
  "bm25_weight": 0.5,
  "vector_weight": 0.5,
  "fusion": "rrf"
}
```

**Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | string | required | Search query text |
| `k` | integer | 10 | Number of results to return |
| `bm25_weight` | float | 0.5 | Weight for BM25 scores (0-1) |
| `vector_weight` | float | 0.5 | Weight for vector scores (0-1) |
| `fusion` | string | "rrf" | Fusion algorithm: `rrf` or `linear` |

**Fusion Algorithms:**

- `rrf` - Reciprocal Rank Fusion (recommended): Combines rankings using `1/(k + rank)` formula
- `linear` - Linear combination: Weighted sum of normalized scores

**Response:**

```json
{
  "results": [
    {
      "id": "doc-456",
      "score": 0.85,
      "bm25_score": 2.45,
      "vector_score": 0.92
    }
  ]
}
```

| Status | Error Code | Description |
|--------|------------|-------------|
| 200 | - | Search completed successfully |
| 400 | `bad_request` | Missing or invalid query |
| 400 | `no_bm25_index` | BM25 not configured for collection |

### Cluster Status

#### Get Cluster Status

```
GET /vectordb/cluster/status
```

**Response:**

```json
{
  "state": "member",
  "is_leader": true,
  "leader": "barrel@paris.enki.io"
}
```

#### Get Cluster Nodes

```
GET /vectordb/cluster/nodes
```

**Response:**

```json
{
  "nodes": [
    "barrel@paris.enki.io",
    "barrel@lille.enki.io"
  ]
}
```

#### Leave Cluster

Gracefully remove the current node from the cluster.

```
POST /vectordb/cluster/leave
```

**Response:**

```json
{
  "status": "leaving",
  "node": "barrel@paris.enki.io"
}
```

| Status | Error Code | Description |
|--------|------------|-------------|
| 200 | - | Node is leaving the cluster |
| 400 | `not_member` | Node is not a cluster member |
| 400 | `not_clustered` | Node is not in cluster mode |

!!! warning "Irreversible"
    Once a node leaves, it must rejoin as a new node with fresh data.

### Collection Operations

#### Reshard Collection

Change the number of shards for a collection. Documents are automatically migrated.

```
POST /vectordb/collections/:collection/reshard
```

**Request Body:**

```json
{
  "num_shards": 4
}
```

**Response:**

```json
{
  "status": "resharding",
  "info": {
    "old_shards": 2,
    "new_shards": 4,
    "documents_migrated": 1000
  }
}
```

| Status | Error Code | Description |
|--------|------------|-------------|
| 200 | - | Resharding completed successfully |
| 400 | `bad_request` | Invalid or missing num_shards |
| 400 | `same_shard_count` | New shard count equals current |
| 400 | `not_clustered` | Only available in cluster mode |
| 404 | `not_found` | Collection not found |

!!! note "Duration"
    Resharding duration depends on collection size. The collection remains readable during the operation.

## Error Responses

All error responses have the format:

```json
{
  "error": "error_code",
  "message": "Human readable message"
}
```

| Status | Error Code | Description |
|--------|------------|-------------|
| 400 | `bad_request` | Invalid JSON body or missing required fields |
| 404 | `not_found` | Collection or document not found |
| 405 | `method_not_allowed` | HTTP method not supported |
| 409 | `already_exists` | Collection already exists |
| 500 | `error` | Internal server error |

## Standalone vs Clustered Mode

The HTTP API transparently handles both modes:

- **Standalone**: Documents stored in local store named after collection
- **Clustered**: Documents routed to appropriate shard based on ID hash

To check the current mode:

```
GET /vectordb/cluster/status
```

In standalone mode, returns:

```json
{
  "state": "standalone",
  "is_leader": false
}
```

## Configuration

### Enable HTTP Server

In standalone deployments, enable the HTTP server in cluster options:

```erlang
{barrel_vectordb, [
    {enable_cluster, true},
    {cluster_options, #{
        http => #{
            ip => {0, 0, 0, 0},
            port => 8080,
            num_acceptors => 100
        }
    }}
]}
```

### Embedding Routes

When embedded in another application, mount routes without starting a separate HTTP server:

```erlang
%% Get all routes
Routes = barrel_vectordb_http_routes:routes().

%% Or with custom prefix
Routes = barrel_vectordb_http_routes:routes(<<"/api/vectors">>).

%% Or separate route groups
ClusterRoutes = barrel_vectordb_http_routes:cluster_routes().
CollectionRoutes = barrel_vectordb_http_routes:collection_routes().
```
