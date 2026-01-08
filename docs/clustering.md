# Clustering

Barrel VectorDB supports optional clustering for horizontal scaling. Clustering adds sharding with consistent hash partitioning and automatic rebalancing.

## Overview

```
barrel_vectordb (single project)
├── Standalone (default)         # Current behavior, unchanged
│   └── Local storage only
│
└── Clustered (after start_cluster/1 or enable_cluster=true)
    ├── Ra/Raft for consensus (shard metadata)
    ├── aten for failure detection (fast: 1-5s)
    ├── Consistent hash ring for sharding
    └── Optional HTTP API
```

## Configuration

### Standalone Mode (Default)

```erlang
{barrel_vectordb, [
    {enable_cluster, false},
    {path, "data/vectors"}
]}
```

### Cluster Mode

```erlang
{barrel_vectordb, [
    {enable_cluster, true},
    {cluster_options, #{
        cluster_name => barrel_vectors,
        seed_nodes => [
            'barrel@paris.enki.io',
            'barrel@lille.enki.io'
        ],
        sharding => #{
            replication_factor => 2
        }
    }},
    {path, "data/vectors"}
]}
```

### Standalone HTTP Server

For standalone deployments (not embedded in another application):

```erlang
{barrel_vectordb, [
    {enable_cluster, true},
    {cluster_options, #{
        cluster_name => barrel_vectors,
        seed_nodes => [...],
        http => #{
            ip => {0, 0, 0, 0},
            port => 8080
        },
        sharding => #{
            replication_factor => 2
        }
    }},
    {path, "data/vectors"}
]}
```

## Cluster API

### Start Cluster

```erlang
%% Start cluster explicitly (if enable_cluster = false)
barrel_vectordb:start_cluster(#{
    cluster_name => barrel_vectors,
    seed_nodes => ['barrel@lille.enki.io']
}).
```

### Join/Leave Cluster

```erlang
%% Join existing cluster
barrel_vectordb:cluster_join(['barrel@paris.enki.io', 'barrel@lille.enki.io']).

%% Leave cluster gracefully
barrel_vectordb:cluster_leave().
```

### Cluster Status

```erlang
%% Check cluster status
barrel_vectordb:cluster_status().
%% => #{state => member, nodes => [...], leader => ..., is_leader => true/false}

%% Get healthy nodes
barrel_vectordb:cluster_nodes().
%% => ['barrel@paris.enki.io', 'barrel@lille.enki.io', ...]

%% Check if clustered
barrel_vectordb:is_clustered().
%% => true | false
```

## Cluster Document Operations

Explicit cluster operations that route to the correct shard:

```erlang
%% Add document (routes to shard based on ID hash)
barrel_vectordb:cluster_add(Collection, Id, Text, Metadata).
barrel_vectordb:cluster_add_vector(Collection, Id, Text, Metadata, Vector).

%% Get document
barrel_vectordb:cluster_get(Collection, Id).

%% Delete document
barrel_vectordb:cluster_delete(Collection, Id).

%% Search (scatter-gather across all shards)
barrel_vectordb:cluster_search(Collection, Query, Opts).
barrel_vectordb:cluster_search_vector(Collection, Vector, Opts).
```

## Sharding Strategy

### How It Works

Documents are distributed across nodes using consistent hashing on the document ID:

```
Collection "memories" with 4 nodes:
┌─────────────────────────────────────────────────────────────┐
│                    Consistent Hash Ring                      │
│                                                              │
│    doc_001 ──hash──▶ Paris      (shard 0-25%)               │
│    doc_002 ──hash──▶ Lille      (shard 25-50%)              │
│    doc_003 ──hash──▶ Amsterdam  (shard 50-75%)              │
│    doc_004 ──hash──▶ Geneva     (shard 75-100%)             │
└─────────────────────────────────────────────────────────────┘
```

### Operation Routing

| Operation | Routing | Notes |
|-----------|---------|-------|
| **add/update/delete** | Hash(doc_id) → single node | Fast, single hop |
| **get** | Hash(doc_id) → single node | Fast, single hop |
| **search** | Scatter to ALL nodes, gather results | Parallel queries, merge top-K |

### Replication

Each shard is replicated to R nodes (configurable, default R=2):

- **Leader**: Handles writes, replicates to followers
- **Followers**: Sync'd via Raft, can serve reads

## Leader/Follower Model

- Each shard has ONE leader and N-1 followers (replicas)
- Leader handles writes, replicates to followers
- Followers can serve reads (configurable)
- On leader failure: coordinator promotes a follower

### Rebalancing

On node failure:

1. `promote_new_leader` - if failed node was leader
2. `remove_failed_replica` - if failed node was replica
3. `maybe_add_replacement_replica` - maintain replication factor

## Embedding in Other Applications

When barrel_vectordb is embedded in another application (like barrel_memory), the HTTP routes can be mounted directly:

```erlang
%% In your HTTP server setup
cowboy_routes() ->
    YourRoutes = [...],
    VectordbRoutes = barrel_vectordb_http_routes:routes(),
    YourRoutes ++ VectordbRoutes.
```

Route groups are available separately:

```erlang
%% Cluster status endpoints only
ClusterRoutes = barrel_vectordb_http_routes:cluster_routes().
%% => /vectordb/cluster/status, /vectordb/cluster/nodes

%% Collection/document/search endpoints
CollectionRoutes = barrel_vectordb_http_routes:collection_routes().
%% => /vectordb/collections/*, /vectordb/collections/:collection/docs/*, etc.

%% All routes combined
AllRoutes = barrel_vectordb_http_routes:routes().
```

Custom prefix:

```erlang
%% Mount under /api/v1 instead of /vectordb
Routes = barrel_vectordb_http_routes:routes(<<"/api/v1">>).
```

## Failure Detection

Cluster uses [aten](https://github.com/rabbitmq/aten) (via Ra) for fast failure detection:

- Detection time: 1-5 seconds
- Adaptive heartbeat intervals
- Network partition handling

## Architecture

- **Ra/Raft**: Consensus for shard metadata and leader election
- **aten**: Fast failure detection (included with Ra)
- **Consistent hashing**: Document distribution
- **Scatter-gather**: Parallel search across shards
- **Async replication**: Leader queues writes, batch replicates to followers
