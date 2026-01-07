# barrel_vectordb Cluster Mode Plan

## Overview

Add optional clustering support directly to barrel_vectordb, making it a self-contained vector database that works in standalone OR distributed mode.

**Goal**: `barrel_vectordb_cluster` becomes deprecated; barrel_vectordb handles clustering natively.

## Current State

```
barrel_vectordb (standalone)     barrel_vectordb_cluster (separate project)
├── Single node only             ├── Ra/Raft consensus
├── RocksDB storage              ├── Shard management
├── HNSW/FAISS indexes           ├── HTTP API
└── Embedding providers          └── Discovery (seed/DNS)
```

## Target State

```
barrel_vectordb
├── mode: standalone (default)   # Current behavior
├── mode: mesh                   # New: peer-to-peer cluster
│   ├── Ra/Raft consensus
│   ├── aten failure detection
│   ├── Consistent hashing
│   └── Node discovery
└── Transparent API              # Same API, routing handled internally
```

## Configuration

### Standalone Mode (Default - No Change)

```erlang
{barrel_vectordb, [
    {mode, standalone},
    {path, "data/vectors"},
    {dimensions, 768}
]}
```

### Mesh Mode (New)

```erlang
{barrel_vectordb, [
    {mode, mesh},
    {cluster_name, barrel_vectors},
    {nodes, [
        'barrel@paris.enki.io',
        'barrel@lille.enki.io',
        'barrel@amsterdam.enki.io',
        'barrel@geneva.enki.io'
    ]},
    {discovery, #{
        mode => static,        % static | dns | seed
        %% For dns mode:
        %% domain => <<"barrel.enki.io">>,
        %% type => srv
    }},
    {path, "data/vectors"},
    {dimensions, 768}
]}
```

## Architecture

### Module Structure

```
barrel_vectordb/
├── src/
│   ├── barrel_vectordb.erl              # Public API (add routing)
│   ├── barrel_vectordb_app.erl          # Mode-aware startup
│   ├── barrel_vectordb_sup.erl          # Mode-aware supervision
│   │
│   ├── # Core (existing - unchanged)
│   ├── barrel_vectordb_server.erl
│   ├── barrel_vectordb_store.erl
│   ├── barrel_vectordb_hnsw.erl
│   ├── barrel_vectordb_embed.erl
│   │
│   ├── # Clustering (new)
│   ├── cluster/
│   │   ├── barrel_vectordb_mesh.erl         # Cluster coordinator
│   │   ├── barrel_vectordb_mesh_sup.erl     # Cluster supervision
│   │   ├── barrel_vectordb_ra.erl           # Ra/Raft state machine
│   │   ├── barrel_vectordb_ra_sm.erl        # Ra state machine callbacks
│   │   ├── barrel_vectordb_router.erl       # Request routing
│   │   ├── barrel_vectordb_discovery.erl    # Node discovery
│   │   └── barrel_vectordb_health.erl       # aten-based health
│   │
│   └── # Optional HTTP (moved from cluster project)
│   └── http/
│       ├── barrel_vectordb_http.erl
│       └── barrel_vectordb_http_handlers.erl
```

### Supervision Tree

```
barrel_vectordb_sup
├── [standalone mode]
│   └── barrel_vectordb_store_sup (existing)
│
└── [mesh mode]
    ├── barrel_vectordb_store_sup (existing - local storage)
    └── barrel_vectordb_mesh_sup (new)
        ├── barrel_vectordb_mesh (coordinator)
        ├── barrel_vectordb_ra (raft consensus)
        ├── barrel_vectordb_discovery (node discovery)
        └── barrel_vectordb_health (aten monitoring)
```

## Key Components

### 1. barrel_vectordb_mesh.erl

Cluster coordinator - manages node membership and cluster state.

```erlang
-module(barrel_vectordb_mesh).
-behaviour(gen_server).

-export([
    start_link/0,
    nodes/0,
    healthy_nodes/0,
    is_clustered/0,
    local_node_id/0,
    node_for_key/1
]).

%% Uses aten for failure detection
%% Uses Ra for consensus on cluster state
%% Provides routing information
```

### 2. barrel_vectordb_router.erl

Routes requests to correct node based on consistent hashing.

```erlang
-module(barrel_vectordb_router).

-export([
    route/3,           % (Collection, Key, Fun) -> Result
    route_search/2,    % (Collection, Fun) -> scatter-gather
    node_for_key/2     % (Collection, Key) -> Node
]).

%% Consistent hashing for writes
%% Scatter-gather for searches (query all nodes, merge results)
```

### 3. barrel_vectordb_health.erl

aten-based failure detection.

```erlang
-module(barrel_vectordb_health).
-behaviour(gen_server).

-export([
    start_link/0,
    is_healthy/1,
    healthy_nodes/0
]).

%% Registers with aten for each peer node
%% Tracks node health status
%% Fast failure detection (1-5s vs 45-90s net_ticktime)
```

### 4. barrel_vectordb.erl Changes

Transparent routing - API unchanged, routing handled internally.

```erlang
%% Before (standalone only)
add(Store, Id, Text, Metadata) ->
    barrel_vectordb_server:add(Store, Id, Text, Metadata).

%% After (mode-aware)
add(Store, Id, Text, Metadata) ->
    case get_mode() of
        standalone ->
            barrel_vectordb_server:add(Store, Id, Text, Metadata);
        mesh ->
            barrel_vectordb_router:route(Store, Id, fun() ->
                barrel_vectordb_server:add(Store, Id, Text, Metadata)
            end)
    end.

search(Store, Query, Opts) ->
    case get_mode() of
        standalone ->
            barrel_vectordb_server:search(Store, Query, Opts);
        mesh ->
            %% Scatter-gather: query all nodes, merge results
            barrel_vectordb_router:route_search(Store, fun() ->
                barrel_vectordb_server:search(Store, Query, Opts)
            end)
    end.
```

## Dependencies

### Current

```erlang
{deps, [
    {rocksdb, "2.4.1"},
    {hackney, "1.20.1"},
    {gen_batch_server, "0.8.8"}
]}.
```

### With Clustering

```erlang
{deps, [
    {rocksdb, "2.4.1"},
    {hackney, "1.20.1"},
    {gen_batch_server, "0.8.8"},
    %% New (only loaded in mesh mode)
    {ra, "2.15.0"},       % Includes aten
    {cowboy, "2.12.0"}    % Optional HTTP API
]}.
```

Note: Ra includes aten as a dependency, so no separate dep needed.

## Implementation Phases

### Phase 1: Core Clustering Infrastructure

1. Add `{mode, standalone | mesh}` configuration
2. Create `barrel_vectordb_mesh_sup.erl` (conditional supervision)
3. Create `barrel_vectordb_mesh.erl` (cluster coordinator)
4. Create `barrel_vectordb_health.erl` (aten integration)
5. Modify `barrel_vectordb_sup.erl` for mode-aware startup
6. Add Ra and aten dependencies

**Deliverable**: Nodes can form cluster, detect failures via aten

### Phase 2: Consensus & Routing

1. Create `barrel_vectordb_ra.erl` (Ra server wrapper)
2. Create `barrel_vectordb_ra_sm.erl` (state machine)
3. Create `barrel_vectordb_router.erl` (consistent hashing)
4. Modify `barrel_vectordb.erl` for transparent routing
5. Implement scatter-gather for searches

**Deliverable**: Distributed writes and searches work

### Phase 3: Discovery & Operations

1. Create `barrel_vectordb_discovery.erl` (static/dns/seed)
2. Add cluster management API (join, leave, status)
3. Add metrics for clustering (prometheus integration)
4. Create operational documentation

**Deliverable**: Production-ready clustering

### Phase 4: HTTP API (Optional)

1. Move HTTP handlers from barrel_vectordb_cluster
2. Make HTTP optional (`{http_enabled, true/false}`)
3. Add cluster endpoints (/cluster/status, /cluster/nodes)

**Deliverable**: HTTP API for cluster management

### Phase 5: Migration & Deprecation

1. Migrate barrel_vectordb_cluster users to barrel_vectordb
2. Archive barrel_vectordb_cluster repository
3. Update barrel_memory to use new API

**Deliverable**: Single project for all vector DB needs

## Data Distribution Strategy

### Option A: Full Replication (Simpler)

Every node has complete copy of all collections.
- Writes: Ra consensus ensures all nodes get update
- Reads: Local reads, no network hop
- Search: Local search, no scatter-gather

**Pros**: Simple, fast reads
**Cons**: Storage = N * data_size, write latency

### Option B: Sharded (Scalable)

Collections partitioned across nodes via consistent hashing.
- Writes: Route to owning node(s)
- Reads: Route to owning node
- Search: Scatter-gather across all shards

**Pros**: Scales storage, write throughput
**Cons**: More complex, network hops for reads

### Recommendation

**Start with Option A (Full Replication)** for simplicity.
- Your POPs are fixed (4 nodes)
- Data fits on each node (256GB RAM available)
- Simpler to implement and reason about
- Can add sharding later if needed

## Configuration Reference

```erlang
{barrel_vectordb, [
    %% Mode: standalone (default) | mesh
    {mode, mesh},

    %% Cluster name (must match across nodes)
    {cluster_name, barrel_vectors},

    %% Node list (static discovery)
    {nodes, [
        'barrel@paris.enki.io',
        'barrel@lille.enki.io',
        'barrel@amsterdam.enki.io',
        'barrel@geneva.enki.io'
    ]},

    %% Discovery configuration
    {discovery, #{
        mode => static,  % static | dns | seed

        %% DNS mode options
        %% domain => <<"_barrel._tcp.enki.io">>,
        %% type => srv,  % srv | a

        %% Seed mode options
        %% seeds => [<<"http://paris.enki.io:8080">>]
    }},

    %% aten configuration
    {health, #{
        heartbeat_interval => 1000,  % ms
        phi_threshold => 8           % 1-16, lower = faster detection
    }},

    %% Ra configuration
    {ra, #{
        data_dir => "data/ra",
        wal_max_size_bytes => 134217728  % 128MB
    }},

    %% HTTP API (optional)
    {http_enabled, false},
    {http_port, 8080},

    %% Storage (same as standalone)
    {path, "data/vectors"},
    {dimensions, 768}
]}
```

## API Compatibility

**100% backward compatible** - existing code continues to work.

```erlang
%% These calls work in both standalone and mesh mode
barrel_vectordb:add(Store, Id, Text, Metadata).
barrel_vectordb:search(Store, Query, Opts).
barrel_vectordb:get(Store, Id).
barrel_vectordb:delete(Store, Id).

%% New cluster-specific calls (mesh mode only)
barrel_vectordb:cluster_status().
barrel_vectordb:cluster_nodes().
barrel_vectordb:is_clustered().
```

## Testing Strategy

1. **Unit tests**: Each new module tested in isolation
2. **Integration tests**: Multi-node cluster in CT
3. **Chaos tests**: Node failures, network partitions
4. **Benchmark**: Compare standalone vs mesh performance

## Files to Create

| File | Description |
|------|-------------|
| `src/cluster/barrel_vectordb_mesh.erl` | Cluster coordinator |
| `src/cluster/barrel_vectordb_mesh_sup.erl` | Cluster supervision |
| `src/cluster/barrel_vectordb_ra.erl` | Ra server wrapper |
| `src/cluster/barrel_vectordb_ra_sm.erl` | Ra state machine |
| `src/cluster/barrel_vectordb_router.erl` | Request routing |
| `src/cluster/barrel_vectordb_discovery.erl` | Node discovery |
| `src/cluster/barrel_vectordb_health.erl` | aten health checks |

## Files to Modify

| File | Changes |
|------|---------|
| `rebar.config` | Add ra dependency |
| `src/barrel_vectordb.app.src` | Add ra to applications |
| `src/barrel_vectordb_app.erl` | Mode-aware startup |
| `src/barrel_vectordb_sup.erl` | Conditional supervision |
| `src/barrel_vectordb.erl` | Add routing logic |

## Timeline Estimate

| Phase | Effort |
|-------|--------|
| Phase 1: Core Infrastructure | ~3-4 days |
| Phase 2: Consensus & Routing | ~3-4 days |
| Phase 3: Discovery & Ops | ~2-3 days |
| Phase 4: HTTP API | ~1-2 days |
| Phase 5: Migration | ~1-2 days |

## Questions to Resolve

1. **Replication vs Sharding**: Start with full replication?
2. **HTTP API**: Include in barrel_vectordb or keep separate?
3. **Collection distribution**: Same collection on all nodes or configurable?

## Next Steps

1. Review and approve this plan
2. Create feature branch: `feature/cluster-mode`
3. Implement Phase 1
4. Iterate based on testing
