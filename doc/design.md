# barrel_vectordb Architecture & Design

This document describes the internal architecture and design decisions of barrel_vectordb.

## Overview

barrel_vectordb is an Erlang vector database optimized for semantic search workloads. It combines:

- **RocksDB** for persistent storage
- **HNSW** (Hierarchical Navigable Small World) for approximate nearest neighbor search
- **gen_batch_server** for automatic write batching
- **Pluggable embedders** for text-to-vector conversion

## Module Structure

```
src/
├── barrel_vectordb.erl           # Public API (facade)
├── barrel_vectordb_server.erl    # Per-store gen_batch_server
├── barrel_vectordb_hnsw.erl      # HNSW index implementation
├── barrel_vectordb_embed.erl     # Embedding coordination
├── barrel_vectordb_embed_provider.erl  # Provider implementations
├── barrel_vectordb_store.erl     # Store registry
├── barrel_vectordb_sup.erl       # Top-level supervisor
└── barrel_vectordb_app.erl       # Application callbacks
```

## Storage Layer

### RocksDB Column Families

Each store uses four column families for separation of concerns:

| Column Family | Key | Value | Purpose |
|---------------|-----|-------|---------|
| `cf_vectors` | Document ID | Quantized vector + norm | Vector storage |
| `cf_metadata` | Document ID | Erlang term (binary) | Document metadata |
| `cf_text` | Document ID | Raw text | Original text content |
| `cf_hnsw` | Node ID | HNSW node data | Index persistence |

Benefits:
- Independent compaction per data type
- Efficient range scans within each family
- Better cache locality

### Vector Quantization

Vectors are stored with 8-bit quantization to reduce storage:

```erlang
%% Store: [Norm::float(), Quantized::binary()]
%% Quantized: each dimension scaled to 0-255 range
encode_vector(Vector) ->
    Norm = vector_norm(Vector),
    Normalized = [V / Norm || V <- Vector],
    Quantized = << <<(round((V + 1.0) * 127.5)):8>> || V <- Normalized >>,
    term_to_binary({Norm, Quantized}).
```

This reduces storage ~4x (32-bit float -> 8-bit int) with minimal accuracy loss for similarity search.

## HNSW Index

### Algorithm

HNSW builds a multi-layer graph where:
- Layer 0 contains all vectors
- Higher layers contain progressively fewer vectors
- Search starts at top layer, descends to bottom

Key parameters:

| Parameter | Default | Effect |
|-----------|---------|--------|
| `m` | 16 | Max edges per node (higher = more accurate, more memory) |
| `ef_construction` | 200 | Search width during build (higher = better index quality) |
| `ef_search` | 50 | Search width during query (higher = more accurate, slower) |

### Performance Optimizations

#### 1. gb_trees for Candidate Management

HNSW search maintains a candidate set that must be sorted by distance. Originally used `lists:sort/1` which is O(N log N) per iteration.

Optimized to use `gb_trees` (balanced binary tree):
- Insert: O(log N)
- Remove largest: O(log N)
- Get largest: O(log N)

```erlang
%% Before: O(N log N) per iteration
Candidates = lists:sort(fun({D1,_}, {D2,_}) -> D1 < D2 end, NewCandidates)

%% After: O(log N) per operation
{_, _, Candidates} = gb_trees:take_largest(CandidateTree)
```

#### 2. Pre-computed Norms

Vector norms are computed once at insert time and cached. Cosine similarity then only requires:
- Dot product (O(D))
- Division by cached norms (O(1))

## Write Path

### gen_batch_server Integration

Write operations go through gen_batch_server which automatically batches concurrent requests:

```
Client A: add_vector(...) ─┐
Client B: add_vector(...) ─┼──> gen_batch_server ──> handle_batch([A, B, C])
Client C: add_vector(...) ─┘                              │
                                                          ▼
                                                   Single RocksDB
                                                   WriteBatch
```

Configuration:
- `min_batch_size`: 4 (wait for at least 4 concurrent requests)
- `max_batch_size`: 256 (cap batch size)

Benefits:
- Single fsync for multiple writes
- Reduced write amplification
- Better throughput under concurrent load

### Write Batching

All writes within a batch use a single RocksDB WriteBatch:

```erlang
handle_batch(Requests, State) ->
    {ok, Batch} = rocksdb:batch(),
    Results = lists:map(fun(Req) ->
        process_write(Req, Batch, State)
    end, Requests),
    ok = rocksdb:write_batch(Db, Batch, [{sync, true}]),
    {Results, State}.
```

## Search Path

### Query Flow

```
search_vector(Query, Opts)
        │
        ▼
┌─────────────────────┐
│ HNSW Search         │  In-memory graph traversal
│ (barrel_vectordb_   │  Returns: [{Id, Distance}, ...]
│  hnsw:search)       │
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│ Batch Lookup        │  rocksdb:multi_get for metadata + text
│ (multi_get)         │  Skippable with include_* options
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│ Filter & Format     │  Apply metadata filter, format results
└─────────────────────┘
```

### Search Optimizations

#### 1. Batch RocksDB Lookups

Originally, search made 2 sequential `rocksdb:get` calls per result (metadata + text). For k=50, that's 100 sequential calls.

Optimized to use `rocksdb:multi_get`:

```erlang
%% Before: 100 sequential calls
[{ok, Meta} = rocksdb:get(Db, CfM, Id, []) || Id <- Ids],
[{ok, Text} = rocksdb:get(Db, CfT, Id, []) || Id <- Ids]

%% After: 2 batch calls
MetaResults = rocksdb:multi_get(Db, CfM, Ids, []),
TextResults = rocksdb:multi_get(Db, CfT, Ids, [])
```

#### 2. Skip Unnecessary Lookups

Search options allow skipping data that isn't needed:

```erlang
%% Only get distances and IDs (no RocksDB reads)
{ok, Results} = barrel_vectordb:search_vector(Store, Query, #{
    k => 50,
    include_text => false,
    include_metadata => false
}).
```

Useful for:
- Pagination (just need IDs)
- Re-ranking pipelines
- Distance-only queries

## Embedding Layer

### Provider Architecture

```
barrel_vectordb_embed.erl (coordinator)
        │
        ├── {local, Config}  ──> Python subprocess + sentence-transformers
        │
        ├── {ollama, Config} ──> HTTP to Ollama server
        │
        ├── {openai, Config} ──> HTTP to OpenAI API
        │
        └── [Provider1, Provider2, ...] ──> Chain with fallback
```

### Provider Chain

When configured with a list of providers, they're tried in order:

```erlang
embedder => [
    {ollama, #{url => <<"http://gpu-server:11434">>}},
    {openai, #{api_key => Key}},
    {local, #{}}  % CPU fallback
]
```

This enables:
- GPU acceleration when available
- Cloud fallback for capacity
- Local fallback for reliability

## Data Flow Diagrams

### Insert Flow

```
add_vector(Id, Text, Meta, Vector)
        │
        ▼
gen_batch_server:call
        │
        ▼
handle_batch (may batch with other writes)
        │
        ├──> Quantize vector
        │
        ├──> WriteBatch:
        │    ├── put(cf_vectors, Id, QuantizedVector)
        │    ├── put(cf_metadata, Id, term_to_binary(Meta))
        │    └── put(cf_text, Id, Text)
        │
        ├──> HNSW insert (update in-memory index)
        │
        └──> rocksdb:write_batch (atomic commit)
```

### Search Flow

```
search_vector(QueryVector, #{k => 10})
        │
        ▼
HNSW search (in-memory)
        │
        ▼
Top-K results: [{id1, 0.05}, {id2, 0.08}, ...]
        │
        ▼
multi_get metadata (batch)
        │
        ▼
multi_get text (batch, if include_text)
        │
        ▼
Apply filter function
        │
        ▼
Format and return results
```

## Performance Characteristics

### Complexity

| Operation | Time Complexity | Notes |
|-----------|-----------------|-------|
| Insert | O(M * log N) | M = HNSW m parameter |
| Search | O(log N * ef) | ef = ef_search parameter |
| Get by ID | O(1) | Direct RocksDB lookup |
| Delete | O(M * log N) | Must update HNSW graph |

### Throughput (typical, single node)

| Operation | Throughput | Notes |
|-----------|------------|-------|
| Insert (batched) | 5,000-15,000/s | Depends on vector dimension |
| Search | 1,000-5,000/s | Depends on k and ef_search |
| Concurrent writes | Near-linear scaling | Up to CPU core count |

### Latency

| Metric | Value | Notes |
|--------|-------|-------|
| Search P50 | ~1ms | Warm cache |
| Search P99 | ~5ms | With include_text=true |
| Insert P50 | ~0.5ms | Batched |

## Configuration Tuning

### For High Throughput

```erlang
#{
    batch => #{
        min_batch_size => 16,
        max_batch_size => 1024
    },
    hnsw => #{
        m => 12,              % Lower m for faster inserts
        ef_construction => 100
    }
}
```

### For High Accuracy

```erlang
#{
    hnsw => #{
        m => 32,              % More connections
        ef_construction => 400,
        ef_search => 200
    }
}
```

### For Low Memory

```erlang
#{
    hnsw => #{
        m => 8,               % Fewer connections
        ef_construction => 100
    }
}
```

## Future Considerations

Potential improvements:

1. **Sharding**: Distribute vectors across multiple stores
2. **GPU acceleration**: CUDA-based distance computations
3. **Product quantization**: Further compress vectors for billion-scale
4. **Persistent HNSW**: Memory-map the index for faster startup
5. **Streaming inserts**: Background index building
