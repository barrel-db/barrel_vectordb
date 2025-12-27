# barrel_vectordb

Erlang vector database for semantic search.

## Quick Start

### With Embeddings

```erlang
%% Start a store with local Python embeddings
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    embedder => {local, #{}}  %% requires Python + sentence-transformers
}).

%% Add documents (text is embedded automatically)
ok = barrel_vectordb:add(my_store, <<"doc1">>, <<"Hello world">>, #{}).
ok = barrel_vectordb:add(my_store, <<"doc2">>, <<"Goodbye world">>, #{}).

%% Search with text query
{ok, Results} = barrel_vectordb:search(my_store, <<"hi there">>, #{k => 5}).
%% => [#{key => <<"doc1">>, text => <<"Hello world">>, score => 0.89, ...}, ...]
```

### Vector-Only (no embedder)

```erlang
%% Start a store without embedder
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    dimensions => 768
}).

%% Add with pre-computed vectors
ok = barrel_vectordb:add_vector(my_store, <<"doc1">>, <<"Hello">>, #{}, Vector).

%% Search with vector query
{ok, Results} = barrel_vectordb:search_vector(my_store, QueryVector, #{k => 5}).
```

## Installation

Add to your `rebar.config`:

```erlang
{deps, [
    {barrel_vectordb, {git, "https://github.com/barrel-db/barrel_vectordb.git", {branch, "main"}}}
]}.
```

## Core API

### Add Documents

```erlang
%% Add with text (requires embedder)
ok = barrel_vectordb:add(Store, Id, Text, Metadata).

%% Add with explicit vector (no embedder required)
ok = barrel_vectordb:add_vector(Store, Id, Text, Metadata, Vector).

%% Add batch (requires embedder)
{ok, #{inserted := N}} = barrel_vectordb:add_batch(Store, [
    {<<"id1">>, <<"text 1">>, #{type => a}},
    {<<"id2">>, <<"text 2">>, #{type => b}}
]).
```

### Search

```erlang
%% Search with text query (requires embedder)
{ok, Results} = barrel_vectordb:search(Store, <<"query text">>, #{k => 10}).

%% Search with vector (no embedder required)
{ok, Results} = barrel_vectordb:search_vector(Store, Vector, #{k => 10}).

%% Search with metadata filter
{ok, Results} = barrel_vectordb:search(Store, <<"query">>, #{
    k => 10,
    filter => fun(Meta) -> maps:get(type, Meta) =:= important end
}).
```

### Document Operations

```erlang
%% Get document by ID
{ok, Doc} = barrel_vectordb:get(Store, <<"doc1">>).

%% Update document (requires embedder)
ok = barrel_vectordb:update(Store, <<"doc1">>, <<"New text">>, #{}).

%% Upsert (requires embedder)
ok = barrel_vectordb:upsert(Store, <<"doc1">>, <<"Text">>, #{}).

%% Delete
ok = barrel_vectordb:delete(Store, <<"doc1">>).

%% Peek (sample documents)
{ok, Docs} = barrel_vectordb:peek(Store, 10).

%% Count
N = barrel_vectordb:count(Store).
```

## Configuration

```erlang
barrel_vectordb:start_link(#{
    name => my_store,              %% Store name (required)
    path => "/var/data/vectors",   %% RocksDB path
    dimensions => 768,             %% Vector dimensions (default: 768)
    embedder => EmbedderConfig,    %% Embedding provider (optional)
    hnsw => #{                     %% HNSW index parameters
        m => 16,
        ef_construction => 200
    }
}).
```

## Embedding Providers

Embedder is **explicit** - if not configured, only `add_vector/5` and `search_vector/3` work.
Text-based operations return `{error, embedder_not_configured}`.

### Local

Local Python with sentence-transformers. CPU-based, no external API calls.

```erlang
embedder => {local, #{
    python => "python3",                %% Python executable
    model => "BAAI/bge-base-en-v1.5"    %% Model name (768 dimensions)
}}
```

Requires:
```bash
pip install sentence-transformers
```

### Ollama

Local Ollama server.

```erlang
embedder => {ollama, #{
    url => <<"http://localhost:11434">>,
    model => <<"nomic-embed-text">>
}}
```

### Provider Chain

Try providers in order until one succeeds.

```erlang
embedder => [
    {ollama, #{url => <<"http://localhost:11434">>}},
    {local, #{}}  %% Fallback to CPU
]
```

## HNSW Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `m` | 16 | Max connections per node |
| `ef_construction` | 200 | Build-time search width |
| `ef_search` | 50 | Query-time search width |
| `distance_fn` | cosine | `cosine` or `euclidean` |

## Architecture

- **Storage**: RocksDB with column families
- **Index**: HNSW for approximate nearest neighbor search
- **Vectors**: 8-bit quantization with norm caching
- **Embeddings**: Pluggable providers with fallback

## License

Apache-2.0
