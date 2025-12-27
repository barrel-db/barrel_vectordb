# barrel_vectordb

Erlang vector database with built-in embeddings. Add semantic search to your application.

## Quick Start

```erlang
%% Start a store
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors"
}).

%% Add documents (auto-embeds the text)
ok = barrel_vectordb:add(my_store, <<"doc1">>, <<"Hello world">>, #{source => greeting}).
ok = barrel_vectordb:add(my_store, <<"doc2">>, <<"Goodbye world">>, #{source => farewell}).

%% Search for similar documents
{ok, Results} = barrel_vectordb:search(my_store, <<"hi there">>, #{k => 5}).
%% => [#{key => <<"doc1">>, text => <<"Hello world">>, score => 0.89, ...}, ...]
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
%% Add with auto-embedding
ok = barrel_vectordb:add(Store, Id, Text, Metadata).

%% Add with explicit vector
ok = barrel_vectordb:add_vector(Store, Id, Text, Metadata, Vector).

%% Add batch
{ok, #{inserted := N}} = barrel_vectordb:add_batch(Store, [
    {<<"id1">>, <<"text 1">>, #{type => a}},
    {<<"id2">>, <<"text 2">>, #{type => b}}
]).
```

### Search

```erlang
%% Search with text query (auto-embeds)
{ok, Results} = barrel_vectordb:search(Store, <<"query text">>, #{k => 10}).

%% Search with vector
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

%% Update document (re-embeds text)
ok = barrel_vectordb:update(Store, <<"doc1">>, <<"New text">>, #{updated => true}).

%% Upsert (insert or update)
ok = barrel_vectordb:upsert(Store, <<"doc1">>, <<"Text">>, #{}).

%% Delete
ok = barrel_vectordb:delete(Store, <<"doc1">>).

%% Peek (sample without search)
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

    %% Embedding provider
    embedder => {local, #{
        python => "python3",
        model => "BAAI/bge-base-en-v1.5"
    }},

    %% HNSW index parameters
    hnsw => #{
        m => 16,                   %% Max connections per node
        ef_construction => 200     %% Build-time search width
    }
}).
```

## Embedding Providers

### Local (default)

CPU-based embeddings using sentence-transformers. No GPU required.

```erlang
embedder => {local, #{
    python => "python3",
    model => "BAAI/bge-base-en-v1.5"  %% 768 dimensions
}}
```

### Ollama

Local LLM server with embedding support.

```erlang
embedder => {ollama, #{
    url => <<"http://localhost:11434">>,
    model => <<"nomic-embed-text">>
}}
```

### Provider Chain (fallback)

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
| `m` | 16 | Max connections per node. Higher = better recall, more memory |
| `ef_construction` | 200 | Build-time search width. Higher = better index quality |
| `ef_search` | 50 | Query-time search width. Higher = better recall, slower |
| `distance_fn` | cosine | Distance function: `cosine` or `euclidean` |

## Architecture

- **Storage**: RocksDB with column families for vectors, metadata, text, and HNSW graph
- **Index**: HNSW (Hierarchical Navigable Small World) for approximate nearest neighbor search
- **Vectors**: 8-bit quantization with norm caching for memory efficiency
- **Embeddings**: Pluggable providers with automatic fallback

## License

Apache-2.0
