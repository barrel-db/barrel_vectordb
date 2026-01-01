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

%% Search with optimized options (skip text/metadata for faster results)
{ok, Results} = barrel_vectordb:search_vector(Store, Vector, #{
    k => 50,
    include_text => false,      %% Skip text lookup
    include_metadata => false   %% Skip metadata lookup
}).

%% Search with custom ef_search (higher = better recall, slower)
{ok, Results} = barrel_vectordb:search_vector(Store, Vector, #{
    k => 10,
    ef_search => 200   %% Default is max(k, 50)
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

%% Checkpoint HNSW index (speeds up restart)
ok = barrel_vectordb:checkpoint(Store).
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
    },
    batch => #{                    %% Write batching options
        min_batch_size => 4,       %% Min requests before batching
        max_batch_size => 256      %% Max batch size
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
    python => "python3",                %% Python executable (default)
    model => "BAAI/bge-base-en-v1.5",   %% Model name (default, 768 dims)
    timeout => 120000                   %% Timeout in ms (default)
}}
```

**Setup with virtual environment (recommended):**

```bash
# Create virtual environment
python3 -m venv ~/.venv/barrel_embed
source ~/.venv/barrel_embed/bin/activate

# Install dependencies
pip install sentence-transformers

# Verify installation
python -c "from sentence_transformers import SentenceTransformer; print('OK')"
```

Then start the store with the virtual environment's Python path:

```erlang
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    embedder => {local, #{
        python => "/home/user/.venv/barrel_embed/bin/python"
    }}
}).
```

Or activate the venv before starting your Erlang application:

```bash
source ~/.venv/barrel_embed/bin/activate
rebar3 shell
```

```erlang
%% Now python3 will use the venv automatically
{ok, _} = barrel_vectordb:start_link(#{
    name => my_store,
    path => "/tmp/vectors",
    embedder => {local, #{}}  %% uses default python3
}).
```

**Supported Models:**

Any sentence-transformers or HuggingFace model works. Popular choices:

| Model | Dimensions | Notes |
|-------|------------|-------|
| `BAAI/bge-base-en-v1.5` | 768 | Default, good quality/speed |
| `BAAI/bge-small-en-v1.5` | 384 | Faster, smaller |
| `BAAI/bge-large-en-v1.5` | 1024 | Best quality, slower |
| `sentence-transformers/all-MiniLM-L6-v2` | 384 | Fast, general purpose |
| `sentence-transformers/all-mpnet-base-v2` | 768 | High quality |
| `nomic-ai/nomic-embed-text-v1.5` | 768 | Long context (8192 tokens) |

The dimension is auto-detected from the model.

### Ollama

Local Ollama server. Requires [Ollama](https://ollama.ai) to be running.

```erlang
embedder => {ollama, #{
    url => <<"http://localhost:11434">>,   %% Ollama API URL (default)
    model => <<"nomic-embed-text">>,       %% Model name (default, 768 dims)
    timeout => 30000                       %% Timeout in ms (default)
}}
```

```bash
# Pull embedding models:
ollama pull nomic-embed-text
```

**Supported Models:**

| Model | Dimensions | Notes |
|-------|------------|-------|
| `nomic-embed-text` | 768 | Default, general purpose |
| `mxbai-embed-large` | 1024 | High quality |
| `all-minilm` | 384 | Fast |
| `snowflake-arctic-embed` | 1024 | Multilingual |

### OpenAI

OpenAI Embeddings API. Requires an API key.

```erlang
embedder => {openai, #{
    api_key => <<"sk-...">>,               %% API key (or set OPENAI_API_KEY env var)
    model => <<"text-embedding-3-small">>, %% Model name (default, 1536 dims)
    timeout => 30000                       %% Timeout in ms (default)
}}
```

```bash
# Set API key as environment variable (alternative to config)
export OPENAI_API_KEY=sk-...
```

**Supported Models:**

| Model | Dimensions | Notes |
|-------|------------|-------|
| `text-embedding-3-small` | 1536 | Default, fast and cheap |
| `text-embedding-3-large` | 3072 | Higher quality |
| `text-embedding-ada-002` | 1536 | Legacy model |

### Provider Chain

Try providers in order until one succeeds.

```erlang
embedder => [
    {openai, #{api_key => <<"sk-...">>}},  %% Try OpenAI first
    {ollama, #{url => <<"http://localhost:11434">>}},
    {local, #{}}  %% Fallback to CPU
]
```

## Search Options

| Option | Default | Description |
|--------|---------|-------------|
| `k` | 5 | Number of results to return |
| `filter` | - | Function `fun(Metadata) -> boolean()` to filter results |
| `include_text` | true | Include text in results |
| `include_metadata` | true | Include metadata in results |
| `ef_search` | max(k, 50) | Search width (higher = better recall, slower) |

## HNSW Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `m` | 16 | Max connections per node |
| `ef_construction` | 200 | Build-time search width |
| `ef_search` | 50 | Default query-time search width |
| `distance_fn` | cosine | `cosine` or `euclidean` |

## Testing

### Unit Tests

Unit tests use mocking and don't require external dependencies:

```bash
rebar3 eunit
```

### Integration Tests

Integration tests verify real embedding providers. They require backends to be available:

```bash
# Setup for local provider
pip install sentence-transformers

# Setup for Ollama provider
ollama serve &
ollama pull nomic-embed-text

# Setup for OpenAI provider
export OPENAI_API_KEY=sk-...

# Run integration tests
rebar3 eunit --module=barrel_vectordb_integration_tests
```

Tests automatically skip if their required backend is unavailable.

See `test/integration/README.md` for details.

## Performance

### Search Latency

| Metric | Typical Value |
|--------|---------------|
| P50 | ~1ms |
| P99 | ~5ms |

### Optimizations

- **Batch writes**: Concurrent writes are automatically batched via gen_batch_server
- **Batch lookups**: Search uses `rocksdb:multi_get` for efficient result fetching
- **Skip options**: Use `include_text => false` to skip unnecessary RocksDB reads
- **HNSW optimization**: O(log N) candidate management with balanced trees

### Benchmarking

Run the benchmark suite:

```bash
rebar3 as bench compile && rebar3 as bench eunit --module=barrel_vectordb_bench
```

## Architecture

- **Storage**: RocksDB with column families
- **Index**: HNSW for approximate nearest neighbor search
- **Vectors**: 8-bit quantization with norm caching
- **Embeddings**: Pluggable providers with fallback
- **Batching**: gen_batch_server for automatic write coalescing

See the API documentation for detailed architecture information.

## License

Apache-2.0
