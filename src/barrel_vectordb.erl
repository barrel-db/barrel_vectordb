%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb - Erlang Vector Database
%%%
%%% An Erlang library for storing and searching vectors. Supports optional
%%% embedding providers for text-to-vector conversion.
%%%
%%% == Quick Start (with embeddings) ==
%%% ```
%%% %% Start a store with local Python embeddings
%%% {ok, _} = barrel_vectordb:start_link(#{
%%%     name => my_store,
%%%     path => "/tmp/vectors",
%%%     embedder => {local, #{}}  %% requires Python + sentence-transformers
%%% }).
%%%
%%% %% Add a document (embeds the text automatically)
%%% ok = barrel_vectordb:add(my_store, <<"doc-1">>, <<"Hello world">>, #{}).
%%%
%%% %% Search with text query
%%% {ok, Results} = barrel_vectordb:search(my_store, <<"greetings">>, #{k => 5}).
%%% '''
%%%
%%% == Quick Start (vector-only, no embedder) ==
%%% ```
%%% %% Start a store without embedder
%%% {ok, _} = barrel_vectordb:start_link(#{
%%%     name => my_store,
%%%     path => "/tmp/vectors",
%%%     dimensions => 768
%%% }).
%%%
%%% %% Add with pre-computed vector
%%% ok = barrel_vectordb:add_vector(my_store, <<"doc-1">>, <<"Hello">>, #{}, Vector).
%%%
%%% %% Search with vector query
%%% {ok, Results} = barrel_vectordb:search_vector(my_store, QueryVector, #{k => 5}).
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% #{
%%%     name => atom(),              %% Store name (required)
%%%     path => string(),            %% RocksDB path
%%%     dimensions => pos_integer(), %% Vector dimensions (default: 768)
%%%     embedder => EmbedderConfig,  %% Embedding provider (optional)
%%%     hnsw => HnswConfig           %% HNSW index parameters
%%% }
%%% '''
%%%
%%% == Embedding Providers ==
%%%
%%% Embedder is **explicit** - if not configured, only `add_vector/5' and
%%% `search_vector/3' work. Text-based operations return `{error, embedder_not_configured}'.
%%%
%%% ```
%%% %% Local Python with sentence-transformers (CPU, no external calls)
%%% embedder => {local, #{
%%%     python => "python3",
%%%     model => "BAAI/bge-base-en-v1.5"
%%% }}
%%%
%%% %% Ollama (local LLM server)
%%% embedder => {ollama, #{
%%%     url => <<"http://localhost:11434">>,
%%%     model => <<"nomic-embed-text">>
%%% }}
%%%
%%% %% Provider chain with fallback
%%% embedder => [
%%%     {ollama, #{url => <<"http://localhost:11434">>}},
%%%     {local, #{}}  %% Fallback to CPU
%%% ]
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb).

%% API - Lifecycle
-export([
    start_link/1,
    stop/1
]).

%% API - Cluster
-export([
    start_cluster/0,
    start_cluster/1,
    cluster_join/1,
    cluster_leave/0,
    cluster_status/0,
    cluster_nodes/0,
    is_clustered/0
]).

%% API - Cluster Collections
-export([
    create_collection/2,
    delete_collection/1,
    get_collection/1,
    list_collections/0
]).

%% API - Cluster Document Operations (routed to shards)
-export([
    cluster_add/4,
    cluster_add/5,
    cluster_add_vector/5,
    cluster_get/2,
    cluster_delete/2,
    cluster_search/3,
    cluster_search_vector/3
]).

%% API - Document Operations
-export([
    add/4,
    add/5,
    add_vector/5,
    add_batch/2,
    add_vector_batch/2,
    get/2,
    update/4,
    upsert/4,
    delete/2,
    peek/2
]).

%% API - Search
-export([
    search/3,
    search_vector/3
]).

%% API - Embedding
-export([
    embed/2,
    embed_batch/2
]).

%% API - Info
-export([
    stats/1,
    count/1,
    embedder_info/1
]).

%% API - Maintenance
-export([
    checkpoint/1
]).

%% Types
-type store() :: atom() | pid().
%% A store reference - either a registered name or a pid.

-type id() :: binary().
%% Unique document identifier.

-type text() :: binary().
%% Document text content.

-type vector() :: [float()].
%% Embedding vector (list of floats).

-type metadata() :: #{atom() => term()}.
%% Arbitrary metadata map associated with a document.

-type search_opts() :: #{
    k => pos_integer(),
    filter => fun((metadata()) -> boolean()),
    include_text => boolean(),
    include_metadata => boolean(),
    ef_search => pos_integer()
}.
%% Options for search operations.
%% - `k': Number of results to return (default: 5)
%% - `filter': Function to filter results by metadata
%% - `include_text': Include text in results (default: true)
%% - `include_metadata': Include metadata in results (default: true)
%% - `ef_search': Search width, higher = better recall (default: max(k, 50))

-type search_result() :: #{
    key := id(),
    text := text(),
    metadata := metadata(),
    score := float(),
    vector => vector()
}.
%% A single search result.

-type store_config() :: #{
    name := atom(),
    path => string(),
    dimensions => pos_integer(),
    embedder => embedder_config(),
    hnsw => hnsw_config()
}.
%% Store configuration options.

-type embedder_config() ::
    {local, map()} |
    {ollama, map()} |
    {openai, map()} |
    {anthropic, map()} |
    [{atom(), map()}].
%% Embedding provider configuration.

-type hnsw_config() :: #{
    m => pos_integer(),
    ef_construction => pos_integer(),
    distance_fn => cosine | euclidean
}.
%% HNSW index parameters.

-export_type([
    store/0, id/0, text/0, vector/0, metadata/0,
    search_opts/0, search_result/0,
    store_config/0, embedder_config/0, hnsw_config/0
]).

%%====================================================================
%% Lifecycle API
%%====================================================================

%% @doc Start a new vector store.
%%
%% Creates a new vector store with the given configuration. The store
%% is registered under the name provided in the config.
%%
%% == Example ==
%% ```
%% {ok, Pid} = barrel_vectordb:start_link(#{
%%     name => my_store,
%%     path => "/var/lib/myapp/vectors",
%%     dimensions => 768,
%%     embedder => {local, #{}}
%% }).
%% '''
%%
%% @param Config Store configuration map
%% @returns `{ok, Pid}' on success, `{error, Reason}' on failure
-spec start_link(store_config()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    Name = maps:get(name, Config, barrel_vectordb_store),
    %% Normalize dimension/dimensions key
    Dimension = case {maps:get(dimension, Config, undefined), maps:get(dimensions, Config, undefined)} of
        {undefined, undefined} -> 768;
        {undefined, D} -> D;
        {D, _} -> D
    end,
    %% Normalize path/db_path key
    DbPath = case {maps:get(path, Config, undefined), maps:get(db_path, Config, undefined)} of
        {undefined, undefined} -> default_path(Name);
        {undefined, P} -> P;
        {P, _} -> P
    end,
    StoreConfig = maps:merge(#{
        db_path => DbPath,
        dimension => Dimension
    }, maps:without([name, dimensions, path], Config)),
    barrel_vectordb_server:start_link(Name, StoreConfig).

%% @doc Stop a vector store.
%%
%% Gracefully shuts down the store, persisting any pending data.
%%
%% @param Store Store name or pid
%% @returns `ok'
-spec stop(store()) -> ok.
stop(Store) ->
    barrel_vectordb_server:stop(Store).

%%====================================================================
%% Document Operations API
%%====================================================================

%% @doc Add a document with automatic embedding.
%%
%% Embeds the text using the configured provider and stores it in the
%% vector database along with its metadata.
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:add(my_store, <<"doc-1">>, <<"Hello world">>, #{
%%     type => greeting,
%%     language => english
%% }).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Unique document identifier
%% @param Text The text to embed and store
%% @param Metadata Arbitrary metadata map
%% @returns `ok' on success, `{error, Reason}' on failure
-spec add(store(), id(), text(), metadata()) -> ok | {error, term()}.
add(Store, Id, Text, Metadata) ->
    barrel_vectordb_server:add(Store, Id, Text, Metadata).

%% @doc Add a document with explicit vector.
%%
%% Stores the document with a pre-computed embedding vector instead of
%% generating one automatically.
%%
%% == Example ==
%% ```
%% Vector = [0.1, 0.2, ...],  %% 768 dimensions
%% ok = barrel_vectordb:add(my_store, <<"doc-1">>, <<"Hello">>, #{}, Vector).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Unique document identifier
%% @param Text The text content
%% @param Metadata Arbitrary metadata map
%% @param Vector Pre-computed embedding vector
%% @returns `ok' on success, `{error, Reason}' on failure
-spec add(store(), id(), text(), metadata(), vector()) -> ok | {error, term()}.
add(Store, Id, Text, Metadata, Vector) ->
    barrel_vectordb_server:add_vector(Store, Id, Text, Metadata, Vector).

%% @doc Add a document with pre-computed vector (alias).
%%
%% Same as `add/5', provided for API clarity.
%%
%% @see add/5
-spec add_vector(store(), id(), text(), metadata(), vector()) -> ok | {error, term()}.
add_vector(Store, Id, Text, Metadata, Vector) ->
    barrel_vectordb_server:add_vector(Store, Id, Text, Metadata, Vector).

%% @doc Add multiple documents in batch.
%%
%% Efficiently adds multiple documents with automatic embedding.
%% More efficient than calling `add/4' multiple times.
%%
%% == Example ==
%% ```
%% Docs = [
%%     {<<"id-1">>, <<"text 1">>, #{type => a}},
%%     {<<"id-2">>, <<"text 2">>, #{type => b}}
%% ],
%% {ok, #{inserted := 2}} = barrel_vectordb:add_batch(my_store, Docs).
%% '''
%%
%% @param Store Store name or pid
%% @param Docs List of `{Id, Text, Metadata}' tuples
%% @returns `{ok, Stats}' on success, `{error, Reason}' on failure
-spec add_batch(store(), [{id(), text(), metadata()}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_batch(Store, Docs) ->
    barrel_vectordb_server:add_batch(Store, Docs).

%% @doc Add multiple documents with pre-computed vectors in batch.
%%
%% Efficiently adds multiple documents with their vectors in a single
%% atomic RocksDB write. Much faster than calling `add_vector/5' multiple times.
%%
%% == Example ==
%% ```
%% Docs = [
%%     {<<"id-1">>, <<"text 1">>, #{type => a}, Vector1},
%%     {<<"id-2">>, <<"text 2">>, #{type => b}, Vector2}
%% ],
%% {ok, #{inserted := 2}} = barrel_vectordb:add_vector_batch(my_store, Docs).
%% '''
%%
%% @param Store Store name or pid
%% @param Docs List of `{Id, Text, Metadata, Vector}' tuples
%% @returns `{ok, Stats}' on success, `{error, Reason}' on failure
-spec add_vector_batch(store(), [{id(), text(), metadata(), vector()}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_vector_batch(Store, Docs) ->
    barrel_vectordb_server:add_vector_batch(Store, Docs).

%% @doc Get a document by ID.
%%
%% Retrieves a document with its vector, text, and metadata.
%%
%% == Example ==
%% ```
%% {ok, Doc} = barrel_vectordb:get(my_store, <<"doc-1">>),
%% Text = maps:get(text, Doc),
%% Meta = maps:get(metadata, Doc).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Document identifier
%% @returns `{ok, Document}' if found, `not_found', or `{error, Reason}'
-spec get(store(), id()) -> {ok, map()} | not_found | {error, term()}.
get(Store, Id) ->
    barrel_vectordb_server:get(Store, Id).

%% @doc Delete a document.
%%
%% Removes a document from the store, including its vector and metadata.
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:delete(my_store, <<"doc-1">>).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Document identifier
%% @returns `ok' on success, `{error, Reason}' on failure
-spec delete(store(), id()) -> ok | {error, term()}.
delete(Store, Id) ->
    barrel_vectordb_server:delete(Store, Id).

%% @doc Update a document.
%%
%% Updates an existing document by re-embedding the text and storing
%% the new text and metadata. Returns `not_found' if the document
%% does not exist.
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:update(my_store, <<"doc-1">>, <<"New text">>, #{updated => true}).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Document identifier
%% @param Text New text to embed and store
%% @param Metadata New metadata map
%% @returns `ok' on success, `not_found', or `{error, Reason}'
-spec update(store(), id(), text(), metadata()) -> ok | not_found | {error, term()}.
update(Store, Id, Text, Metadata) ->
    barrel_vectordb_server:update(Store, Id, Text, Metadata).

%% @doc Insert or update a document.
%%
%% If the document exists, updates it. If not, inserts it.
%% Always succeeds (unless there's an error).
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:upsert(my_store, <<"doc-1">>, <<"Text">>, #{}).
%% '''
%%
%% @param Store Store name or pid
%% @param Id Document identifier
%% @param Text Text to embed and store
%% @param Metadata Metadata map
%% @returns `ok' on success, `{error, Reason}' on failure
-spec upsert(store(), id(), text(), metadata()) -> ok | {error, term()}.
upsert(Store, Id, Text, Metadata) ->
    barrel_vectordb_server:upsert(Store, Id, Text, Metadata).

%% @doc Peek at documents.
%%
%% Returns a sample of documents without performing a search.
%% Useful for inspecting the store contents.
%%
%% == Example ==
%% ```
%% {ok, Docs} = barrel_vectordb:peek(my_store, 10),
%% [#{key := K, text := T, metadata := M} | _] = Docs.
%% '''
%%
%% @param Store Store name or pid
%% @param Limit Maximum number of documents to return
%% @returns `{ok, Docs}' list of documents
-spec peek(store(), pos_integer()) -> {ok, [map()]}.
peek(Store, Limit) ->
    barrel_vectordb_server:peek(Store, Limit).

%%====================================================================
%% Search API
%%====================================================================

%% @doc Search for similar documents using text query.
%%
%% Embeds the query text and finds the most similar documents using
%% approximate nearest neighbor search (HNSW).
%%
%% == Example ==
%% ```
%% {ok, Results} = barrel_vectordb:search(my_store, <<"hello">>, #{
%%     k => 5,
%%     filter => fun(Meta) -> maps:get(type, Meta, undefined) =:= greeting end
%% }),
%% [#{key := Key, text := Text, score := Score} | _] = Results.
%% '''
%%
%% @param Store Store name or pid
%% @param Query Text to search for
%% @param Opts Search options
%% @returns `{ok, Results}' list of matching documents sorted by similarity
-spec search(store(), text(), search_opts()) ->
    {ok, [search_result()]} | {error, term()}.
search(Store, Query, Opts) when is_binary(Query) ->
    barrel_vectordb_server:search(Store, Query, Opts).

%% @doc Search for similar documents using a vector query.
%%
%% Finds the most similar documents to the given vector without
%% needing to embed a text query first.
%%
%% == Example ==
%% ```
%% QueryVector = [0.1, 0.2, ...],  %% 768 dimensions
%% {ok, Results} = barrel_vectordb:search_vector(my_store, QueryVector, #{k => 5}).
%% '''
%%
%% @param Store Store name or pid
%% @param Vector Query vector
%% @param Opts Search options
%% @returns `{ok, Results}' list of matching documents sorted by similarity
-spec search_vector(store(), vector(), search_opts()) ->
    {ok, [search_result()]} | {error, term()}.
search_vector(Store, Vector, Opts) when is_list(Vector) ->
    barrel_vectordb_server:search_vector(Store, Vector, Opts).

%%====================================================================
%% Embedding API
%%====================================================================

%% @doc Generate embedding for a single text.
%%
%% Uses the store's configured embedding provider to generate a vector.
%% Useful when you need the vector for other purposes.
%%
%% == Example ==
%% ```
%% {ok, Vector} = barrel_vectordb:embed(my_store, <<"hello world">>),
%% 768 = length(Vector).
%% '''
%%
%% @param Store Store name or pid
%% @param Text Text to embed
%% @returns `{ok, Vector}' or `{error, Reason}'
-spec embed(store(), text()) -> {ok, vector()} | {error, term()}.
embed(Store, Text) ->
    barrel_vectordb_server:embed(Store, Text).

%% @doc Generate embeddings for multiple texts.
%%
%% More efficient than calling `embed/2' multiple times as it batches
%% the requests to the embedding provider.
%%
%% == Example ==
%% ```
%% {ok, Vectors} = barrel_vectordb:embed_batch(my_store, [<<"text 1">>, <<"text 2">>]),
%% 2 = length(Vectors).
%% '''
%%
%% @param Store Store name or pid
%% @param Texts List of texts to embed
%% @returns `{ok, Vectors}' or `{error, Reason}'
-spec embed_batch(store(), [text()]) -> {ok, [vector()]} | {error, term()}.
embed_batch(Store, Texts) ->
    barrel_vectordb_server:embed_batch(Store, Texts).

%%====================================================================
%% Info API
%%====================================================================

%% @doc Get store statistics.
%%
%% Returns information about the store including document count,
%% dimensions, and HNSW index statistics.
%%
%% == Example ==
%% ```
%% {ok, Stats} = barrel_vectordb:stats(my_store),
%% Count = maps:get(count, Stats),
%% Dims = maps:get(dimension, Stats).
%% '''
%%
%% @param Store Store name or pid
%% @returns `{ok, Stats}' map with store statistics
-spec stats(store()) -> {ok, map()}.
stats(Store) ->
    barrel_vectordb_server:stats(Store).

%% @doc Get document count.
%%
%% Returns the number of documents in the store.
%%
%% @param Store Store name or pid
%% @returns Document count
-spec count(store()) -> non_neg_integer().
count(Store) ->
    barrel_vectordb_server:count(Store).

%% @doc Get embedding provider information.
%%
%% Returns information about the configured embedding providers.
%%
%% == Example ==
%% ```
%% {ok, Info} = barrel_vectordb:embedder_info(my_store),
%% Dimension = maps:get(dimension, Info),
%% Providers = maps:get(providers, Info).
%% '''
%%
%% @param Store Store name or pid
%% @returns `{ok, Info}' map with embedder information
-spec embedder_info(store()) -> {ok, map()}.
embedder_info(Store) ->
    barrel_vectordb_server:embedder_info(Store).

%% @doc Checkpoint the HNSW index to disk.
%%
%% Persists the current in-memory HNSW index metadata to RocksDB.
%% This can speed up restart by avoiding a full index rebuild.
%%
%% Note: The index is automatically rebuilt from vectors on startup
%% if no checkpoint exists, so this is optional but improves restart time.
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:checkpoint(my_store).
%% '''
%%
%% @param Store Store name or pid
%% @returns `ok' on success
-spec checkpoint(store()) -> ok.
checkpoint(Store) ->
    barrel_vectordb_server:checkpoint(Store).

%%====================================================================
%% Cluster API
%%====================================================================

%% @doc Start a new cluster (this node becomes leader).
%%
%% Initializes clustering on this node. Other nodes can then join
%% using `cluster_join/1'.
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:start_cluster().
%% '''
%%
%% @returns `{ok, ServerId}' on success, `{error, Reason}' on failure
-spec start_cluster() -> {ok, term()} | {error, term()}.
start_cluster() ->
    barrel_vectordb_mesh:start_cluster().

%% @doc Start a new cluster with configuration.
%%
%% @param Config Cluster configuration options
%% @returns `{ok, ServerId}' on success, `{error, Reason}' on failure
-spec start_cluster(map()) -> {ok, term()} | {error, term()}.
start_cluster(Config) ->
    barrel_vectordb_mesh:start_cluster(Config).

%% @doc Join an existing cluster via seed node(s).
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:cluster_join('barrel@paris.enki.io').
%% ok = barrel_vectordb:cluster_join(['barrel@paris.enki.io', 'barrel@lille.enki.io']).
%% '''
%%
%% @param SeedNodes Seed node atom or list of seed node atoms
%% @returns `ok' on success, `{error, Reason}' on failure
-spec cluster_join(atom() | [atom()]) -> ok | {error, term()}.
cluster_join(SeedNodes) ->
    barrel_vectordb_mesh:cluster_join(SeedNodes).

%% @doc Leave the cluster gracefully.
%%
%% Removes this node from the cluster. Shards will be rebalanced
%% to remaining nodes.
%%
%% @returns `ok' on success, `{error, Reason}' on failure
-spec cluster_leave() -> ok | {error, term()}.
cluster_leave() ->
    barrel_vectordb_mesh:cluster_leave().

%% @doc Get cluster status.
%%
%% Returns information about the cluster including node list,
%% leader, and this node's status.
%%
%% == Example ==
%% ```
%% Status = barrel_vectordb:cluster_status(),
%% #{state := State, nodes := Nodes, leader := Leader} = Status.
%% '''
%%
%% @returns Cluster status map
-spec cluster_status() -> map().
cluster_status() ->
    barrel_vectordb_mesh:cluster_status().

%% @doc Get list of healthy cluster nodes.
%%
%% Returns nodes that are currently reachable (via aten).
%%
%% @returns List of healthy nodes
-spec cluster_nodes() -> [node()].
cluster_nodes() ->
    barrel_vectordb_mesh:healthy_nodes().

%% @doc Check if this node is part of a cluster.
%%
%% @returns `true' if clustered, `false' if standalone
-spec is_clustered() -> boolean().
is_clustered() ->
    barrel_vectordb_mesh:is_clustered().

%%====================================================================
%% Cluster Collection API
%%====================================================================

%% @doc Create a collection in the cluster.
%%
%% Creates a sharded collection distributed across cluster nodes.
%% Only available when clustered.
%%
%% == Example ==
%% ```
%% ok = barrel_vectordb:create_collection(<<"memories">>, #{
%%     dimensions => 768,
%%     num_shards => 4,
%%     replication_factor => 2
%% }).
%% '''
%%
%% @param Name Collection name (binary)
%% @param Opts Collection options (dimensions, num_shards, replication_factor)
%% @returns `{ok, Meta}' on success, `{error, Reason}' on failure
-spec create_collection(binary(), map()) -> {ok, term()} | {error, term()}.
create_collection(Name, Opts) when is_binary(Name) ->
    case is_clustered() of
        true ->
            barrel_vectordb_cluster_client:create_collection(Name, Opts);
        false ->
            {error, not_clustered}
    end.

%% @doc Delete a collection from the cluster.
%%
%% @param Name Collection name
%% @returns `ok' on success, `{error, Reason}' on failure
-spec delete_collection(binary()) -> ok | {error, term()}.
delete_collection(Name) when is_binary(Name) ->
    case is_clustered() of
        true ->
            barrel_vectordb_cluster_client:delete_collection(Name);
        false ->
            {error, not_clustered}
    end.

%% @doc Get collection metadata.
%%
%% @param Name Collection name
%% @returns `{ok, Metadata}' or `{error, Reason}'
-spec get_collection(binary()) -> {ok, map()} | {error, term()}.
get_collection(Name) when is_binary(Name) ->
    case is_clustered() of
        true ->
            case barrel_vectordb_cluster_client:get_collections() of
                {ok, Collections} ->
                    case maps:get(Name, Collections, undefined) of
                        undefined -> {error, not_found};
                        Meta -> {ok, Meta}
                    end;
                {error, _} = Error ->
                    Error
            end;
        false ->
            {error, not_clustered}
    end.

%% @doc List all collections in the cluster.
%%
%% @returns `{ok, CollectionMap}' or `{error, Reason}'
-spec list_collections() -> {ok, map()} | {error, term()}.
list_collections() ->
    case is_clustered() of
        true ->
            barrel_vectordb_cluster_client:get_collections();
        false ->
            {error, not_clustered}
    end.

%%====================================================================
%% Cluster Document Operations (explicit cluster mode)
%%====================================================================

%% @doc Add a document to a cluster collection.
%%
%% Routes to appropriate shard based on document ID.
%% Only available when clustered.
%%
%% @param Collection Collection name
%% @param Id Document identifier
%% @param Text Text to embed and store
%% @param Metadata Document metadata
%% @returns `ok' on success, `{error, Reason}' on failure
-spec cluster_add(binary(), id(), text(), metadata()) -> ok | {error, term()}.
cluster_add(Collection, Id, Text, Metadata) when is_binary(Collection), is_binary(Id) ->
    case is_clustered() of
        true ->
            EmbedderInfo = get_collection_embedder(Collection),
            barrel_vectordb_shard_router:route_add(Collection, Id, Text, Metadata, EmbedderInfo);
        false ->
            {error, not_clustered}
    end.

%% @doc Add a document with explicit vector to a cluster collection.
-spec cluster_add(binary(), id(), text(), metadata(), vector()) -> ok | {error, term()}.
cluster_add(Collection, Id, Text, Metadata, Vector) when is_binary(Collection), is_binary(Id) ->
    cluster_add_vector(Collection, Id, Text, Metadata, Vector).

%% @doc Add a document with vector to a cluster collection.
-spec cluster_add_vector(binary(), id(), text(), metadata(), vector()) -> ok | {error, term()}.
cluster_add_vector(Collection, Id, Text, Metadata, Vector) when is_binary(Collection), is_binary(Id) ->
    case is_clustered() of
        true ->
            EmbedderInfo = get_collection_embedder(Collection),
            barrel_vectordb_shard_router:route_add_vector(Collection, Id, Text, Metadata, Vector, EmbedderInfo);
        false ->
            {error, not_clustered}
    end.

%% @doc Get a document from a cluster collection.
-spec cluster_get(binary(), id()) -> {ok, map()} | not_found | {error, term()}.
cluster_get(Collection, Id) when is_binary(Collection), is_binary(Id) ->
    case is_clustered() of
        true ->
            barrel_vectordb_shard_router:route_get(Collection, Id, #{});
        false ->
            {error, not_clustered}
    end.

%% @doc Delete a document from a cluster collection.
-spec cluster_delete(binary(), id()) -> ok | {error, term()}.
cluster_delete(Collection, Id) when is_binary(Collection), is_binary(Id) ->
    case is_clustered() of
        true ->
            barrel_vectordb_shard_router:route_delete(Collection, Id, #{});
        false ->
            {error, not_clustered}
    end.

%% @doc Search a cluster collection with text query.
%%
%% Scatter-gather across all shards.
-spec cluster_search(binary(), text(), search_opts()) -> {ok, [search_result()]} | {error, term()}.
cluster_search(Collection, Query, Opts) when is_binary(Collection), is_binary(Query) ->
    case is_clustered() of
        true ->
            EmbedderInfo = get_collection_embedder(Collection),
            OptsWithCollection = Opts#{collection => Collection},
            barrel_vectordb_shard_router:route_search(Collection, Query, OptsWithCollection, EmbedderInfo);
        false ->
            {error, not_clustered}
    end.

%% @doc Search a cluster collection with vector query.
%%
%% Scatter-gather across all shards.
-spec cluster_search_vector(binary(), vector(), search_opts()) -> {ok, [search_result()]} | {error, term()}.
cluster_search_vector(Collection, Vector, Opts) when is_binary(Collection), is_list(Vector) ->
    case is_clustered() of
        true ->
            OptsWithCollection = Opts#{collection => Collection},
            barrel_vectordb_shard_router:route_search_vector(Collection, Vector, OptsWithCollection, #{});
        false ->
            {error, not_clustered}
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
%% Get embedder info for a collection (from cluster metadata)
get_collection_embedder(Collection) ->
    case barrel_vectordb_cluster_client:get_collections() of
        {ok, Collections} ->
            case maps:get(Collection, Collections, undefined) of
                undefined ->
                    #{};
                Meta ->
                    case element(4, Meta) of  %% embedder field in collection metadata
                        undefined -> #{};
                        EmbedderConfig -> #{embedder => EmbedderConfig}
                    end
            end;
        _ ->
            #{}
    end.

%% @private
%% Generate default database path for a store name
default_path(Name) when is_atom(Name) ->
    "priv/barrel_vectordb_" ++ atom_to_list(Name).
