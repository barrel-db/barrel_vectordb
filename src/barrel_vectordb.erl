%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb - Erlang Vector Database
%%%
%%% An Erlang library for storing and searching vectors with built-in
%%% embedding support. Use it to add semantic search to your application.
%%%
%%% == Quick Start ==
%%% ```
%%% %% Start a store
%%% {ok, _} = barrel_vectordb:start_link(#{
%%%     name => my_store,
%%%     path => "/tmp/vectors",
%%%     dimensions => 768
%%% }).
%%%
%%% %% Add a document (auto-embeds the text)
%%% ok = barrel_vectordb:add(my_store, <<"doc-1">>, <<"Hello world">>, #{type => greeting}).
%%%
%%% %% Search for similar documents
%%% {ok, Results} = barrel_vectordb:search(my_store, <<"greetings">>, #{k => 5}).
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% #{
%%%     name => atom(),              %% Store name (required)
%%%     path => string(),            %% RocksDB path (default: "priv/barrel_vectordb_data")
%%%     dimensions => pos_integer(), %% Vector dimensions (default: 768)
%%%     embedder => EmbedderConfig,  %% Embedding provider config
%%%     hnsw => HnswConfig           %% HNSW index parameters
%%% }
%%% '''
%%%
%%% == Embedding Providers ==
%%% ```
%%% %% Local CPU (default) - no GPU required
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
%%% %% OpenAI API
%%% embedder => {openai, #{
%%%     api_key => "sk-...",
%%%     model => "text-embedding-3-small"
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

%% API - Document Operations
-export([
    add/4,
    add/5,
    add_vector/5,
    add_batch/2,
    get/2,
    delete/2
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
    include_vectors => boolean()
}.
%% Options for search operations.
%% - `k': Number of results to return (default: 5)
%% - `filter': Function to filter results by metadata
%% - `include_vectors': Whether to include vectors in results

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

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
%% Generate default database path for a store name
default_path(Name) when is_atom(Name) ->
    "priv/barrel_vectordb_" ++ atom_to_list(Name).
