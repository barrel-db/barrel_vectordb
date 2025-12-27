%%%-------------------------------------------------------------------
%%% @doc Per-store gen_server managing RocksDB, HNSW index, and embeddings
%%%
%%% Each store runs as a separate gen_server registered under its name.
%%% Handles all document operations, search, and embedding coordination.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_server).
-behaviour(gen_server).

-include("barrel_vectordb.hrl").

%% API
-export([
    start_link/2,
    stop/1,
    add/4,
    add_vector/5,
    add_batch/2,
    get/2,
    delete/2,
    search/3,
    search_vector/3,
    embed/2,
    embed_batch/2,
    stats/1,
    count/1,
    embedder_info/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    name :: atom(),
    db :: rocksdb:db_handle(),
    cf_vectors :: rocksdb:cf_handle(),
    cf_metadata :: rocksdb:cf_handle(),
    cf_text :: rocksdb:cf_handle(),
    cf_hnsw :: rocksdb:cf_handle(),
    hnsw_index :: hnsw_index(),
    dimension :: pos_integer(),
    embed_state :: map(),
    config :: map()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a named store.
-spec start_link(atom(), map()) -> {ok, pid()} | {error, term()}.
start_link(Name, Config) ->
    gen_server:start_link({local, Name}, ?MODULE, {Name, Config}, []).

%% @doc Stop a store.
-spec stop(atom() | pid()) -> ok.
stop(Store) ->
    gen_server:stop(Store).

%% @doc Add document with auto-embedding.
-spec add(atom() | pid(), binary(), binary(), map()) -> ok | {error, term()}.
add(Store, Id, Text, Metadata) ->
    gen_server:call(Store, {add, Id, Text, Metadata}, infinity).

%% @doc Add document with explicit vector.
-spec add_vector(atom() | pid(), binary(), binary(), map(), [float()]) -> ok | {error, term()}.
add_vector(Store, Id, Text, Metadata, Vector) ->
    gen_server:call(Store, {add_vector, Id, Text, Metadata, Vector}, infinity).

%% @doc Add multiple documents.
-spec add_batch(atom() | pid(), [{binary(), binary(), map()}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_batch(Store, Docs) ->
    gen_server:call(Store, {add_batch, Docs}, infinity).

%% @doc Get document by ID.
-spec get(atom() | pid(), binary()) -> {ok, map()} | not_found | {error, term()}.
get(Store, Id) ->
    gen_server:call(Store, {get, Id}).

%% @doc Delete document.
-spec delete(atom() | pid(), binary()) -> ok | {error, term()}.
delete(Store, Id) ->
    gen_server:call(Store, {delete, Id}).

%% @doc Search with text query.
-spec search(atom() | pid(), binary(), map()) -> {ok, [map()]} | {error, term()}.
search(Store, Query, Opts) ->
    gen_server:call(Store, {search, Query, Opts}, infinity).

%% @doc Search with vector query.
-spec search_vector(atom() | pid(), [float()], map()) -> {ok, [map()]} | {error, term()}.
search_vector(Store, Vector, Opts) ->
    gen_server:call(Store, {search_vector, Vector, Opts}, infinity).

%% @doc Embed single text.
-spec embed(atom() | pid(), binary()) -> {ok, [float()]} | {error, term()}.
embed(Store, Text) ->
    gen_server:call(Store, {embed, Text}, infinity).

%% @doc Embed multiple texts.
-spec embed_batch(atom() | pid(), [binary()]) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Store, Texts) ->
    gen_server:call(Store, {embed_batch, Texts}, infinity).

%% @doc Get store statistics.
-spec stats(atom() | pid()) -> {ok, map()}.
stats(Store) ->
    gen_server:call(Store, stats).

%% @doc Get document count.
-spec count(atom() | pid()) -> non_neg_integer().
count(Store) ->
    gen_server:call(Store, count).

%% @doc Get embedder information.
-spec embedder_info(atom() | pid()) -> {ok, map()}.
embedder_info(Store) ->
    gen_server:call(Store, embedder_info).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init({Name, Config}) ->
    process_flag(trap_exit, true),

    DbPath = maps:get(db_path, Config, "priv/barrel_vectordb_data"),
    Dimension = maps:get(dimension, Config, ?DEFAULT_DIMENSION),
    HnswConfig = maps:get(hnsw, Config, #{}),

    %% Initialize embedder
    EmbedConfig = Config#{dimensions => Dimension},
    case barrel_vectordb_embed:init(EmbedConfig) of
        {ok, EmbedState} ->
            case init_rocksdb(DbPath) of
                {ok, Db, CfHandles} ->
                    %% Load or create HNSW index
                    HnswIndex = load_or_create_index(Db, CfHandles, Dimension, HnswConfig),

                    State = #state{
                        name = Name,
                        db = Db,
                        cf_vectors = maps:get(vectors, CfHandles),
                        cf_metadata = maps:get(metadata, CfHandles),
                        cf_text = maps:get(text, CfHandles),
                        cf_hnsw = maps:get(hnsw, CfHandles),
                        hnsw_index = HnswIndex,
                        dimension = Dimension,
                        embed_state = EmbedState,
                        config = Config
                    },
                    {ok, State};
                {error, Reason} ->
                    {stop, {db_open_failed, Reason}}
            end;
        {error, EmbedError} ->
            {stop, {embed_init_failed, EmbedError}}
    end.

handle_call({add, Id, Text, Metadata}, _From, State) ->
    case do_embed(Text, State) of
        {ok, Vector} ->
            case do_add(Id, Text, Metadata, Vector, State) of
                {ok, NewState} ->
                    {reply, ok, NewState};
                {error, _} = Error ->
                    {reply, Error, State}
            end;
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({add_vector, Id, Text, Metadata, Vector}, _From, #state{dimension = Dim} = State) ->
    case length(Vector) of
        Dim ->
            case do_add(Id, Text, Metadata, Vector, State) of
                {ok, NewState} ->
                    {reply, ok, NewState};
                {error, _} = Error ->
                    {reply, Error, State}
            end;
        Other ->
            {reply, {error, {dimension_mismatch, Dim, Other}}, State}
    end;

handle_call({add_batch, Docs}, _From, State) ->
    Result = do_add_batch(Docs, State),
    case Result of
        {ok, Stats, NewState} ->
            {reply, {ok, Stats}, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({get, Id}, _From, State) ->
    Result = do_get(Id, State),
    {reply, Result, State};

handle_call({delete, Id}, _From, State) ->
    case do_delete(Id, State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({search, Query, Opts}, _From, State) ->
    case do_embed(Query, State) of
        {ok, Vector} ->
            Result = do_search(Vector, Opts, State),
            {reply, Result, State};
        {error, _} = Error ->
            {reply, Error, State}
    end;

handle_call({search_vector, Vector, Opts}, _From, #state{dimension = Dim} = State) ->
    case length(Vector) of
        Dim ->
            Result = do_search(Vector, Opts, State),
            {reply, Result, State};
        Other ->
            {reply, {error, {dimension_mismatch, Dim, Other}}, State}
    end;

handle_call({embed, Text}, _From, State) ->
    Result = do_embed(Text, State),
    {reply, Result, State};

handle_call({embed_batch, Texts}, _From, State) ->
    Result = do_embed_batch(Texts, State),
    {reply, Result, State};

handle_call(stats, _From, #state{hnsw_index = Index, dimension = Dim, config = Config} = State) ->
    Stats = #{
        dimension => Dim,
        count => barrel_vectordb_hnsw:size(Index),
        hnsw => barrel_vectordb_hnsw:info(Index),
        config => Config
    },
    {reply, {ok, Stats}, State};

handle_call(count, _From, #state{hnsw_index = Index} = State) ->
    {reply, barrel_vectordb_hnsw:size(Index), State};

handle_call(embedder_info, _From, #state{embed_state = EmbedState} = State) ->
    Info = barrel_vectordb_embed:info(EmbedState),
    {reply, {ok, Info}, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db = Db, hnsw_index = Index, cf_hnsw = CfHnsw}) ->
    %% Persist HNSW index metadata before closing
    persist_hnsw_meta(Db, CfHnsw, Index),
    rocksdb:close(Db),
    ok.

%%====================================================================
%% Internal Functions - Database
%%====================================================================

%% Initialize RocksDB with column families
init_rocksdb(DbPath) ->
    %% Ensure directory exists
    ok = filelib:ensure_dir(DbPath ++ "/"),

    Options = [{create_if_missing, true}, {create_missing_column_families, true}],

    CfDefs = [
        {?CF_DEFAULT, []},
        {?CF_VECTORS, []},
        {?CF_METADATA, []},
        {?CF_TEXT, []},
        {?CF_HNSW, []}
    ],

    case rocksdb:open_with_cf(DbPath, Options, CfDefs) of
        {ok, Db, [_Default, CfVectors, CfMetadata, CfText, CfHnsw]} ->
            {ok, Db, #{
                vectors => CfVectors,
                metadata => CfMetadata,
                text => CfText,
                hnsw => CfHnsw
            }};
        {error, Reason} ->
            {error, Reason}
    end.

%% Load existing index or create new one
load_or_create_index(Db, CfHandles, Dimension, HnswConfig) ->
    CfHnsw = maps:get(hnsw, CfHandles),
    CfVectors = maps:get(vectors, CfHandles),

    case load_hnsw_meta(Db, CfHnsw) of
        {ok, _IndexMeta} ->
            rebuild_from_vectors(Db, CfVectors, CfHnsw, Dimension, HnswConfig);
        not_found ->
            barrel_vectordb_hnsw:new(HnswConfig#{dimension => Dimension})
    end.

%% Load HNSW metadata from storage
load_hnsw_meta(Db, CfHnsw) ->
    case rocksdb:get(Db, CfHnsw, ?HNSW_META_KEY, []) of
        {ok, Binary} ->
            try
                Meta = binary_to_term(Binary),
                {ok, Meta}
            catch
                _:_ -> not_found
            end;
        not_found ->
            not_found;
        {error, _} ->
            not_found
    end.

%% Persist HNSW metadata
persist_hnsw_meta(Db, CfHnsw, Index) ->
    Meta = #{
        entry_point => Index#hnsw_index.entry_point,
        max_layer => Index#hnsw_index.max_layer,
        size => Index#hnsw_index.size,
        dimension => Index#hnsw_index.dimension
    },
    Binary = term_to_binary(Meta),
    rocksdb:put(Db, CfHnsw, ?HNSW_META_KEY, Binary, []).

%% Rebuild index from stored vectors
rebuild_from_vectors(Db, CfVectors, CfHnsw, Dimension, HnswConfig) ->
    Index = barrel_vectordb_hnsw:new(HnswConfig#{dimension => Dimension}),

    case rocksdb:iterator(Db, CfVectors, []) of
        {ok, Iter} ->
            try
                rebuild_loop(Db, Iter, rocksdb:iterator_move(Iter, first), CfHnsw, Index)
            after
                rocksdb:iterator_close(Iter)
            end;
        {error, _} ->
            Index
    end.

rebuild_loop(_Db, _Iter, {error, _}, _CfHnsw, Index) ->
    Index;
rebuild_loop(Db, Iter, {ok, Key, VectorBin}, CfHnsw, Index) ->
    Vector = decode_vector(VectorBin),
    NewIndex = barrel_vectordb_hnsw:insert(Index, Key, Vector),

    case barrel_vectordb_hnsw:get_node(NewIndex, Key) of
        {ok, Node} ->
            NodeBin = barrel_vectordb_hnsw:serialize_node(Node),
            rocksdb:put(Db, CfHnsw, Key, NodeBin, []);
        not_found ->
            ok
    end,

    rebuild_loop(Db, Iter, rocksdb:iterator_move(Iter, next), CfHnsw, NewIndex).

%%====================================================================
%% Internal Functions - Operations
%%====================================================================

%% Embed text using the configured provider
do_embed(Text, #state{embed_state = EmbedState}) ->
    barrel_vectordb_embed:embed(Text, EmbedState).

%% Embed batch of texts
do_embed_batch(Texts, #state{embed_state = EmbedState}) ->
    barrel_vectordb_embed:embed_batch(Texts, EmbedState).

%% Add a document
do_add(Id, Text, Metadata, Vector, #state{db = Db, cf_vectors = CfV,
                                          cf_metadata = CfM, cf_text = CfT,
                                          cf_hnsw = CfH, hnsw_index = Index} = State) ->
    VectorBin = encode_vector(Vector),
    MetadataBin = term_to_binary(Metadata),

    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
    ok = rocksdb:batch_put(Batch, CfM, Id, MetadataBin),
    ok = rocksdb:batch_put(Batch, CfT, Id, Text),

    case rocksdb:write_batch(Db, Batch, []) of
        ok ->
            NewIndex = barrel_vectordb_hnsw:insert(Index, Id, Vector),

            case barrel_vectordb_hnsw:get_node(NewIndex, Id) of
                {ok, Node} ->
                    NodeBin = barrel_vectordb_hnsw:serialize_node(Node),
                    rocksdb:put(Db, CfH, Id, NodeBin, []);
                not_found ->
                    ok
            end,

            {ok, State#state{hnsw_index = NewIndex}};
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

%% Add multiple documents
do_add_batch(Docs, State) ->
    do_add_batch(Docs, 0, State).

do_add_batch([], Count, State) ->
    {ok, #{inserted => Count}, State};
do_add_batch([{Id, Text, Metadata} | Rest], Count, State) ->
    case do_embed(Text, State) of
        {ok, Vector} ->
            case do_add(Id, Text, Metadata, Vector, State) of
                {ok, NewState} ->
                    do_add_batch(Rest, Count + 1, NewState);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% Get a document
do_get(Id, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM, cf_text = CfT}) ->
    case rocksdb:get(Db, CfV, Id, []) of
        {ok, VectorBin} ->
            case {rocksdb:get(Db, CfM, Id, []), rocksdb:get(Db, CfT, Id, [])} of
                {{ok, MetadataBin}, {ok, Text}} ->
                    {ok, #{
                        key => Id,
                        vector => decode_vector(VectorBin),
                        metadata => binary_to_term(MetadataBin),
                        text => Text
                    }};
                _ ->
                    {error, incomplete_data}
            end;
        not_found ->
            not_found;
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

%% Delete a document
do_delete(Id, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM,
                     cf_text = CfT, cf_hnsw = CfH, hnsw_index = Index} = State) ->
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_delete(Batch, CfV, Id),
    ok = rocksdb:batch_delete(Batch, CfM, Id),
    ok = rocksdb:batch_delete(Batch, CfT, Id),
    ok = rocksdb:batch_delete(Batch, CfH, Id),

    case rocksdb:write_batch(Db, Batch, []) of
        ok ->
            NewIndex = barrel_vectordb_hnsw:delete(Index, Id),
            {ok, State#state{hnsw_index = NewIndex}};
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

%% Search for similar documents
do_search(QueryVector, Opts, #state{db = Db, hnsw_index = Index,
                                     cf_metadata = CfM, cf_text = CfT}) ->
    K = maps:get(k, Opts, 5),
    HnswResults = barrel_vectordb_hnsw:search(Index, QueryVector, K, Opts),

    Filter = maps:get(filter, Opts, fun(_) -> true end),

    Results = lists:filtermap(
        fun({Id, Distance}) ->
            case {rocksdb:get(Db, CfM, Id, []), rocksdb:get(Db, CfT, Id, [])} of
                {{ok, MetadataBin}, {ok, Text}} ->
                    Metadata = binary_to_term(MetadataBin),
                    case Filter(Metadata) of
                        true ->
                            {true, #{
                                key => Id,
                                text => Text,
                                metadata => Metadata,
                                score => 1.0 - Distance
                            }};
                        false ->
                            false
                    end;
                _ ->
                    false
            end
        end,
        HnswResults
    ),

    {ok, Results}.

%%====================================================================
%% Internal Functions - Encoding
%%====================================================================

encode_vector(Vector) when is_list(Vector) ->
    << <<F:64/float-little>> || F <- Vector >>;
encode_vector(Binary) when is_binary(Binary) ->
    Binary.

decode_vector(Binary) ->
    [F || <<F:64/float-little>> <= Binary].
