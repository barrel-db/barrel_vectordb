%%%-------------------------------------------------------------------
%%% @doc Per-store gen_batch_server managing RocksDB, vector index, and embeddings
%%%
%%% Each store runs as a separate gen_batch_server registered under its name.
%%% Handles all document operations, search, and embedding coordination.
%%%
%%% Uses gen_batch_server to automatically batch concurrent write operations
%%% into single atomic RocksDB WriteBatch operations, improving throughput
%%% under concurrent load.
%%%
%%% Supports pluggable vector index backends:
%%% - hnsw: Pure Erlang HNSW implementation (default)
%%% - faiss: Facebook FAISS via NIF binding (optional, requires barrel_faiss)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_server).
-behaviour(gen_batch_server).

-include("barrel_vectordb.hrl").

%% API
-export([
    start_link/2,
    stop/1,
    add/4,
    add_vector/5,
    add_batch/2,
    add_vector_batch/2,
    get/2,
    update/4,
    upsert/4,
    delete/2,
    peek/2,
    search/3,
    search_vector/3,
    embed/2,
    embed_batch/2,
    stats/1,
    count/1,
    embedder_info/1,
    checkpoint/1
]).

%% gen_batch_server callbacks
-export([init/1, handle_batch/2, terminate/2]).

-record(state, {
    name :: atom(),
    db :: rocksdb:db_handle(),
    cf_vectors :: rocksdb:cf_handle(),
    cf_metadata :: rocksdb:cf_handle(),
    cf_text :: rocksdb:cf_handle(),
    cf_hnsw :: rocksdb:cf_handle(),
    index :: term(),                      %% Vector index (HNSW or FAISS state)
    index_module :: module(),             %% Index backend module
    dimension :: pos_integer(),
    embed_state :: map(),
    config :: map()
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start a named store.
%% Config options:
%%   - db_path: RocksDB storage path
%%   - dimension: Vector dimension
%%   - hnsw: HNSW index configuration
%%   - batch: gen_batch_server options (max_batch_size, min_batch_size)
%%
%% Default batch settings optimized for vector DB workloads:
%%   - min_batch_size: 4 (responsive for single inserts, batches concurrent ones)
%%   - max_batch_size: 256 (reasonable upper bound for memory/latency)
-spec start_link(atom(), map()) -> {ok, pid()} | {error, term()}.
start_link(Name, Config) ->
    BatchOpts = maps:get(batch, Config, #{}),
    %% Apply sensible defaults for vector DB workload
    DefaultBatch = #{min_batch_size => 4, max_batch_size => 256},
    MergedBatch = maps:merge(DefaultBatch, BatchOpts),
    GBOpts = maps:fold(fun
        (max_batch_size, V, Acc) -> [{max_batch_size, V} | Acc];
        (min_batch_size, V, Acc) -> [{min_batch_size, V} | Acc];
        (_, _, Acc) -> Acc
    end, [], MergedBatch),
    gen_batch_server:start_link({local, Name}, ?MODULE, {Name, Config},
                                [{gen_batch_server, GBOpts}]).

%% @doc Stop a store.
-spec stop(atom() | pid()) -> ok.
stop(Store) ->
    gen_batch_server:stop(Store).

%% @doc Add document with auto-embedding.
-spec add(atom() | pid(), binary(), binary(), map()) -> ok | {error, term()}.
add(Store, Id, Text, Metadata) ->
    gen_batch_server:call(Store, {add, Id, Text, Metadata}, infinity).

%% @doc Add document with explicit vector.
-spec add_vector(atom() | pid(), binary(), binary(), map(), [float()]) -> ok | {error, term()}.
add_vector(Store, Id, Text, Metadata, Vector) ->
    gen_batch_server:call(Store, {add_vector, Id, Text, Metadata, Vector}, infinity).

%% @doc Add multiple documents.
-spec add_batch(atom() | pid(), [{binary(), binary(), map()}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_batch(Store, Docs) ->
    gen_batch_server:call(Store, {add_batch, Docs}, infinity).

%% @doc Add multiple documents with pre-computed vectors (bulk insert).
-spec add_vector_batch(atom() | pid(), [{binary(), binary(), map(), [float()]}]) ->
    {ok, #{inserted := non_neg_integer()}} | {error, term()}.
add_vector_batch(Store, Docs) ->
    gen_batch_server:call(Store, {add_vector_batch, Docs}, infinity).

%% @doc Get document by ID.
-spec get(atom() | pid(), binary()) -> {ok, map()} | not_found | {error, term()}.
get(Store, Id) ->
    gen_batch_server:call(Store, {get, Id}).

%% @doc Update document metadata (re-embeds the text).
-spec update(atom() | pid(), binary(), binary(), map()) -> ok | not_found | {error, term()}.
update(Store, Id, Text, Metadata) ->
    gen_batch_server:call(Store, {update, Id, Text, Metadata}, infinity).

%% @doc Insert or update document.
-spec upsert(atom() | pid(), binary(), binary(), map()) -> ok | {error, term()}.
upsert(Store, Id, Text, Metadata) ->
    gen_batch_server:call(Store, {upsert, Id, Text, Metadata}, infinity).

%% @doc Delete document.
-spec delete(atom() | pid(), binary()) -> ok | {error, term()}.
delete(Store, Id) ->
    gen_batch_server:call(Store, {delete, Id}).

%% @doc Peek at documents (sample without search).
-spec peek(atom() | pid(), pos_integer()) -> {ok, [map()]}.
peek(Store, Limit) ->
    gen_batch_server:call(Store, {peek, Limit}).

%% @doc Search with text query.
-spec search(atom() | pid(), binary(), map()) -> {ok, [map()]} | {error, term()}.
search(Store, Query, Opts) ->
    gen_batch_server:call(Store, {search, Query, Opts}, infinity).

%% @doc Search with vector query.
-spec search_vector(atom() | pid(), [float()], map()) -> {ok, [map()]} | {error, term()}.
search_vector(Store, Vector, Opts) ->
    gen_batch_server:call(Store, {search_vector, Vector, Opts}, infinity).

%% @doc Embed single text.
-spec embed(atom() | pid(), binary()) -> {ok, [float()]} | {error, term()}.
embed(Store, Text) ->
    gen_batch_server:call(Store, {embed, Text}, infinity).

%% @doc Embed multiple texts.
-spec embed_batch(atom() | pid(), [binary()]) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Store, Texts) ->
    gen_batch_server:call(Store, {embed_batch, Texts}, infinity).

%% @doc Get store statistics.
-spec stats(atom() | pid()) -> {ok, map()}.
stats(Store) ->
    gen_batch_server:call(Store, stats).

%% @doc Get document count.
-spec count(atom() | pid()) -> non_neg_integer().
count(Store) ->
    gen_batch_server:call(Store, count).

%% @doc Get embedder information.
-spec embedder_info(atom() | pid()) -> {ok, map()}.
embedder_info(Store) ->
    gen_batch_server:call(Store, embedder_info).

%% @doc Checkpoint HNSW index to disk.
-spec checkpoint(atom() | pid()) -> ok.
checkpoint(Store) ->
    gen_batch_server:call(Store, checkpoint).

%%====================================================================
%% gen_batch_server callbacks
%%====================================================================

init({Name, Config}) ->
    process_flag(trap_exit, true),

    DbPath = maps:get(db_path, Config, "priv/barrel_vectordb_data"),
    Dimension = maps:get(dimension, Config, ?DEFAULT_DIMENSION),

    %% Select index backend (default: hnsw)
    Backend = maps:get(backend, Config, hnsw),
    IndexModule = barrel_vectordb_index:backend_module(Backend),

    %% Get backend-specific configuration
    IndexConfig = case Backend of
        hnsw -> maps:get(hnsw, Config, #{});
        faiss -> maps:get(faiss, Config, #{});
        diskann ->
            DiskAnnConfig = maps:get(diskann, Config, #{}),
            %% Set default base_path relative to db_path if not specified
            case maps:is_key(base_path, DiskAnnConfig) of
                true -> DiskAnnConfig;
                false -> DiskAnnConfig#{base_path => filename:join(DbPath, "diskann")}
            end
    end,

    %% Initialize embedder
    EmbedConfig = Config#{dimensions => Dimension},
    case barrel_vectordb_embed:init(EmbedConfig) of
        {ok, EmbedState} ->
            case init_rocksdb(DbPath) of
                {ok, Db, CfHandles} ->
                    %% Load or create vector index
                    case load_or_create_index(Db, CfHandles, Dimension, IndexConfig, IndexModule) of
                        {ok, Index} ->
                            State = #state{
                                name = Name,
                                db = Db,
                                cf_vectors = maps:get(vectors, CfHandles),
                                cf_metadata = maps:get(metadata, CfHandles),
                                cf_text = maps:get(text, CfHandles),
                                cf_hnsw = maps:get(hnsw, CfHandles),
                                index = Index,
                                index_module = IndexModule,
                                dimension = Dimension,
                                embed_state = EmbedState,
                                config = Config
                            },
                            {ok, State};
                        {error, IndexError} ->
                            {stop, {index_init_failed, IndexError}}
                    end;
                {error, Reason} ->
                    {stop, {db_open_failed, Reason}}
            end;
        {error, EmbedError} ->
            {stop, {embed_init_failed, EmbedError}}
    end.

%% @doc Handle a batch of operations.
%% Partitions operations into reads (processed immediately) and writes (batched atomically).
handle_batch(Ops, State) ->
    %% Separate reads from writes
    {Reads, Writes} = partition_ops(Ops),

    %% Process reads immediately (they don't modify state)
    {ReadActions, State1} = process_reads(Reads, State),

    %% Process writes atomically in a single batch
    {WriteActions, State2} = process_writes_atomic(Writes, State1),

    {ok, ReadActions ++ WriteActions, State2}.

%% Partition operations into reads and writes
partition_ops(Ops) ->
    lists:partition(fun(Op) ->
        case Op of
            {_, _, {add, _, _, _}} -> false;
            {_, _, {add_vector, _, _, _, _}} -> false;
            {_, _, {add_batch, _}} -> false;
            {_, _, {add_vector_batch, _}} -> false;
            _ -> true  % reads: get, search, peek, stats, count, delete, update, upsert
        end
    end, Ops).

%% Process read operations (and delete/update/upsert which need immediate state access)
process_reads(Reads, State) ->
    lists:foldl(fun(Op, {AccActions, AccState}) ->
        {Action, NewState} = process_single_op(Op, AccState),
        {[Action | AccActions], NewState}
    end, {[], State}, Reads).

%% Process a single operation (for reads and non-batched operations)
process_single_op({call, From, {get, Id}}, State) ->
    Result = do_get(Id, State),
    {{reply, From, Result}, State};

process_single_op({call, From, {delete, Id}}, State) ->
    case do_delete(Id, State) of
        {ok, NewState} ->
            {{reply, From, ok}, NewState};
        {error, _} = Error ->
            {{reply, From, Error}, State}
    end;

process_single_op({call, From, {update, Id, Text, Metadata}}, State) ->
    case do_get(Id, State) of
        {ok, _Existing} ->
            case do_delete(Id, State) of
                {ok, State1} ->
                    case do_embed(Text, State1) of
                        {ok, Vector} ->
                            case do_add(Id, Text, Metadata, Vector, State1) of
                                {ok, NewState} ->
                                    {{reply, From, ok}, NewState};
                                {error, _} = Error ->
                                    {{reply, From, Error}, State}
                            end;
                        {error, _} = Error ->
                            {{reply, From, Error}, State}
                    end;
                {error, _} = Error ->
                    {{reply, From, Error}, State}
            end;
        not_found ->
            {{reply, From, not_found}, State};
        {error, _} = Error ->
            {{reply, From, Error}, State}
    end;

process_single_op({call, From, {upsert, Id, Text, Metadata}}, State) ->
    case do_get(Id, State) of
        {ok, _Existing} ->
            case do_delete(Id, State) of
                {ok, State1} ->
                    case do_embed(Text, State1) of
                        {ok, Vector} ->
                            case do_add(Id, Text, Metadata, Vector, State1) of
                                {ok, NewState} ->
                                    {{reply, From, ok}, NewState};
                                {error, _} = Error ->
                                    {{reply, From, Error}, State}
                            end;
                        {error, _} = Error ->
                            {{reply, From, Error}, State}
                    end;
                {error, _} = Error ->
                    {{reply, From, Error}, State}
            end;
        not_found ->
            case do_embed(Text, State) of
                {ok, Vector} ->
                    case do_add(Id, Text, Metadata, Vector, State) of
                        {ok, NewState} ->
                            {{reply, From, ok}, NewState};
                        {error, _} = Error ->
                            {{reply, From, Error}, State}
                    end;
                {error, _} = Error ->
                    {{reply, From, Error}, State}
            end;
        {error, _} = Error ->
            {{reply, From, Error}, State}
    end;

process_single_op({call, From, {peek, Limit}}, State) ->
    Result = do_peek(Limit, State),
    {{reply, From, Result}, State};

process_single_op({call, From, {search, Query, Opts}}, State) ->
    case do_embed(Query, State) of
        {ok, Vector} ->
            Result = do_search(Vector, Opts, State),
            {{reply, From, Result}, State};
        {error, _} = Error ->
            {{reply, From, Error}, State}
    end;

process_single_op({call, From, {search_vector, Vector, Opts}}, #state{dimension = Dim} = State) ->
    case length(Vector) of
        Dim ->
            Result = do_search(Vector, Opts, State),
            {{reply, From, Result}, State};
        Other ->
            {{reply, From, {error, {dimension_mismatch, Dim, Other}}}, State}
    end;

process_single_op({call, From, {embed, Text}}, State) ->
    Result = do_embed(Text, State),
    {{reply, From, Result}, State};

process_single_op({call, From, {embed_batch, Texts}}, State) ->
    Result = do_embed_batch(Texts, State),
    {{reply, From, Result}, State};

process_single_op({call, From, stats}, #state{index = Index, index_module = Mod,
                                                dimension = Dim, config = Config} = State) ->
    Stats = #{
        dimension => Dim,
        count => Mod:size(Index),
        index => Mod:info(Index),
        config => Config
    },
    {{reply, From, {ok, Stats}}, State};

process_single_op({call, From, count}, #state{index = Index, index_module = Mod} = State) ->
    {{reply, From, Mod:size(Index)}, State};

process_single_op({call, From, embedder_info}, #state{embed_state = EmbedState} = State) ->
    Info = barrel_vectordb_embed:info(EmbedState),
    {{reply, From, {ok, Info}}, State};

process_single_op({call, From, checkpoint}, #state{db = Db, cf_hnsw = CfHnsw,
                                                    index = Index, index_module = Mod} = State) ->
    _ = persist_index_meta(Db, CfHnsw, Index, Mod),
    {{reply, From, ok}, State};

process_single_op({call, From, _Unknown}, State) ->
    {{reply, From, {error, unknown_request}}, State}.

%% Process write operations atomically in a single RocksDB batch
process_writes_atomic([], State) ->
    {[], State};
process_writes_atomic(Writes, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM,
                                      cf_text = CfT, cf_hnsw = CfH,
                                      index = Index, index_module = Mod, dimension = Dim} = State) ->
    {ok, Batch} = rocksdb:batch(),

    %% First, prepare all embeddings if needed (this is done before batch to avoid
    %% partial writes on embedding failures)
    case prepare_writes(Writes, State) of
        {ok, PreparedWrites} ->
            %% Now apply all writes to the batch
            {NewIndex, Replies} = lists:foldl(fun({From, WriteOp, PreparedData}, {AccIndex, AccReplies}) ->
                case apply_write_to_batch(WriteOp, PreparedData, Batch, CfV, CfM, CfT, CfH, AccIndex, Mod, Dim) of
                    {ok, UpdatedIndex, Reply} ->
                        {UpdatedIndex, [{reply, From, Reply} | AccReplies]};
                    {error, Reason} ->
                        {AccIndex, [{reply, From, {error, Reason}} | AccReplies]}
                end
            end, {Index, []}, PreparedWrites),

            %% Commit the batch atomically
            case rocksdb:write_batch(Db, Batch, []) of
                ok ->
                    {Replies, State#state{index = NewIndex}};
                {error, Reason} ->
                    %% All writes fail on batch commit error
                    ErrorReplies = [{reply, From, {error, {db_error, Reason}}}
                                   || {From, _, _} <- PreparedWrites],
                    {ErrorReplies, State}
            end;
        {error, From, Reason, SuccessfulPreps} ->
            %% Embedding failed for one operation, fail that one and process others
            ErrorReply = {reply, From, {error, Reason}},
            case SuccessfulPreps of
                [] ->
                    {[ErrorReply], State};
                _ ->
                    %% Process successful preps
                    {OkReplies, NewState} = process_writes_atomic(
                        [{call, F, Op} || {F, Op, _} <- SuccessfulPreps],
                        State),
                    {[ErrorReply | OkReplies], NewState}
            end
    end.

%% Prepare writes by computing embeddings where needed
prepare_writes(Writes, State) ->
    prepare_writes(Writes, State, []).

prepare_writes([], _State, Acc) ->
    {ok, lists:reverse(Acc)};
prepare_writes([{call, From, {add, Id, Text, Metadata}} | Rest], State, Acc) ->
    case do_embed(Text, State) of
        {ok, Vector} ->
            prepare_writes(Rest, State, [{From, {add_vector, Id, Text, Metadata, Vector}, prepared} | Acc]);
        {error, Reason} ->
            {error, From, Reason, lists:reverse(Acc)}
    end;
prepare_writes([{call, From, {add_vector, Id, Text, Metadata, Vector}} | Rest], State, Acc) ->
    prepare_writes(Rest, State, [{From, {add_vector, Id, Text, Metadata, Vector}, prepared} | Acc]);
prepare_writes([{call, From, {add_batch, Docs}} | Rest], State, Acc) ->
    case prepare_batch_embeddings(Docs, State) of
        {ok, PreparedDocs} ->
            prepare_writes(Rest, State, [{From, {add_vector_batch, PreparedDocs}, prepared} | Acc]);
        {error, Reason} ->
            {error, From, Reason, lists:reverse(Acc)}
    end;
prepare_writes([{call, From, {add_vector_batch, Docs}} | Rest], State, Acc) ->
    prepare_writes(Rest, State, [{From, {add_vector_batch, Docs}, prepared} | Acc]).

%% Prepare embeddings for batch add
prepare_batch_embeddings(Docs, State) ->
    Texts = [Text || {_Id, Text, _Meta} <- Docs],
    case do_embed_batch(Texts, State) of
        {ok, Vectors} ->
            PreparedDocs = lists:zipwith(fun({Id, Text, Meta}, Vector) ->
                {Id, Text, Meta, Vector}
            end, Docs, Vectors),
            {ok, PreparedDocs};
        {error, Reason} ->
            {error, Reason}
    end.

%% Apply a single write operation to the batch
apply_write_to_batch({add_vector, Id, Text, Metadata, Vector}, _PreparedData,
                      Batch, CfV, CfM, CfT, _CfH, Index, Mod, Dim) ->
    case length(Vector) of
        Dim ->
            VectorBin = encode_vector(Vector),
            MetadataBin = term_to_binary(Metadata),
            ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
            ok = rocksdb:batch_put(Batch, CfM, Id, MetadataBin),
            ok = rocksdb:batch_put(Batch, CfT, Id, Text),
            %% Index updated in-memory only (rebuilt from vectors on startup)
            case Mod:insert(Index, Id, Vector) of
                {ok, NewIndex} -> {ok, NewIndex, ok};
                {error, Reason} -> {error, Reason}
            end;
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end;

apply_write_to_batch({add_vector_batch, Docs}, _PreparedData,
                      Batch, CfV, CfM, CfT, _CfH, Index, Mod, Dim) ->
    try
        {NewIndex, Count} = lists:foldl(fun({Id, Text, Meta, Vector}, {AccIndex, AccCount}) ->
            case length(Vector) of
                Dim ->
                    VectorBin = encode_vector(Vector),
                    MetadataBin = term_to_binary(Meta),
                    ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
                    ok = rocksdb:batch_put(Batch, CfM, Id, MetadataBin),
                    ok = rocksdb:batch_put(Batch, CfT, Id, Text),
                    %% Index updated in-memory only (rebuilt from vectors on startup)
                    case Mod:insert(AccIndex, Id, Vector) of
                        {ok, UpdatedIndex} -> {UpdatedIndex, AccCount + 1};
                        {error, Reason} -> throw({insert_error, Reason})
                    end;
                Other ->
                    throw({dimension_mismatch, Dim, Other})
            end
        end, {Index, 0}, Docs),
        {ok, NewIndex, {ok, #{inserted => Count}}}
    catch
        throw:{dimension_mismatch, Expected, Got} ->
            {error, {dimension_mismatch, Expected, Got}};
        throw:{insert_error, Reason} ->
            {error, Reason}
    end.

terminate(_Reason, #state{db = Db, index = Index, index_module = Mod, cf_hnsw = CfHnsw}) ->
    %% Persist index metadata before closing
    _ = persist_index_meta(Db, CfHnsw, Index, Mod),
    %% Close index if backend supports it (e.g., FAISS releases NIF resources)
    _ = maybe_close_index(Mod, Index),
    _ = rocksdb:close(Db),
    ok.

%% Close index if the backend module supports close/1
maybe_close_index(Mod, Index) ->
    case erlang:function_exported(Mod, close, 1) of
        true -> Mod:close(Index);
        false -> ok
    end.

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
load_or_create_index(Db, CfHandles, Dimension, IndexConfig, IndexModule) ->
    case IndexModule of
        barrel_vectordb_diskann ->
            %% DiskANN manages its own persistence - use open/new directly
            load_or_create_diskann(IndexConfig, Dimension);
        _ ->
            %% HNSW/FAISS - rebuild from vectors stored in RocksDB
            CfHnsw = maps:get(hnsw, CfHandles),
            CfVectors = maps:get(vectors, CfHandles),
            case load_index_meta(Db, CfHnsw) of
                {ok, _IndexMeta} ->
                    rebuild_from_vectors(Db, CfVectors, Dimension, IndexConfig, IndexModule);
                not_found ->
                    IndexModule:new(IndexConfig#{dimension => Dimension})
            end
    end.

%% DiskANN-specific loading: try open existing, else create new
load_or_create_diskann(Config, Dimension) ->
    BasePath = maps:get(base_path, Config),
    ok = filelib:ensure_dir(filename:join(BasePath, "dummy")),
    case barrel_vectordb_diskann:open(BasePath) of
        {ok, Index} ->
            {ok, Index};
        {error, _} ->
            %% No existing index or failed to open - create new
            barrel_vectordb_diskann:new(Config#{dimension => Dimension, storage_mode => disk})
    end.

%% Load index metadata from storage
load_index_meta(Db, CfHnsw) ->
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

%% Persist index metadata
persist_index_meta(Db, CfHnsw, Index, Mod) ->
    %% Use the index module's info function to get metadata
    Info = Mod:info(Index),
    Meta = #{
        size => maps:get(size, Info, Mod:size(Index)),
        dimension => maps:get(dimension, Info, undefined),
        backend => maps:get(backend, Info, hnsw)
    },
    Binary = term_to_binary(Meta),
    rocksdb:put(Db, CfHnsw, ?HNSW_META_KEY, Binary, []).

%% Rebuild index from stored vectors (index is in-memory only, rebuilt from RocksDB)
rebuild_from_vectors(Db, CfVectors, Dimension, IndexConfig, IndexModule) ->
    case IndexModule:new(IndexConfig#{dimension => Dimension}) of
        {ok, Index} ->
            case rocksdb:iterator(Db, CfVectors, []) of
                {ok, Iter} ->
                    try
                        rebuild_loop(Iter, rocksdb:iterator_move(Iter, first), Index, IndexModule)
                    after
                        rocksdb:iterator_close(Iter)
                    end;
                {error, _} ->
                    {ok, Index}
            end;
        {error, _} = Error ->
            Error
    end.

rebuild_loop(_Iter, {error, _}, Index, _Mod) ->
    {ok, Index};
rebuild_loop(Iter, {ok, Key, VectorBin}, Index, Mod) ->
    Vector = decode_vector(VectorBin),
    case Mod:insert(Index, Key, Vector) of
        {ok, NewIndex} ->
            rebuild_loop(Iter, rocksdb:iterator_move(Iter, next), NewIndex, Mod);
        {error, Reason} ->
            error_logger:warning_msg("Failed to insert vector ~p during rebuild: ~p~n",
                                     [Key, Reason]),
            rebuild_loop(Iter, rocksdb:iterator_move(Iter, next), Index, Mod)
    end.

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
                                          index = Index, index_module = Mod} = State) ->
    VectorBin = encode_vector(Vector),
    MetadataBin = term_to_binary(Metadata),

    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
    ok = rocksdb:batch_put(Batch, CfM, Id, MetadataBin),
    ok = rocksdb:batch_put(Batch, CfT, Id, Text),

    %% Index updated in-memory only (rebuilt from vectors on startup)
    case Mod:insert(Index, Id, Vector) of
        {ok, NewIndex} ->
            case rocksdb:write_batch(Db, Batch, []) of
                ok ->
                    {ok, State#state{index = NewIndex}};
                {error, Reason} ->
                    {error, {db_error, Reason}}
            end;
        {error, Reason} ->
            {error, Reason}
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
                     cf_text = CfT, cf_hnsw = CfH, index = Index, index_module = Mod} = State) ->
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_delete(Batch, CfV, Id),
    ok = rocksdb:batch_delete(Batch, CfM, Id),
    ok = rocksdb:batch_delete(Batch, CfT, Id),
    ok = rocksdb:batch_delete(Batch, CfH, Id),

    case rocksdb:write_batch(Db, Batch, []) of
        ok ->
            case Mod:delete(Index, Id) of
                {ok, NewIndex} -> {ok, State#state{index = NewIndex}};
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, {db_error, Reason}}
    end.

%% Peek at documents (sample without search)
%% Optimized: collect keys from iterator, then batch fetch metadata/text
do_peek(Limit, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM, cf_text = CfT}) ->
    case rocksdb:iterator(Db, CfV, []) of
        {ok, Iter} ->
            try
                %% Phase 1: Collect keys and vectors from iterator
                KeyVectors = collect_keys(Iter, rocksdb:iterator_move(Iter, first), Limit, []),
                case KeyVectors of
                    [] ->
                        {ok, []};
                    _ ->
                        %% Phase 2: Batch fetch metadata and text
                        Keys = [K || {K, _} <- KeyVectors],
                        MetaResults = rocksdb:multi_get(Db, CfM, Keys, []),
                        TextResults = rocksdb:multi_get(Db, CfT, Keys, []),
                        %% Phase 3: Combine results
                        Docs = combine_peek_results(KeyVectors, MetaResults, TextResults, []),
                        {ok, Docs}
                end
            after
                rocksdb:iterator_close(Iter)
            end;
        {error, _} ->
            {ok, []}
    end.

%% Collect keys and vector binaries from iterator
collect_keys(_Iter, _, 0, Acc) ->
    lists:reverse(Acc);
collect_keys(_Iter, {error, _}, _Limit, Acc) ->
    lists:reverse(Acc);
collect_keys(Iter, {ok, Key, VectorBin}, Limit, Acc) ->
    collect_keys(Iter, rocksdb:iterator_move(Iter, next), Limit - 1, [{Key, VectorBin} | Acc]).

%% Combine key-vectors with batched metadata/text results
combine_peek_results([], [], [], Acc) ->
    lists:reverse(Acc);
combine_peek_results([{Key, VectorBin} | KVs], [MetaResult | Ms], [TextResult | Ts], Acc) ->
    case {MetaResult, TextResult} of
        {{ok, MetadataBin}, {ok, Text}} ->
            Doc = #{
                key => Key,
                vector => decode_vector(VectorBin),
                metadata => binary_to_term(MetadataBin),
                text => Text
            },
            combine_peek_results(KVs, Ms, Ts, [Doc | Acc]);
        _ ->
            %% Skip documents with missing metadata or text
            combine_peek_results(KVs, Ms, Ts, Acc)
    end.

%% Search for similar documents
%% Options:
%%   - k: number of results (default 5)
%%   - filter: function to filter by metadata
%%   - include_text: include text in results (default true)
%%   - include_metadata: include metadata in results (default true)
do_search(QueryVector, Opts, #state{db = Db, index = Index, index_module = Mod,
                                     cf_metadata = CfM, cf_text = CfT}) ->
    K = maps:get(k, Opts, 5),
    IndexResults = Mod:search(Index, QueryVector, K, Opts),

    case IndexResults of
        [] ->
            {ok, []};
        _ ->
            %% Extract IDs and distances
            {Ids, Distances} = lists:unzip(IndexResults),

            %% Check what data to include
            IncludeText = maps:get(include_text, Opts, true),
            IncludeMeta = maps:get(include_metadata, Opts, true),
            Filter = maps:get(filter, Opts, fun(_) -> true end),

            %% Only fetch what's needed (skip lookups if not needed and no filter)
            NeedMeta = IncludeMeta orelse Filter =/= fun(_) -> true end,

            MetaResults = case NeedMeta of
                true -> rocksdb:multi_get(Db, CfM, Ids, []);
                false -> [not_needed || _ <- Ids]
            end,
            TextResults = case IncludeText of
                true -> rocksdb:multi_get(Db, CfT, Ids, []);
                false -> [not_needed || _ <- Ids]
            end,

            %% Combine results
            Results = combine_search_results(Ids, Distances, MetaResults, TextResults,
                                             Filter, IncludeText, IncludeMeta),
            {ok, Results}
    end.

%% Combine HNSW results with fetched metadata/text
combine_search_results(Ids, Distances, MetaResults, TextResults, Filter, IncludeText, IncludeMeta) ->
    combine_search_results(Ids, Distances, MetaResults, TextResults,
                           Filter, IncludeText, IncludeMeta, []).

combine_search_results([], [], [], [], _Filter, _IncludeText, _IncludeMeta, Acc) ->
    lists:reverse(Acc);
combine_search_results([Id | Ids], [Distance | Distances],
                       [MetaRes | MetaResults], [TextRes | TextResults],
                       Filter, IncludeText, IncludeMeta, Acc) ->
    %% Parse metadata if available
    Metadata = case MetaRes of
        {ok, MetadataBin} -> binary_to_term(MetadataBin);
        not_needed -> #{};
        _ -> undefined
    end,

    %% Parse text if available
    Text = case TextRes of
        {ok, TextBin} -> TextBin;
        not_needed -> undefined;
        _ -> undefined
    end,

    %% Check filter (skip if metadata couldn't be fetched for filtering)
    PassesFilter = case Metadata of
        undefined -> false;
        _ -> Filter(Metadata)
    end,

    case PassesFilter of
        true ->
            %% Build result map with only requested fields
            Result0 = #{key => Id, score => 1.0 - Distance},
            Result1 = case IncludeText of
                true when Text =/= undefined -> Result0#{text => Text};
                _ -> Result0
            end,
            Result2 = case IncludeMeta of
                true when Metadata =/= undefined -> Result1#{metadata => Metadata};
                _ -> Result1
            end,
            combine_search_results(Ids, Distances, MetaResults, TextResults,
                                   Filter, IncludeText, IncludeMeta, [Result2 | Acc]);
        false ->
            combine_search_results(Ids, Distances, MetaResults, TextResults,
                                   Filter, IncludeText, IncludeMeta, Acc)
    end.

%%====================================================================
%% Internal Functions - Encoding
%%====================================================================

%% Use 32-bit floats for 50% storage reduction (standard for similarity search)
encode_vector(Vector) when is_list(Vector) ->
    << <<F:32/float-little>> || F <- Vector >>.

decode_vector(Binary) ->
    [F || <<F:32/float-little>> <= Binary].
