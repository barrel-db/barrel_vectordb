%%%-------------------------------------------------------------------
%%% @doc Per-store gen_batch_server managing RocksDB, HNSW index, and embeddings
%%%
%%% Each store runs as a separate gen_batch_server registered under its name.
%%% Handles all document operations, search, and embedding coordination.
%%%
%%% Uses gen_batch_server to automatically batch concurrent write operations
%%% into single atomic RocksDB WriteBatch operations, improving throughput
%%% under concurrent load.
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
    embedder_info/1
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
    gen_batch_server:start_link({local, Name}, ?MODULE, {Name, Config}, []).

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

%%====================================================================
%% gen_batch_server callbacks
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

process_single_op({call, From, stats}, #state{hnsw_index = Index, dimension = Dim, config = Config} = State) ->
    Stats = #{
        dimension => Dim,
        count => barrel_vectordb_hnsw:size(Index),
        hnsw => barrel_vectordb_hnsw:info(Index),
        config => Config
    },
    {{reply, From, {ok, Stats}}, State};

process_single_op({call, From, count}, #state{hnsw_index = Index} = State) ->
    {{reply, From, barrel_vectordb_hnsw:size(Index)}, State};

process_single_op({call, From, embedder_info}, #state{embed_state = EmbedState} = State) ->
    Info = barrel_vectordb_embed:info(EmbedState),
    {{reply, From, {ok, Info}}, State};

process_single_op({call, From, _Unknown}, State) ->
    {{reply, From, {error, unknown_request}}, State}.

%% Process write operations atomically in a single RocksDB batch
process_writes_atomic([], State) ->
    {[], State};
process_writes_atomic(Writes, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM,
                                      cf_text = CfT, cf_hnsw = CfH,
                                      hnsw_index = Index, dimension = Dim} = State) ->
    {ok, Batch} = rocksdb:batch(),

    %% First, prepare all embeddings if needed (this is done before batch to avoid
    %% partial writes on embedding failures)
    case prepare_writes(Writes, State) of
        {ok, PreparedWrites} ->
            %% Now apply all writes to the batch
            {NewIndex, Replies} = lists:foldl(fun({From, WriteOp, PreparedData}, {AccIndex, AccReplies}) ->
                case apply_write_to_batch(WriteOp, PreparedData, Batch, CfV, CfM, CfT, CfH, AccIndex, Dim) of
                    {ok, UpdatedIndex, Reply} ->
                        {UpdatedIndex, [{reply, From, Reply} | AccReplies]};
                    {error, Reason} ->
                        {AccIndex, [{reply, From, {error, Reason}} | AccReplies]}
                end
            end, {Index, []}, PreparedWrites),

            %% Commit the batch atomically
            case rocksdb:write_batch(Db, Batch, []) of
                ok ->
                    {Replies, State#state{hnsw_index = NewIndex}};
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
                      Batch, CfV, CfM, CfT, CfH, Index, Dim) ->
    case length(Vector) of
        Dim ->
            VectorBin = encode_vector(Vector),
            MetadataBin = term_to_binary(Metadata),
            ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
            ok = rocksdb:batch_put(Batch, CfM, Id, MetadataBin),
            ok = rocksdb:batch_put(Batch, CfT, Id, Text),
            NewIndex = barrel_vectordb_hnsw:insert(Index, Id, Vector),
            case barrel_vectordb_hnsw:get_node(NewIndex, Id) of
                {ok, Node} ->
                    NodeBin = barrel_vectordb_hnsw:serialize_node(Node),
                    ok = rocksdb:batch_put(Batch, CfH, Id, NodeBin);
                not_found ->
                    ok
            end,
            {ok, NewIndex, ok};
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end;

apply_write_to_batch({add_vector_batch, Docs}, _PreparedData,
                      Batch, CfV, CfM, CfT, CfH, Index, Dim) ->
    try
        {NewIndex, Count} = lists:foldl(fun({Id, Text, Meta, Vector}, {AccIndex, AccCount}) ->
            case length(Vector) of
                Dim ->
                    VectorBin = encode_vector(Vector),
                    MetadataBin = term_to_binary(Meta),
                    ok = rocksdb:batch_put(Batch, CfV, Id, VectorBin),
                    ok = rocksdb:batch_put(Batch, CfM, Id, MetadataBin),
                    ok = rocksdb:batch_put(Batch, CfT, Id, Text),
                    UpdatedIndex = barrel_vectordb_hnsw:insert(AccIndex, Id, Vector),
                    case barrel_vectordb_hnsw:get_node(UpdatedIndex, Id) of
                        {ok, Node} ->
                            NodeBin = barrel_vectordb_hnsw:serialize_node(Node),
                            ok = rocksdb:batch_put(Batch, CfH, Id, NodeBin);
                        not_found ->
                            ok
                    end,
                    {UpdatedIndex, AccCount + 1};
                Other ->
                    throw({dimension_mismatch, Dim, Other})
            end
        end, {Index, 0}, Docs),
        {ok, NewIndex, {ok, #{inserted => Count}}}
    catch
        throw:{dimension_mismatch, Expected, Got} ->
            {error, {dimension_mismatch, Expected, Got}}
    end.

terminate(_Reason, #state{db = Db, hnsw_index = Index, cf_hnsw = CfHnsw}) ->
    %% Persist HNSW index metadata before closing
    _ = persist_hnsw_meta(Db, CfHnsw, Index),
    _ = rocksdb:close(Db),
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

    _ = case barrel_vectordb_hnsw:get_node(NewIndex, Key) of
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

    %% Update HNSW index in memory first
    NewIndex = barrel_vectordb_hnsw:insert(Index, Id, Vector),

    %% Add HNSW node to the same batch (atomic write)
    _ = case barrel_vectordb_hnsw:get_node(NewIndex, Id) of
        {ok, Node} ->
            NodeBin = barrel_vectordb_hnsw:serialize_node(Node),
            rocksdb:batch_put(Batch, CfH, Id, NodeBin);
        not_found ->
            ok
    end,

    case rocksdb:write_batch(Db, Batch, []) of
        ok ->
            {ok, State#state{hnsw_index = NewIndex}};
        {error, Reason} ->
            {error, {db_error, Reason}}
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

%% Peek at documents (sample without search)
do_peek(Limit, #state{db = Db, cf_vectors = CfV, cf_metadata = CfM, cf_text = CfT}) ->
    case rocksdb:iterator(Db, CfV, []) of
        {ok, Iter} ->
            try
                Docs = peek_loop(Db, Iter, rocksdb:iterator_move(Iter, first),
                                 CfM, CfT, Limit, []),
                {ok, Docs}
            after
                rocksdb:iterator_close(Iter)
            end;
        {error, _} ->
            {ok, []}
    end.

peek_loop(_Db, _Iter, _, _CfM, _CfT, 0, Acc) ->
    lists:reverse(Acc);
peek_loop(_Db, _Iter, {error, _}, _CfM, _CfT, _Limit, Acc) ->
    lists:reverse(Acc);
peek_loop(Db, Iter, {ok, Key, VectorBin}, CfM, CfT, Limit, Acc) ->
    case {rocksdb:get(Db, CfM, Key, []), rocksdb:get(Db, CfT, Key, [])} of
        {{ok, MetadataBin}, {ok, Text}} ->
            Doc = #{
                key => Key,
                vector => decode_vector(VectorBin),
                metadata => binary_to_term(MetadataBin),
                text => Text
            },
            peek_loop(Db, Iter, rocksdb:iterator_move(Iter, next),
                      CfM, CfT, Limit - 1, [Doc | Acc]);
        _ ->
            peek_loop(Db, Iter, rocksdb:iterator_move(Iter, next),
                      CfM, CfT, Limit, Acc)
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
    << <<F:64/float-little>> || F <- Vector >>.

decode_vector(Binary) ->
    [F || <<F:64/float-little>> <= Binary].
