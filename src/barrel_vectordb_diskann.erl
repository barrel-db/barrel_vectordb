%%%-------------------------------------------------------------------
%%% @doc DiskANN Vamana Graph Implementation
%%%
%%% Implements the Vamana graph algorithm from the DiskANN paper with:
%%% - Two-pass construction (alpha=1.0 then alpha>1.0)
%%% - RobustPrune for alpha-RNG pruning
%%% - GreedySearch for graph traversal
%%% - FreshVamana insert/delete for streaming updates
%%% - Consolidate deletes for batch cleanup
%%% - BeamSearch with PQ for SSD-resident search
%%%
%%% The alpha parameter (>1) is critical for maintaining graph quality
%%% under streaming updates. It keeps more long-range edges which
%%% are essential for fast convergence during search.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_diskann).

-include("barrel_vectordb.hrl").

%% API
-export([
    new/1,
    build/2,
    insert/3,
    delete/2,
    search/3,
    search/4,
    size/1,
    info/1,
    get_vector/2,
    consolidate_deletes/1,
    %% Persistence API
    open/1,
    close/1,
    sync/1,
    %% Serialization (for barrel_vectordb_index behaviour)
    serialize/1,
    deserialize/1
]).

%% Internal exports for testing
-export([
    greedy_search/5,
    robust_prune/5,
    find_medoid/2
]).

-record(diskann_config, {
    r = 64 :: pos_integer(),              %% Max out-degree
    l_build = 100 :: pos_integer(),       %% Build search width
    l_search = 100 :: pos_integer(),      %% Query search width
    alpha = 1.2 :: float(),               %% Pruning factor (>1 for long-range)
    dimension :: pos_integer(),
    distance_fn = cosine :: cosine | euclidean
}).

-record(diskann_index, {
    config :: #diskann_config{},
    size = 0 :: non_neg_integer(),
    medoid_id :: binary() | undefined,    %% Entry point (centroid)

    %% In RAM (small footprint) - graph structure
    nodes = #{} :: #{binary() => diskann_node()},

    %% ID <-> disk index mapping (for disk mode)
    id_to_idx = #{} :: #{binary() => non_neg_integer()},
    idx_to_id = #{} :: #{non_neg_integer() => binary()},

    %% PQ compression (always in RAM)
    pq_state :: term() | undefined,       %% Trained PQ for compression
    pq_codes = #{} :: #{binary() => binary()},  %% Id -> PQ code (M bytes)
    use_pq = false :: boolean(),          %% Whether to use PQ for search

    %% Deletion tracking
    deleted_set = sets:new() :: sets:set(binary()),

    %% Storage mode: memory | disk
    storage_mode = memory :: memory | disk,

    %% In-memory vectors (used only in memory mode)
    vectors = #{} :: #{binary() => [float()]},

    %% Disk file handle and path (used only in disk mode)
    file_handle :: term() | undefined,
    base_path :: binary() | undefined,

    %% Vector cache for disk mode (LRU eviction)
    vector_cache = #{} :: #{binary() => [float()]},
    cache_max_size = 10000 :: pos_integer(),
    cache_lru = [] :: [binary()]
}).

-record(diskann_node, {
    id :: binary(),
    neighbors = [] :: [binary()]
}).

-type diskann_index() :: #diskann_index{}.
-type diskann_node() :: #diskann_node{}.
-type diskann_config() :: #diskann_config{}.

-export_type([diskann_index/0, diskann_config/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new empty DiskANN index
%% Options:
%%   - dimension: Vector dimension (required)
%%   - r: Max out-degree (default: 64)
%%   - l_build: Build search width (default: 100)
%%   - l_search: Query search width (default: 100)
%%   - alpha: Pruning factor (default: 1.2)
%%   - distance_fn: cosine | euclidean (default: cosine)
%%   - storage_mode: memory | disk (default: memory)
%%   - base_path: Path for disk storage (required if storage_mode=disk)
%%   - cache_max_size: Max vectors in LRU cache (default: 10000)
-spec new(map()) -> {ok, diskann_index()} | {error, term()}.
new(Options) ->
    R = maps:get(r, Options, 64),
    LBuild = maps:get(l_build, Options, 100),
    LSearch = maps:get(l_search, Options, 100),
    Alpha = maps:get(alpha, Options, 1.2),
    Dimension = maps:get(dimension, Options, undefined),
    DistanceFn = maps:get(distance_fn, Options, cosine),
    StorageMode = maps:get(storage_mode, Options, memory),
    BasePath = maps:get(base_path, Options, undefined),
    CacheMaxSize = maps:get(cache_max_size, Options, 10000),

    case Dimension of
        undefined ->
            {error, dimension_required};
        _ when Dimension > 0 ->
            case validate_storage_options(StorageMode, BasePath) of
                ok ->
                    Config = #diskann_config{
                        r = R,
                        l_build = LBuild,
                        l_search = LSearch,
                        alpha = Alpha,
                        dimension = Dimension,
                        distance_fn = DistanceFn
                    },
                    {ok, #diskann_index{
                        config = Config,
                        storage_mode = StorageMode,
                        base_path = to_binary_or_undefined(BasePath),
                        cache_max_size = CacheMaxSize
                    }};
                {error, _} = Error ->
                    Error
            end;
        _ ->
            {error, {invalid_dimension, Dimension}}
    end.

validate_storage_options(disk, undefined) ->
    {error, {disk_mode_requires_base_path}};
validate_storage_options(_, _) ->
    ok.

to_binary_or_undefined(undefined) -> undefined;
to_binary_or_undefined(Path) when is_list(Path) -> list_to_binary(Path);
to_binary_or_undefined(Path) when is_binary(Path) -> Path.

%% @doc Build index from a list of {Id, Vector} pairs using two-pass Vamana
-spec build(map(), [{binary(), [float()]}]) -> {ok, diskann_index()} | {error, term()}.
build(Options, Vectors) when length(Vectors) > 0 ->
    case new(Options) of
        {ok, Index0} ->
            case Index0#diskann_index.storage_mode of
                memory ->
                    build_memory_mode(Index0, Options, Vectors);
                disk ->
                    build_disk_mode(Index0, Options, Vectors)
            end;
        {error, _} = Error ->
            Error
    end;
build(_, []) ->
    {error, empty_vectors}.

%% Build index in memory mode (original behavior)
build_memory_mode(Index0, Options, Vectors) ->
    %% Store all vectors in memory
    VectorMap = maps:from_list(Vectors),
    Index1 = Index0#diskann_index{
        vectors = VectorMap,
        size = length(Vectors)
    },

    %% Build id to idx mappings (for consistency with disk mode)
    {IdToIdx, IdxToId} = build_id_mappings(Vectors),
    Index1b = Index1#diskann_index{
        id_to_idx = IdToIdx,
        idx_to_id = IdxToId
    },

    %% Find medoid (centroid) as entry point
    MedoidId = find_medoid(Vectors, Index1b#diskann_index.config),
    Index2 = Index1b#diskann_index{medoid_id = MedoidId},

    %% Initialize random graph
    Index3 = init_random_graph(Index2, maps:keys(VectorMap)),

    %% Two-pass Vamana construction
    Config = Index3#diskann_index.config,
    R = Config#diskann_config.r,
    L = Config#diskann_config.l_build,
    Alpha = Config#diskann_config.alpha,

    %% Pass 1: alpha = 1.0 (finds good short edges)
    Index4 = vamana_pass(Index3, 1.0, L, R),

    %% Pass 2: alpha > 1.0 (adds long-range edges for fast convergence)
    Index5 = vamana_pass(Index4, Alpha, L, R),

    %% Train and apply PQ if enabled and enough vectors
    UsePQ = maps:get(use_pq, Options, false),
    PQK = maps:get(pq_k, Options, 256),
    Index6 = case UsePQ andalso length(Vectors) >= PQK of
        true ->
            train_and_apply_pq(Index5, Options, Vectors);
        false ->
            Index5
    end,

    {ok, Index6}.

%% Build index in disk mode (vectors stored on SSD)
build_disk_mode(Index0, Options, Vectors) ->
    BasePath = Index0#diskann_index.base_path,
    Config = Index0#diskann_index.config,
    Dimension = Config#diskann_config.dimension,

    %% Create disk files
    FileConfig = #{
        dimension => Dimension,
        r => Config#diskann_config.r,
        distance_fn => Config#diskann_config.distance_fn
    },
    case barrel_vectordb_diskann_file:create(BasePath, FileConfig) of
        {ok, FileHandle0} ->
            %% Build id to idx mappings and write vectors to disk
            {IdToIdx, IdxToId, FileHandle1} = write_vectors_to_disk(Vectors, FileHandle0),

            %% Update header with entry point (will be set after finding medoid)
            {ok, FileHandle2} = barrel_vectordb_diskann_file:write_header(
                FileHandle1,
                #{
                    dimension => Dimension,
                    r => Config#diskann_config.r,
                    node_count => length(Vectors),
                    distance_fn => Config#diskann_config.distance_fn,
                    entry_point => undefined
                }
            ),

            %% During build, we need vectors in cache for distance calculations
            VectorMap = maps:from_list(Vectors),
            Index1 = Index0#diskann_index{
                file_handle = FileHandle2,
                id_to_idx = IdToIdx,
                idx_to_id = IdxToId,
                size = length(Vectors),
                %% Temporarily put all vectors in cache for build
                vector_cache = VectorMap,
                cache_lru = maps:keys(VectorMap)
            },

            %% Find medoid
            MedoidId = find_medoid(Vectors, Config),
            Index2 = Index1#diskann_index{medoid_id = MedoidId},

            %% Initialize random graph
            Index3 = init_random_graph(Index2, maps:keys(VectorMap)),

            %% Two-pass Vamana construction
            R = Config#diskann_config.r,
            L = Config#diskann_config.l_build,
            Alpha = Config#diskann_config.alpha,

            Index4 = vamana_pass(Index3, 1.0, L, R),
            Index5 = vamana_pass(Index4, Alpha, L, R),

            %% Train and apply PQ (required for efficient disk-based search)
            UsePQ = maps:get(use_pq, Options, true),  %% Default to true for disk mode
            PQK = maps:get(pq_k, Options, 256),
            Index6 = case UsePQ andalso length(Vectors) >= min(PQK, 16) of
                true ->
                    train_and_apply_pq(Index5, Options, Vectors);
                false ->
                    Index5
            end,

            %% Update header with medoid
            {ok, FileHandle3} = barrel_vectordb_diskann_file:write_header(
                Index6#diskann_index.file_handle,
                #{
                    dimension => Dimension,
                    r => Config#diskann_config.r,
                    node_count => length(Vectors),
                    distance_fn => Config#diskann_config.distance_fn,
                    entry_point => MedoidId
                }
            ),

            %% Clear the build cache, keep only limited cache for search
            CacheMaxSize = Index6#diskann_index.cache_max_size,
            Index7 = Index6#diskann_index{
                file_handle = FileHandle3,
                vectors = #{},  %% Clear vectors from memory
                vector_cache = #{},  %% Clear build cache
                cache_lru = []
            },

            %% Pre-warm cache with medoid neighbors
            Index8 = prewarm_cache(Index7, MedoidId, CacheMaxSize),

            {ok, Index8};

        {error, Reason} ->
            {error, {disk_file_create_failed, Reason}}
    end.

%% Write all vectors to disk and build mappings
write_vectors_to_disk(Vectors, FileHandle) ->
    {IdToIdx, IdxToId, FinalHandle} = lists:foldl(
        fun({Id, Vec}, {AccIdToIdx, AccIdxToId, AccHandle}) ->
            Idx = maps:size(AccIdToIdx),
            ok = barrel_vectordb_diskann_file:write_vector(AccHandle, Idx, Vec),
            {AccIdToIdx#{Id => Idx}, AccIdxToId#{Idx => Id}, AccHandle}
        end,
        {#{}, #{}, FileHandle},
        Vectors
    ),
    {IdToIdx, IdxToId, FinalHandle}.

%% Build id <-> idx mappings from vector list
build_id_mappings(Vectors) ->
    {IdToIdx, IdxToId, _} = lists:foldl(
        fun({Id, _Vec}, {AccIdToIdx, AccIdxToId, Idx}) ->
            {AccIdToIdx#{Id => Idx}, AccIdxToId#{Idx => Id}, Idx + 1}
        end,
        {#{}, #{}, 0},
        Vectors
    ),
    {IdToIdx, IdxToId}.

%% Pre-warm cache with vectors near the medoid
prewarm_cache(Index, MedoidId, MaxSize) ->
    %% Get medoid neighbors
    Neighbors = get_neighbors(Index, MedoidId),
    %% Load medoid and its neighbors into cache
    ToLoad = lists:sublist([MedoidId | Neighbors], MaxSize),
    lists:foldl(
        fun(Id, AccIndex) ->
            {_Vec, NewIndex} = get_vector_cached(AccIndex, Id),
            NewIndex
        end,
        Index,
        ToLoad
    ).

%% @doc Insert a new vector (FreshVamana algorithm)
-spec insert(diskann_index(), binary(), [float()]) -> {ok, diskann_index()} | {error, term()}.
insert(#diskann_index{medoid_id = undefined, config = Config} = Index, Id, Vector) ->
    %% First insertion
    Dim = Config#diskann_config.dimension,
    case length(Vector) of
        Dim ->
            case Index#diskann_index.storage_mode of
                memory ->
                    %% Memory mode: store vector in RAM
                    NewIndex = Index#diskann_index{
                        medoid_id = Id,
                        nodes = #{Id => #diskann_node{id = Id, neighbors = []}},
                        vectors = #{Id => Vector},
                        id_to_idx = #{Id => 0},
                        idx_to_id = #{0 => Id},
                        size = 1
                    },
                    {ok, NewIndex};
                disk ->
                    %% Disk mode: write to disk and cache
                    case ensure_disk_files(Index) of
                        {ok, Index1} ->
                            Idx = 0,
                            ok = barrel_vectordb_diskann_file:write_vector(
                                Index1#diskann_index.file_handle, Idx, Vector),
                            Index2 = add_to_cache(Index1, Id, Vector),
                            NewIndex = Index2#diskann_index{
                                medoid_id = Id,
                                nodes = #{Id => #diskann_node{id = Id, neighbors = []}},
                                id_to_idx = #{Id => Idx},
                                idx_to_id = #{Idx => Id},
                                size = 1
                            },
                            {ok, NewIndex};
                        {error, _} = Error ->
                            Error
                    end
            end;
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end;
insert(#diskann_index{config = Config, medoid_id = S, use_pq = UsePQ,
                      pq_state = PQState, pq_codes = PQCodes,
                      storage_mode = StorageMode} = Index, Id, Vector) ->
    Dim = Config#diskann_config.dimension,
    case length(Vector) of
        Dim ->
            #diskann_config{l_build = L, alpha = Alpha, r = R} = Config,

            %% Encode with PQ if enabled
            NewPQCodes = case UsePQ andalso PQState =/= undefined of
                true ->
                    Code = barrel_vectordb_pq:encode(PQState, Vector),
                    maps:put(Id, Code, PQCodes);
                false ->
                    PQCodes
            end,

            %% Add vector to storage (memory or disk)
            Index1 = case StorageMode of
                memory ->
                    Index#diskann_index{
                        vectors = maps:put(Id, Vector, Index#diskann_index.vectors),
                        nodes = maps:put(Id, #diskann_node{id = Id, neighbors = []},
                                         Index#diskann_index.nodes),
                        id_to_idx = maps:put(Id, Index#diskann_index.size,
                                             Index#diskann_index.id_to_idx),
                        idx_to_id = maps:put(Index#diskann_index.size, Id,
                                             Index#diskann_index.idx_to_id),
                        pq_codes = NewPQCodes,
                        size = Index#diskann_index.size + 1
                    };
                disk ->
                    Idx = Index#diskann_index.size,
                    ok = barrel_vectordb_diskann_file:write_vector(
                        Index#diskann_index.file_handle, Idx, Vector),
                    %% Add to cache for the insert operation
                    Index0 = add_to_cache(Index, Id, Vector),
                    Index0#diskann_index{
                        nodes = maps:put(Id, #diskann_node{id = Id, neighbors = []},
                                         Index0#diskann_index.nodes),
                        id_to_idx = maps:put(Id, Idx, Index0#diskann_index.id_to_idx),
                        idx_to_id = maps:put(Idx, Id, Index0#diskann_index.idx_to_id),
                        pq_codes = NewPQCodes,
                        size = Index0#diskann_index.size + 1
                    }
            end,

            %% Search to find candidate neighbors
            {_Results, Visited} = greedy_search(Index1, S, Vector, 1, L),

            %% Prune to select R out-neighbors
            Index2 = robust_prune(Index1, Id, sets:to_list(Visited), Alpha, R),
            Neighbors = get_neighbors(Index2, Id),

            %% Add backward edges (critical for navigability)
            Index3 = lists:foldl(
                fun(J, AccIndex) ->
                    JNeighbors = get_neighbors(AccIndex, J),
                    case length(JNeighbors) + 1 > R of
                        true ->
                            %% Prune if degree exceeded
                            robust_prune(AccIndex, J, [Id | JNeighbors], Alpha, R);
                        false ->
                            add_neighbor(AccIndex, J, Id)
                    end
                end,
                Index2,
                Neighbors
            ),

            {ok, Index3};
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end.

%% Ensure disk files are created (for incremental insert in disk mode)
ensure_disk_files(#diskann_index{file_handle = undefined, base_path = BasePath,
                                  config = Config} = Index) when BasePath =/= undefined ->
    FileConfig = #{
        dimension => Config#diskann_config.dimension,
        r => Config#diskann_config.r,
        distance_fn => Config#diskann_config.distance_fn
    },
    case barrel_vectordb_diskann_file:create(BasePath, FileConfig) of
        {ok, FileHandle} ->
            {ok, Index#diskann_index{file_handle = FileHandle}};
        {error, _} = Error ->
            Error
    end;
ensure_disk_files(#diskann_index{file_handle = Handle} = Index) when Handle =/= undefined ->
    {ok, Index};
ensure_disk_files(_Index) ->
    {error, disk_mode_requires_base_path}.

%% @doc Lazy delete - marks node as deleted
-spec delete(diskann_index(), binary()) -> {ok, diskann_index()}.
delete(Index, Id) ->
    NewDeleted = sets:add_element(Id, Index#diskann_index.deleted_set),
    {ok, Index#diskann_index{deleted_set = NewDeleted}}.

%% @doc Search for K nearest neighbors
-spec search(diskann_index(), [float()], pos_integer()) -> [{binary(), float()}].
search(Index, Query, K) ->
    search(Index, Query, K, #{}).

%% @doc Search with options
-spec search(diskann_index(), [float()], pos_integer(), map()) -> [{binary(), float()}].
search(#diskann_index{medoid_id = undefined}, _Query, _K, _Opts) ->
    [];
search(#diskann_index{medoid_id = S, config = Config, deleted_set = DeletedSet,
                      use_pq = UsePQ, pq_state = PQState} = Index,
       Query, K, Opts) ->
    L = maps:get(l_search, Opts, Config#diskann_config.l_search),

    %% Use PQ-based beam search if PQ is available
    Results = case UsePQ andalso PQState =/= undefined of
        true ->
            %% Precompute distance tables for this query
            DistTables = barrel_vectordb_pq:precompute_tables(PQState, Query),
            beam_search_pq(Index, S, Query, DistTables, K * 2, L);
        false ->
            %% Standard greedy search
            {Res, _Visited} = greedy_search(Index, S, Query, K * 2, L),
            Res
    end,

    %% Filter deleted nodes
    Filtered = [{D, Id} || {D, Id} <- Results,
                           not sets:is_element(Id, DeletedSet)],

    %% Return top K
    TopK = lists:sublist(Filtered, K),
    [{Id, D} || {D, Id} <- TopK].

%% @doc Get index size (excluding deleted)
-spec size(diskann_index()) -> non_neg_integer().
size(#diskann_index{size = Size, deleted_set = Deleted}) ->
    Size - sets:size(Deleted).

%% @doc Get index info
-spec info(diskann_index()) -> map().
info(#diskann_index{config = Config, size = Size, medoid_id = Medoid,
                    deleted_set = Deleted, nodes = Nodes, use_pq = UsePQ,
                    pq_state = PQState, pq_codes = PQCodes,
                    storage_mode = StorageMode, base_path = BasePath,
                    vector_cache = VectorCache, cache_max_size = CacheMaxSize}) ->
    AvgDegree = case maps:size(Nodes) of
        0 -> 0.0;
        N ->
            TotalDegree = maps:fold(
                fun(_, #diskann_node{neighbors = Ns}, Acc) ->
                    Acc + length(Ns)
                end,
                0,
                Nodes
            ),
            TotalDegree / N
    end,
    PQInfo = case UsePQ andalso PQState =/= undefined of
        true ->
            #{
                enabled => true,
                m => barrel_vectordb_pq:info(PQState),
                num_codes => maps:size(PQCodes)
            };
        false ->
            #{enabled => false}
    end,
    StorageInfo = #{
        mode => StorageMode,
        base_path => BasePath,
        cache_size => maps:size(VectorCache),
        cache_max_size => CacheMaxSize
    },
    #{
        size => Size,
        active_size => Size - sets:size(Deleted),
        deleted_count => sets:size(Deleted),
        medoid => Medoid,
        avg_degree => AvgDegree,
        pq => PQInfo,
        storage => StorageInfo,
        config => #{
            r => Config#diskann_config.r,
            l_build => Config#diskann_config.l_build,
            l_search => Config#diskann_config.l_search,
            alpha => Config#diskann_config.alpha,
            dimension => Config#diskann_config.dimension,
            distance_fn => Config#diskann_config.distance_fn
        }
    }.

%% @doc Get vector by ID (uses cache for disk mode)
-spec get_vector(diskann_index(), binary()) -> {ok, [float()]} | not_found.
get_vector(Index, Id) ->
    {Vec, _UpdatedIndex} = get_vector_or_cached(Index, Id),
    case Vec of
        undefined -> not_found;
        _ -> {ok, Vec}
    end.

%% @doc Consolidate deleted nodes (batch cleanup)
%% This repairs the graph by removing edges to deleted nodes
%% and adding new edges to maintain navigability
-spec consolidate_deletes(diskann_index()) -> {ok, diskann_index()}.
consolidate_deletes(#diskann_index{deleted_set = DeletedSet, config = Config,
                                   nodes = Nodes, vectors = Vectors} = Index) ->
    case sets:size(DeletedSet) of
        0 ->
            {ok, Index};
        _ ->
            consolidate_deletes_impl(Index, DeletedSet, Config, Nodes, Vectors)
    end.

consolidate_deletes_impl(Index, DeletedSet, Config, Nodes, Vectors) ->
    #diskann_config{alpha = Alpha, r = R} = Config,

    %% For each node with edges to deleted nodes, repair neighborhood
    UpdatedNodes = maps:fold(
        fun(P, #diskann_node{neighbors = Neighbors} = Node, AccNodes) ->
            case sets:is_element(P, DeletedSet) of
                true ->
                    %% Skip deleted nodes
                    AccNodes;
                false ->
                    DeletedNeighbors = [N || N <- Neighbors,
                                             sets:is_element(N, DeletedSet)],
                    case DeletedNeighbors of
                        [] ->
                            AccNodes#{P => Node};
                        _ ->
                            %% Repair: find new candidates from deleted nodes' neighbors
                            SurvivingNeighbors = Neighbors -- DeletedNeighbors,
                            DeletedOutNeighbors = lists:flatmap(
                                fun(V) ->
                                    case maps:find(V, Nodes) of
                                        {ok, #diskann_node{neighbors = VNs}} ->
                                            [N || N <- VNs,
                                                  not sets:is_element(N, DeletedSet)];
                                        error -> []
                                    end
                                end,
                                DeletedNeighbors
                            ),
                            Candidates = lists:usort(SurvivingNeighbors ++ DeletedOutNeighbors) -- [P],

                            %% Re-prune with alpha
                            NewNeighbors = prune_neighbors(
                                Index, P, Candidates, Alpha, R
                            ),
                            AccNodes#{P => Node#diskann_node{neighbors = NewNeighbors}}
                    end
            end
        end,
        #{},
        Nodes
    ),

    %% Remove deleted nodes from index
    NewVectors = maps:without(sets:to_list(DeletedSet), Vectors),
    NewSize = maps:size(UpdatedNodes),

    %% Update medoid if it was deleted
    NewMedoid = case sets:is_element(Index#diskann_index.medoid_id, DeletedSet) of
        true ->
            %% Pick new medoid from remaining nodes
            case maps:keys(UpdatedNodes) of
                [] -> undefined;
                [First | _] -> First
            end;
        false ->
            Index#diskann_index.medoid_id
    end,

    {ok, Index#diskann_index{
        nodes = UpdatedNodes,
        vectors = NewVectors,
        deleted_set = sets:new(),
        size = NewSize,
        medoid_id = NewMedoid
    }}.

%%====================================================================
%% Persistence API
%%====================================================================

%% @doc Open an existing DiskANN index from disk
%% Reconstructs in-memory structures (graph, PQ codes) from disk files
-spec open(binary() | string()) -> {ok, diskann_index()} | {error, term()}.
open(BasePath) ->
    BasePathBin = to_binary_or_undefined(BasePath),
    case barrel_vectordb_diskann_file:open(BasePathBin) of
        {ok, FileHandle} ->
            Header = barrel_vectordb_diskann_file:read_header(FileHandle),
            rebuild_index_from_disk(FileHandle, Header, BasePathBin);
        {error, _} = Error ->
            Error
    end.

%% Rebuild the in-memory index from disk files
rebuild_index_from_disk(FileHandle, Header, BasePath) ->
    Dimension = maps:get(dimension, Header, 128),
    R = maps:get(r, Header, 64),
    NodeCount = maps:get(node_count, Header, 0),
    MedoidId = maps:get(entry_point, Header, undefined),
    DistFn = maps:get(distance_fn, Header, cosine),

    Config = #diskann_config{
        dimension = Dimension,
        r = R,
        distance_fn = DistFn
    },

    %% Read graph from disk (would need to implement graph reading in diskann_file)
    %% For now, we rebuild from the metadata file if available
    MetaPath = filename:join(BasePath, "diskann.index"),
    Index0 = case file:read_file(MetaPath) of
        {ok, MetaBin} ->
            %% Load serialized index state
            try binary_to_term(MetaBin) of
                SerializedIndex ->
                    SerializedIndex#diskann_index{
                        file_handle = FileHandle,
                        base_path = BasePath,
                        storage_mode = disk
                    }
            catch
                _:_ ->
                    %% Fallback to empty index with header info
                    create_empty_disk_index(Config, FileHandle, BasePath, MedoidId, NodeCount)
            end;
        {error, _} ->
            %% No serialized state, create basic index
            create_empty_disk_index(Config, FileHandle, BasePath, MedoidId, NodeCount)
    end,

    {ok, Index0}.

create_empty_disk_index(Config, FileHandle, BasePath, MedoidId, NodeCount) ->
    #diskann_index{
        config = Config,
        file_handle = FileHandle,
        base_path = BasePath,
        storage_mode = disk,
        medoid_id = MedoidId,
        size = NodeCount,
        nodes = #{},
        id_to_idx = #{},
        idx_to_id = #{}
    }.

%% @doc Close the index and flush to disk
-spec close(diskann_index()) -> ok.
close(#diskann_index{file_handle = undefined}) ->
    ok;
close(#diskann_index{file_handle = FileHandle, base_path = BasePath,
                     storage_mode = disk} = Index) ->
    %% Save index metadata
    save_index_metadata(Index),
    %% Close file handles
    barrel_vectordb_diskann_file:close(FileHandle),
    %% Clear base_path reference
    case BasePath of
        undefined -> ok;
        _ -> ok
    end,
    ok;
close(_Index) ->
    ok.

%% @doc Force sync to disk
-spec sync(diskann_index()) -> ok.
sync(#diskann_index{file_handle = undefined}) ->
    ok;
sync(#diskann_index{file_handle = FileHandle} = Index) ->
    save_index_metadata(Index),
    barrel_vectordb_diskann_file:sync(FileHandle),
    ok.

%% Save index metadata (graph, PQ state, mappings) to disk
save_index_metadata(#diskann_index{base_path = undefined}) ->
    ok;
save_index_metadata(#diskann_index{base_path = BasePath} = Index) ->
    MetaPath = filename:join(BasePath, "diskann.index"),
    %% Clear file handle before serializing (can't serialize file handles)
    IndexToSave = Index#diskann_index{
        file_handle = undefined,
        vector_cache = #{},  %% Don't persist cache
        cache_lru = []
    },
    file:write_file(MetaPath, term_to_binary(IndexToSave)).

%% @doc Serialize index to binary (for barrel_vectordb_index behaviour)
-spec serialize(diskann_index()) -> binary().
serialize(#diskann_index{storage_mode = memory} = Index) ->
    %% Memory mode: serialize entire index including vectors
    term_to_binary(Index);
serialize(#diskann_index{storage_mode = disk} = Index) ->
    %% Disk mode: serialize only in-memory structures (no vectors, no cache)
    IndexToSave = Index#diskann_index{
        file_handle = undefined,
        vectors = #{},
        vector_cache = #{},
        cache_lru = []
    },
    term_to_binary(IndexToSave).

%% @doc Deserialize index from binary
-spec deserialize(binary()) -> {ok, diskann_index()} | {error, term()}.
deserialize(Binary) ->
    try
        case binary_to_term(Binary) of
            #diskann_index{} = Index ->
                %% Re-open disk files if in disk mode
                case Index#diskann_index.storage_mode of
                    disk ->
                        BasePath = Index#diskann_index.base_path,
                        case BasePath of
                            undefined ->
                                {ok, Index};
                            _ ->
                                case barrel_vectordb_diskann_file:open(BasePath) of
                                    {ok, FileHandle} ->
                                        {ok, Index#diskann_index{file_handle = FileHandle}};
                                    {error, _} ->
                                        %% Files might not exist yet
                                        {ok, Index}
                                end
                        end;
                    memory ->
                        {ok, Index}
                end;
            _ ->
                {error, invalid_format}
        end
    catch
        _:_ ->
            {error, invalid_binary}
    end.

%%====================================================================
%% Internal: PQ Training and Encoding
%%====================================================================

%% Train PQ on vectors and encode all vectors to PQ codes
train_and_apply_pq(Index, Options, Vectors) ->
    Dim = (Index#diskann_index.config)#diskann_config.dimension,
    M = maps:get(pq_m, Options, 8),
    K = maps:get(pq_k, Options, 256),

    %% Dimension must be divisible by M
    case Dim rem M of
        0 ->
            %% Create and train PQ
            {ok, PQConfig} = barrel_vectordb_pq:new(#{
                m => M,
                k => K,
                dimension => Dim
            }),
            VecList = [V || {_, V} <- Vectors],
            {ok, TrainedPQ} = barrel_vectordb_pq:train(PQConfig, VecList),

            %% Encode all vectors to PQ codes
            PQCodes = maps:from_list([
                {Id, barrel_vectordb_pq:encode(TrainedPQ, Vec)}
                || {Id, Vec} <- Vectors
            ]),

            Index#diskann_index{
                pq_state = TrainedPQ,
                pq_codes = PQCodes,
                use_pq = true
            };
        _ ->
            %% Can't use PQ with this dimension
            Index
    end.

%%====================================================================
%% Internal: Vamana Build
%%====================================================================

%% Find medoid (vector closest to centroid)
find_medoid(Vectors, #diskann_config{dimension = Dim}) ->
    %% Compute centroid
    N = length(Vectors),
    Centroid = lists:foldl(
        fun({_Id, Vec}, Acc) ->
            [A + V || {A, V} <- lists:zip(Acc, Vec)]
        end,
        [0.0 || _ <- lists:seq(1, Dim)],
        Vectors
    ),
    NormCentroid = [C / N || C <- Centroid],

    %% Find closest to centroid
    {MedoidId, _MinDist} = lists:foldl(
        fun({Id, Vec}, {BestId, BestDist}) ->
            Dist = euclidean_distance(Vec, NormCentroid),
            case Dist < BestDist of
                true -> {Id, Dist};
                false -> {BestId, BestDist}
            end
        end,
        {undefined, infinity},
        Vectors
    ),
    MedoidId.

%% Initialize random R-regular graph
init_random_graph(#diskann_index{config = Config} = Index, Ids) ->
    R = Config#diskann_config.r,
    N = length(Ids),
    IdsArray = list_to_tuple(Ids),

    Nodes = lists:foldl(
        fun(Id, Acc) ->
            %% Pick R random neighbors (excluding self)
            Neighbors = random_neighbors(Id, IdsArray, N, R),
            Acc#{Id => #diskann_node{id = Id, neighbors = Neighbors}}
        end,
        #{},
        Ids
    ),
    Index#diskann_index{nodes = Nodes}.

random_neighbors(Id, IdsArray, N, R) ->
    NumNeighbors = min(R, N - 1),
    random_neighbors(Id, IdsArray, N, NumNeighbors, []).

random_neighbors(_Id, _IdsArray, _N, 0, Acc) ->
    Acc;
random_neighbors(Id, IdsArray, N, Remaining, Acc) ->
    Idx = rand:uniform(N),
    Neighbor = element(Idx, IdsArray),
    case Neighbor =:= Id orelse lists:member(Neighbor, Acc) of
        true ->
            random_neighbors(Id, IdsArray, N, Remaining, Acc);
        false ->
            random_neighbors(Id, IdsArray, N, Remaining - 1, [Neighbor | Acc])
    end.

%% Single pass of Vamana construction
vamana_pass(#diskann_index{medoid_id = S, nodes = Nodes} = Index,
            Alpha, L, R) ->
    %% Random permutation of all node IDs
    Ids = maps:keys(Nodes),
    Sigma = shuffle(Ids),

    lists:foldl(
        fun(Id, AccIndex) ->
            %% Use get_vector_or_cached to support both memory and disk modes
            {Vec, _} = get_vector_or_cached(AccIndex, Id),

            %% Search to find candidates
            {_Results, Visited} = greedy_search(AccIndex, S, Vec, 1, L),

            %% Prune to select R out-neighbors
            AccIndex2 = robust_prune(AccIndex, Id, sets:to_list(Visited), Alpha, R),
            Neighbors = get_neighbors(AccIndex2, Id),

            %% Add backward edges (bidirectional)
            lists:foldl(
                fun(J, AccInner) ->
                    JNeighbors = get_neighbors(AccInner, J),
                    case length(JNeighbors) + 1 > R of
                        true ->
                            robust_prune(AccInner, J, [Id | JNeighbors], Alpha, R);
                        false ->
                            add_neighbor(AccInner, J, Id)
                    end
                end,
                AccIndex2,
                Neighbors
            )
        end,
        Index,
        Sigma
    ).

shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), N} || N <- List])].

%%====================================================================
%% Internal: GreedySearch (Algorithm 1 from DiskANN paper)
%%====================================================================

%% @doc Core search algorithm - optimized version
%% Uses separate candidate queue and result set for efficiency
%% Returns ({SortedResults, VisitedSet})
greedy_search(Index, StartId, Query, K, L) ->
    StartDist = distance(Index, StartId, Query),
    %% Candidates: min-heap of nodes to explore
    Candidates = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Results: best L nodes found so far
    Results = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Visited: nodes we've already expanded
    Visited = sets:from_list([StartId]),
    %% Track furthest result distance for pruning
    FurthestDist = StartDist,

    greedy_loop(Index, Query, Candidates, Results, Visited, FurthestDist, K, L).

greedy_loop(_Index, _Query, {0, nil}, Results, Visited, _FurthestDist, K, _L) ->
    %% No more candidates
    ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
    TopK = lists:sublist(ResultList, K),
    {TopK, Visited};
greedy_loop(Index, Query, Candidates, Results, Visited, FurthestDist, K, L) ->
    %% Get closest candidate
    {{CurrentDist, CurrentId}, _, RestCandidates} = gb_trees:take_smallest(Candidates),

    %% If closest candidate is further than our furthest result, we're done
    case CurrentDist > FurthestDist of
        true ->
            ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
            TopK = lists:sublist(ResultList, K),
            {TopK, Visited};
        false ->
            %% Expand neighbors
            Neighbors = get_neighbors(Index, CurrentId),
            {NewCandidates, NewResults, NewVisited, NewFurthestDist} = lists:foldl(
                fun(N, {CandAcc, ResAcc, VisAcc, FurthAcc}) ->
                    case sets:is_element(N, VisAcc) of
                        true ->
                            {CandAcc, ResAcc, VisAcc, FurthAcc};
                        false ->
                            NewVisAcc = sets:add_element(N, VisAcc),
                            D = distance(Index, N, Query),
                            ResSize = gb_trees:size(ResAcc),
                            ShouldAdd = D < FurthAcc orelse ResSize < L,
                            case ShouldAdd of
                                true ->
                                    NewCandAcc = gb_trees:insert({D, N}, true, CandAcc),
                                    NewResAcc0 = gb_trees:insert({D, N}, true, ResAcc),
                                    %% Trim results if too many
                                    NewResSize = gb_trees:size(NewResAcc0),
                                    {NewResAcc, NewFurthAcc} = case NewResSize > L of
                                        true ->
                                            {_, _, Trimmed} = gb_trees:take_largest(NewResAcc0),
                                            {{LastD, _}, _} = gb_trees:largest(Trimmed),
                                            {Trimmed, LastD};
                                        false ->
                                            {{MaxD, _}, _} = gb_trees:largest(NewResAcc0),
                                            {NewResAcc0, MaxD}
                                    end,
                                    {NewCandAcc, NewResAcc, NewVisAcc, NewFurthAcc};
                                false ->
                                    {CandAcc, ResAcc, NewVisAcc, FurthAcc}
                            end
                    end
                end,
                {RestCandidates, Results, Visited, FurthestDist},
                Neighbors
            ),

            greedy_loop(Index, Query, NewCandidates, NewResults, NewVisited, NewFurthestDist, K, L)
    end.

%%====================================================================
%% Internal: BeamSearch with PQ (Optimized for DiskANN)
%%====================================================================

%% Beam search using PQ distance tables for fast approximate distance
%% Returns sorted results [{Distance, Id}]
beam_search_pq(Index, StartId, Query, DistTables, K, L) ->
    #diskann_index{pq_codes = PQCodes} = Index,

    %% Get PQ distance to start node
    StartDist = case maps:find(StartId, PQCodes) of
        {ok, Code} -> barrel_vectordb_pq:distance(DistTables, Code);
        error -> infinity
    end,

    %% Candidates: min-heap of nodes to explore
    Candidates = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Results: best L nodes found so far
    Results = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Visited: nodes we've already expanded
    Visited = sets:from_list([StartId]),
    FurthestDist = StartDist,

    %% Run beam search with PQ distances
    PQResults = beam_search_pq_loop(Index, DistTables, PQCodes, Candidates,
                                     Results, Visited, FurthestDist, K, L),

    %% Rerank top results with full vectors for accuracy (using lazy disk loading)
    rerank_with_full_vectors(PQResults, Query, Index, K).

beam_search_pq_loop(_Index, _DistTables, _PQCodes, {0, nil}, Results, _Visited, _FurthestDist, K, _L) ->
    %% No more candidates
    ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
    lists:sublist(ResultList, K);
beam_search_pq_loop(Index, DistTables, PQCodes, Candidates, Results, Visited, FurthestDist, K, L) ->
    %% Get closest candidate
    {{CurrentDist, CurrentId}, _, RestCandidates} = gb_trees:take_smallest(Candidates),

    %% If closest candidate is further than our furthest result, we're done
    case CurrentDist > FurthestDist of
        true ->
            ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
            lists:sublist(ResultList, K);
        false ->
            %% Expand neighbors
            Neighbors = get_neighbors(Index, CurrentId),
            {NewCandidates, NewResults, NewVisited, NewFurthestDist} = lists:foldl(
                fun(N, {CandAcc, ResAcc, VisAcc, FurthAcc}) ->
                    case sets:is_element(N, VisAcc) of
                        true ->
                            {CandAcc, ResAcc, VisAcc, FurthAcc};
                        false ->
                            NewVisAcc = sets:add_element(N, VisAcc),
                            %% Use PQ distance (fast O(M) lookup)
                            D = case maps:find(N, PQCodes) of
                                {ok, Code} -> barrel_vectordb_pq:distance(DistTables, Code);
                                error -> infinity
                            end,
                            ResSize = gb_trees:size(ResAcc),
                            ShouldAdd = D < FurthAcc orelse ResSize < L,
                            case ShouldAdd of
                                true ->
                                    NewCandAcc = gb_trees:insert({D, N}, true, CandAcc),
                                    NewResAcc0 = gb_trees:insert({D, N}, true, ResAcc),
                                    %% Trim results if too many
                                    NewResSize = gb_trees:size(NewResAcc0),
                                    {NewResAcc, NewFurthAcc} = case NewResSize > L of
                                        true ->
                                            {_, _, Trimmed} = gb_trees:take_largest(NewResAcc0),
                                            {{LastD, _}, _} = gb_trees:largest(Trimmed),
                                            {Trimmed, LastD};
                                        false ->
                                            {{MaxD, _}, _} = gb_trees:largest(NewResAcc0),
                                            {NewResAcc0, MaxD}
                                    end,
                                    {NewCandAcc, NewResAcc, NewVisAcc, NewFurthAcc};
                                false ->
                                    {CandAcc, ResAcc, NewVisAcc, FurthAcc}
                            end
                    end
                end,
                {RestCandidates, Results, Visited, FurthestDist},
                Neighbors
            ),

            beam_search_pq_loop(Index, DistTables, PQCodes, NewCandidates,
                                NewResults, NewVisited, NewFurthestDist, K, L)
    end.

%% Rerank top candidates using full vectors for accuracy
%% Now uses Index for lazy vector loading from disk
rerank_with_full_vectors(PQResults, Query, Index, K) ->
    Config = Index#diskann_index.config,
    %% Compute exact distances for top candidates
    Reranked = lists:map(
        fun({_PQDist, Id}) ->
            {Vec, _} = get_vector_or_cached(Index, Id),
            case Vec of
                undefined ->
                    {infinity, Id};
                _ ->
                    ExactDist = distance_vec(Config, Query, Vec),
                    {ExactDist, Id}
            end
        end,
        PQResults
    ),
    %% Sort by exact distance and take K
    Sorted = lists:sort(Reranked),
    lists:sublist(Sorted, K).

%%====================================================================
%% Internal: RobustPrune (Algorithm 2 from DiskANN paper)
%%====================================================================

%% @doc RobustPrune: Select R neighbors for node P using alpha-RNG pruning
%% V = candidate neighbors
%% Alpha > 1 keeps more long-range edges
%% Optimized: cache distance computations
robust_prune(Index, P, V, Alpha, R) ->
    %% V <- (V ∪ N_out(P)) \ {P}
    CurrentNeighbors = get_neighbors(Index, P),
    Candidates = lists:usort(V ++ CurrentNeighbors) -- [P],

    %% Pre-compute distances from P to all candidates (cache)
    {PVec, _} = get_vector_or_cached(Index, P),
    Config = Index#diskann_index.config,

    %% Build list with cached distances: [{Dist, Id, Vec}]
    CandidatesWithDist = lists:filtermap(
        fun(C) ->
            {CVec, _} = get_vector_or_cached(Index, C),
            case CVec of
                undefined -> false;
                _ -> {true, {distance_vec(Config, PVec, CVec), C, CVec}}
            end
        end,
        Candidates
    ),

    %% Sort by distance to P
    SortedCandidates = lists:sort(fun({D1, _, _}, {D2, _, _}) -> D1 =< D2 end, CandidatesWithDist),

    %% Prune with cached data
    NewNeighbors = prune_loop_cached(Config, PVec, SortedCandidates, Alpha, R, []),

    %% Update node
    set_neighbors(Index, P, NewNeighbors).

%% Alpha-RNG pruning with cached vectors - avoids repeated map lookups
prune_loop_cached(_Config, _PVec, [], _Alpha, _R, Acc) ->
    lists:reverse(Acc);
prune_loop_cached(_Config, _PVec, _Candidates, _Alpha, R, Acc) when length(Acc) >= R ->
    lists:reverse(Acc);
prune_loop_cached(Config, PVec, [{_Dist, PStar, PStarVec} | Rest], Alpha, R, Acc) ->
    %% Add p* to neighbors
    NewAcc = [PStar | Acc],

    %% Filter out candidates that are closer to p* than to P (with alpha factor)
    FilteredRest = lists:filter(
        fun({DistP_PPrime, _PPrime, PPrimeVec}) ->
            DistPStar_PPrime = distance_vec(Config, PStarVec, PPrimeVec),
            %% Keep p' only if alpha * d(p*, p') > d(p, p')
            Alpha * DistPStar_PPrime > DistP_PPrime
        end,
        Rest
    ),

    prune_loop_cached(Config, PVec, FilteredRest, Alpha, R, NewAcc).

%% Original prune_neighbors kept for compatibility with consolidate_deletes
prune_neighbors(Index, P, Candidates, Alpha, R) ->
    {PVec, _} = get_vector_or_cached(Index, P),
    Config = Index#diskann_index.config,
    CandidatesWithDist = lists:filtermap(
        fun(C) ->
            {CVec, _} = get_vector_or_cached(Index, C),
            case CVec of
                undefined -> false;
                _ -> {true, {distance_vec(Config, PVec, CVec), C, CVec}}
            end
        end,
        Candidates
    ),
    SortedCandidates = lists:sort(fun({D1, _, _}, {D2, _, _}) -> D1 =< D2 end, CandidatesWithDist),
    prune_loop_cached(Config, PVec, SortedCandidates, Alpha, R, []).

%%====================================================================
%% Internal: Vector Cache (LRU) - for disk mode
%%====================================================================

%% Get vector: check cache/memory first, load from disk if needed
get_vector_cached(#diskann_index{storage_mode = memory, vectors = Vectors} = Index, Id) ->
    %% Memory mode: vectors are in the vectors map
    case maps:find(Id, Vectors) of
        {ok, Vec} -> {Vec, Index};
        error -> {undefined, Index}
    end;
get_vector_cached(#diskann_index{storage_mode = disk} = Index, Id) ->
    %% Disk mode: check cache first
    case maps:find(Id, Index#diskann_index.vector_cache) of
        {ok, Vec} ->
            %% Cache hit - touch to update LRU
            {Vec, touch_cache(Index, Id)};
        error ->
            %% Cache miss - load from disk
            load_and_cache_vector(Index, Id)
    end.

%% Load vector from disk and add to cache
load_and_cache_vector(#diskann_index{file_handle = undefined} = Index, _Id) ->
    %% No file handle - can't load
    {undefined, Index};
load_and_cache_vector(Index, Id) ->
    case maps:find(Id, Index#diskann_index.id_to_idx) of
        {ok, Idx} ->
            case barrel_vectordb_diskann_file:read_vector(
                    Index#diskann_index.file_handle, Idx) of
                {ok, Vec} ->
                    Index2 = add_to_cache(Index, Id, Vec),
                    {Vec, Index2};
                {error, _} ->
                    {undefined, Index}
            end;
        error ->
            {undefined, Index}
    end.

%% Add vector to LRU cache with eviction if full
add_to_cache(#diskann_index{vector_cache = Cache, cache_lru = LRU,
                            cache_max_size = MaxSize} = Index, Id, Vec) ->
    %% Check if already in cache
    case maps:is_key(Id, Cache) of
        true ->
            %% Already cached, just update LRU order
            touch_cache(Index#diskann_index{vector_cache = Cache#{Id => Vec}}, Id);
        false ->
            %% Need to add new entry
            {NewCache, NewLRU} = case maps:size(Cache) >= MaxSize of
                true ->
                    %% Evict oldest entry
                    case lists:reverse(LRU) of
                        [] ->
                            {Cache, []};
                        [Oldest | RestReversed] ->
                            {maps:remove(Oldest, Cache), lists:reverse(RestReversed)}
                    end;
                false ->
                    {Cache, LRU}
            end,
            Index#diskann_index{
                vector_cache = NewCache#{Id => Vec},
                cache_lru = [Id | NewLRU]
            }
    end.

%% Touch cache entry to mark as recently used
touch_cache(#diskann_index{cache_lru = LRU} = Index, Id) ->
    NewLRU = [Id | lists:delete(Id, LRU)],
    Index#diskann_index{cache_lru = NewLRU}.

%% Get vector with fallback (for internal use during build/pruning)
get_vector_or_cached(Index, Id) ->
    case Index#diskann_index.storage_mode of
        memory ->
            case maps:find(Id, Index#diskann_index.vectors) of
                {ok, Vec} -> {Vec, Index};
                error ->
                    %% Fallback to cache (during transition)
                    case maps:find(Id, Index#diskann_index.vector_cache) of
                        {ok, Vec} -> {Vec, Index};
                        error -> {undefined, Index}
                    end
            end;
        disk ->
            get_vector_cached(Index, Id)
    end.

%%====================================================================
%% Internal: Distance Functions
%%====================================================================

%% Distance using lazy vector loading
distance(Index, Id, QueryVec) ->
    {NodeVec, _Index2} = get_vector_or_cached(Index, Id),
    case NodeVec of
        undefined -> infinity;
        _ -> distance_vec(Index#diskann_index.config, QueryVec, NodeVec)
    end.

distance_vec(#diskann_config{distance_fn = cosine}, Vec1, Vec2) ->
    cosine_distance(Vec1, Vec2);
distance_vec(#diskann_config{distance_fn = euclidean}, Vec1, Vec2) ->
    euclidean_distance(Vec1, Vec2).

cosine_distance(Vec1, Vec2) ->
    Dot = dot_product(Vec1, Vec2),
    Norm1 = math:sqrt(dot_product(Vec1, Vec1)),
    Norm2 = math:sqrt(dot_product(Vec2, Vec2)),
    Denom = Norm1 * Norm2,
    case Denom < 1.0e-10 of
        true -> 1.0;
        false -> 1.0 - (Dot / Denom)
    end.

euclidean_distance(Vec1, Vec2) ->
    SumSq = lists:sum([math:pow(A - B, 2) || {A, B} <- lists:zip(Vec1, Vec2)]),
    math:sqrt(SumSq).

dot_product(Vec1, Vec2) ->
    lists:sum([A * B || {A, B} <- lists:zip(Vec1, Vec2)]).

%%====================================================================
%% Internal: Graph Operations
%%====================================================================

get_neighbors(#diskann_index{nodes = Nodes}, Id) ->
    case maps:find(Id, Nodes) of
        {ok, #diskann_node{neighbors = Ns}} -> Ns;
        error -> []
    end.

set_neighbors(#diskann_index{nodes = Nodes} = Index, Id, Neighbors) ->
    case maps:find(Id, Nodes) of
        {ok, Node} ->
            NewNode = Node#diskann_node{neighbors = Neighbors},
            Index#diskann_index{nodes = Nodes#{Id => NewNode}};
        error ->
            Index
    end.

add_neighbor(#diskann_index{nodes = Nodes} = Index, NodeId, NewNeighborId) ->
    case maps:find(NodeId, Nodes) of
        {ok, #diskann_node{neighbors = Ns} = Node} ->
            case lists:member(NewNeighborId, Ns) of
                true -> Index;
                false ->
                    NewNode = Node#diskann_node{neighbors = [NewNeighborId | Ns]},
                    Index#diskann_index{nodes = Nodes#{NodeId => NewNode}}
            end;
        error ->
            Index
    end.
