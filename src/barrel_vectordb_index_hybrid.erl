%%%-------------------------------------------------------------------
%%% @doc Hybrid DiskANN + HNSW Index
%%%
%%% Implements a two-layer hybrid index:
%%% - Hot layer: Small in-memory HNSW for recent writes (~10K vectors)
%%% - Cold layer: DiskANN Vamana graph for bulk storage (millions)
%%%
%%% This architecture provides:
%%% - Fast inserts (~1ms) by routing to hot HNSW
%%% - Low memory footprint (~64-128 bytes/vector in cold layer)
%%% - Good recall by merging results from both layers
%%% - Background compaction to move hot vectors to cold
%%%
%%% Based on FreshDiskANN paper architecture.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_index_hybrid).

-behaviour(barrel_vectordb_index).

%% barrel_vectordb_index behaviour callbacks
-export([
    new/1,
    insert/3,
    search/3,
    search/4,
    delete/2,
    size/1,
    info/1,
    serialize/1,
    deserialize/1
]).

%% Additional API
-export([
    should_compact/1,
    compact/1,
    extract_all_vectors/1
]).

-record(hybrid_index, {
    hot_layer :: term(),                      %% HNSW index for recent writes
    cold_layer :: term() | undefined,         %% DiskANN index for bulk storage
    dimension :: pos_integer(),
    distance_fn = cosine :: cosine | euclidean,
    hot_capacity = 10000 :: pos_integer(),    %% Max vectors in hot layer
    hot_age_threshold_ms = 3600000 :: pos_integer(), %% 1 hour
    hot_timestamps = #{} :: #{binary() => integer()}, %% Id -> insert timestamp
    pending_deletes = sets:new() :: sets:set(binary()),
    config :: map()                           %% Original config for compaction
}).

-type hybrid_index() :: #hybrid_index{}.
-export_type([hybrid_index/0]).

%%====================================================================
%% barrel_vectordb_index Behaviour Callbacks
%%====================================================================

%% @doc Create a new hybrid index
-spec new(map()) -> {ok, hybrid_index()} | {error, term()}.
new(Config) ->
    Dimension = maps:get(dimension, Config, undefined),
    case Dimension of
        undefined ->
            {error, dimension_required};
        _ when Dimension > 0 ->
            DistanceFn = maps:get(distance_fn, Config, cosine),
            HotCapacity = maps:get(hot_capacity, Config, 10000),
            HotAgeThreshold = maps:get(hot_age_threshold_ms, Config, 3600000),

            %% Create hot HNSW layer
            HnswConfig = #{
                dimension => Dimension,
                distance_fn => DistanceFn,
                m => maps:get(hnsw_m, Config, 16),
                m_max0 => maps:get(hnsw_m_max0, Config, 32),
                ef_construction => maps:get(hnsw_ef_construction, Config, 200)
            },
            HotLayer = barrel_vectordb_hnsw:new(HnswConfig),

            %% Cold layer starts empty (created on first compaction)
            {ok, #hybrid_index{
                hot_layer = HotLayer,
                cold_layer = undefined,
                dimension = Dimension,
                distance_fn = DistanceFn,
                hot_capacity = HotCapacity,
                hot_age_threshold_ms = HotAgeThreshold,
                config = Config
            }};
        _ ->
            {error, {invalid_dimension, Dimension}}
    end.

%% @doc Insert a vector into the hot layer
-spec insert(hybrid_index(), binary(), [float()]) -> {ok, hybrid_index()} | {error, term()}.
insert(#hybrid_index{hot_layer = Hot, hot_timestamps = Timestamps,
                     pending_deletes = Deletes} = Index, Id, Vector) ->
    %% Insert into hot HNSW layer
    NewHot = barrel_vectordb_hnsw:insert(Hot, Id, Vector),

    %% Track timestamp for age-based compaction
    Now = erlang:system_time(millisecond),
    NewTimestamps = Timestamps#{Id => Now},

    %% Remove from pending deletes if it was there
    NewDeletes = sets:del_element(Id, Deletes),

    {ok, Index#hybrid_index{
        hot_layer = NewHot,
        hot_timestamps = NewTimestamps,
        pending_deletes = NewDeletes
    }}.

%% @doc Search both layers and merge results
-spec search(hybrid_index(), [float()], pos_integer()) -> [{binary(), float()}].
search(Index, Query, K) ->
    search(Index, Query, K, #{}).

%% @doc Search with options
-spec search(hybrid_index(), [float()], pos_integer(), map()) -> [{binary(), float()}].
search(#hybrid_index{hot_layer = Hot, cold_layer = Cold,
                     pending_deletes = Deletes}, Query, K, Opts) ->
    %% Search hot layer
    HotResults = barrel_vectordb_hnsw:search(Hot, Query, K, Opts),

    %% Search cold layer (if exists)
    ColdResults = case Cold of
        undefined -> [];
        _ -> barrel_vectordb_diskann:search(Cold, Query, K, Opts)
    end,

    %% Merge and filter deleted
    merge_results(HotResults, ColdResults, Deletes, K).

%% @doc Delete a vector (lazy delete)
-spec delete(hybrid_index(), binary()) -> {ok, hybrid_index()} | {error, term()}.
delete(#hybrid_index{hot_layer = Hot, hot_timestamps = Timestamps,
                     pending_deletes = Deletes, cold_layer = Cold} = Index, Id) ->
    %% Check if vector is in hot layer
    InHot = maps:is_key(Id, Timestamps),

    %% Remove from hot layer if present
    NewHot = barrel_vectordb_hnsw:delete(Hot, Id),
    NewTimestamps = maps:remove(Id, Timestamps),

    %% Add to pending deletes ONLY if not in hot layer and cold layer exists
    %% (i.e., the vector must be in the cold layer)
    NewDeletes = case {InHot, Cold} of
        {true, _} -> Deletes;      %% Was in hot, don't add to pending
        {false, undefined} -> Deletes;  %% No cold layer
        {false, _} -> sets:add_element(Id, Deletes)  %% Must be in cold
    end,

    {ok, Index#hybrid_index{
        hot_layer = NewHot,
        hot_timestamps = NewTimestamps,
        pending_deletes = NewDeletes
    }}.

%% @doc Get total size (hot + cold - deleted)
-spec size(hybrid_index()) -> non_neg_integer().
size(#hybrid_index{hot_layer = Hot, cold_layer = Cold, pending_deletes = Deletes}) ->
    HotSize = barrel_vectordb_hnsw:size(Hot),
    ColdSize = case Cold of
        undefined -> 0;
        _ -> barrel_vectordb_diskann:size(Cold)
    end,
    HotSize + ColdSize - sets:size(Deletes).

%% @doc Get index info
-spec info(hybrid_index()) -> map().
info(#hybrid_index{hot_layer = Hot, cold_layer = Cold, pending_deletes = Deletes,
                   hot_capacity = HotCapacity, dimension = Dim, distance_fn = DistFn}) ->
    HotInfo = barrel_vectordb_hnsw:info(Hot),
    ColdInfo = case Cold of
        undefined -> #{size => 0, active_size => 0};
        _ -> barrel_vectordb_diskann:info(Cold)
    end,
    #{
        hot_size => maps:get(size, HotInfo, 0),
        cold_size => maps:get(active_size, ColdInfo, 0),
        total_size => maps:get(size, HotInfo, 0) + maps:get(active_size, ColdInfo, 0),
        pending_deletes => sets:size(Deletes),
        hot_capacity => HotCapacity,
        dimension => Dim,
        distance_fn => DistFn,
        hot_info => HotInfo,
        cold_info => ColdInfo
    }.

%% @doc Serialize hybrid index
-spec serialize(hybrid_index()) -> binary().
serialize(#hybrid_index{hot_layer = Hot, cold_layer = Cold, dimension = Dim,
                        distance_fn = DistFn, hot_capacity = HotCapacity,
                        hot_age_threshold_ms = HotAge, hot_timestamps = Timestamps,
                        pending_deletes = Deletes, config = Config}) ->
    Version = 1,
    HotBin = barrel_vectordb_hnsw:serialize(Hot),
    ColdBin = case Cold of
        undefined -> <<0:8>>;
        _ ->
            %% Serialize DiskANN as Erlang term for now
            ColdTerm = term_to_binary(Cold),
            <<1:8, (byte_size(ColdTerm)):32, ColdTerm/binary>>
    end,
    DistFnInt = case DistFn of cosine -> 0; euclidean -> 1 end,
    TimestampBin = term_to_binary(Timestamps),
    DeletesBin = term_to_binary(sets:to_list(Deletes)),
    ConfigBin = term_to_binary(Config),
    <<Version:8, Dim:16, DistFnInt:8, HotCapacity:32, HotAge:32,
      (byte_size(HotBin)):32, HotBin/binary,
      ColdBin/binary,
      (byte_size(TimestampBin)):32, TimestampBin/binary,
      (byte_size(DeletesBin)):32, DeletesBin/binary,
      (byte_size(ConfigBin)):32, ConfigBin/binary>>.

%% @doc Deserialize hybrid index
-spec deserialize(binary()) -> {ok, hybrid_index()} | {error, term()}.
deserialize(<<1:8, Dim:16, DistFnInt:8, HotCapacity:32, HotAge:32,
              HotLen:32, HotBin:HotLen/binary, Rest/binary>>) ->
    try
        {ok, Hot} = barrel_vectordb_hnsw:deserialize(HotBin),
        {Cold, Rest2} = deserialize_cold(Rest),
        <<TsLen:32, TsBin:TsLen/binary, Rest3/binary>> = Rest2,
        Timestamps = binary_to_term(TsBin),
        <<DelLen:32, DelBin:DelLen/binary, Rest4/binary>> = Rest3,
        DeletesList = binary_to_term(DelBin),
        <<CfgLen:32, CfgBin:CfgLen/binary>> = Rest4,
        Config = binary_to_term(CfgBin),
        DistFn = case DistFnInt of 0 -> cosine; 1 -> euclidean end,
        {ok, #hybrid_index{
            hot_layer = Hot,
            cold_layer = Cold,
            dimension = Dim,
            distance_fn = DistFn,
            hot_capacity = HotCapacity,
            hot_age_threshold_ms = HotAge,
            hot_timestamps = Timestamps,
            pending_deletes = sets:from_list(DeletesList),
            config = Config
        }}
    catch
        _:Reason -> {error, {deserialization_failed, Reason}}
    end;
deserialize(_) ->
    {error, invalid_format}.

%%====================================================================
%% Additional API
%%====================================================================

%% @doc Check if compaction should be triggered
-spec should_compact(hybrid_index()) -> boolean().
should_compact(#hybrid_index{hot_layer = Hot, hot_capacity = Capacity,
                             hot_timestamps = Timestamps,
                             hot_age_threshold_ms = AgeThreshold}) ->
    HotSize = barrel_vectordb_hnsw:size(Hot),

    %% Trigger on capacity
    CapacityReached = HotSize >= Capacity,

    %% Trigger on age (if oldest vector is too old)
    Now = erlang:system_time(millisecond),
    AgeReached = case maps:size(Timestamps) of
        0 -> false;
        _ ->
            OldestTs = lists:min(maps:values(Timestamps)),
            (Now - OldestTs) >= AgeThreshold
    end,

    CapacityReached orelse AgeReached.

%% @doc Compact hot layer into cold layer (StreamingMerge)
-spec compact(hybrid_index()) -> {ok, hybrid_index()} | {error, term()}.
compact(#hybrid_index{hot_layer = Hot, cold_layer = Cold,
                      pending_deletes = Deletes, config = Config,
                      dimension = Dim, distance_fn = DistFn} = Index) ->
    %% Extract all vectors from hot layer
    HotVectors = extract_all_vectors_from_hnsw(Hot),

    case {HotVectors, Cold} of
        {[], undefined} ->
            %% Nothing to compact
            {ok, Index};

        {[], _} ->
            %% Only need to consolidate deletes in cold layer
            case sets:size(Deletes) of
                0 ->
                    {ok, Index};
                _ ->
                    %% Apply pending deletes to cold layer
                    Cold2 = apply_deletes_to_cold(Cold, Deletes),
                    {ok, Cold3} = barrel_vectordb_diskann:consolidate_deletes(Cold2),
                    {ok, Index#hybrid_index{
                        cold_layer = Cold3,
                        pending_deletes = sets:new()
                    }}
            end;

        {Vectors, undefined} ->
            %% First compaction: build new DiskANN index
            DiskannConfig = #{
                dimension => Dim,
                distance_fn => DistFn,
                r => maps:get(diskann_r, Config, 64),
                l_build => maps:get(diskann_l_build, Config, 100),
                l_search => maps:get(diskann_l_search, Config, 100),
                alpha => maps:get(diskann_alpha, Config, 1.2)
            },
            {ok, NewCold} = barrel_vectordb_diskann:build(DiskannConfig, Vectors),

            %% Create fresh hot layer
            HnswConfig = #{
                dimension => Dim,
                distance_fn => DistFn,
                m => maps:get(hnsw_m, Config, 16),
                m_max0 => maps:get(hnsw_m_max0, Config, 32),
                ef_construction => maps:get(hnsw_ef_construction, Config, 200)
            },
            NewHot = barrel_vectordb_hnsw:new(HnswConfig),

            {ok, Index#hybrid_index{
                hot_layer = NewHot,
                cold_layer = NewCold,
                hot_timestamps = #{},
                pending_deletes = sets:new()
            }};

        {Vectors, _} ->
            %% Incremental merge: insert hot vectors into cold
            %% First apply pending deletes
            Cold2 = apply_deletes_to_cold(Cold, Deletes),
            {ok, Cold3} = barrel_vectordb_diskann:consolidate_deletes(Cold2),

            %% Insert hot vectors into cold
            NewCold = lists:foldl(
                fun({Id, Vec}, AccCold) ->
                    {ok, Updated} = barrel_vectordb_diskann:insert(AccCold, Id, Vec),
                    Updated
                end,
                Cold3,
                Vectors
            ),

            %% Create fresh hot layer
            HnswConfig = #{
                dimension => Dim,
                distance_fn => DistFn,
                m => maps:get(hnsw_m, Config, 16),
                m_max0 => maps:get(hnsw_m_max0, Config, 32),
                ef_construction => maps:get(hnsw_ef_construction, Config, 200)
            },
            NewHot = barrel_vectordb_hnsw:new(HnswConfig),

            {ok, Index#hybrid_index{
                hot_layer = NewHot,
                cold_layer = NewCold,
                hot_timestamps = #{},
                pending_deletes = sets:new()
            }}
    end.

%% @doc Extract all vectors from hybrid index (for external use)
-spec extract_all_vectors(hybrid_index()) -> [{binary(), [float()]}].
extract_all_vectors(#hybrid_index{hot_layer = Hot, cold_layer = Cold,
                                  pending_deletes = Deletes}) ->
    HotVectors = extract_all_vectors_from_hnsw(Hot),

    ColdVectors = case Cold of
        undefined -> [];
        _ -> extract_all_vectors_from_diskann(Cold, Deletes)
    end,

    %% Merge (hot vectors override cold for same ID)
    HotIds = sets:from_list([Id || {Id, _} <- HotVectors]),
    FilteredCold = [{Id, V} || {Id, V} <- ColdVectors,
                               not sets:is_element(Id, HotIds)],
    HotVectors ++ FilteredCold.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Merge results from hot and cold layers, filter deleted, return top K
merge_results(HotResults, ColdResults, Deletes, K) ->
    %% Convert to common format {Distance, Id}
    Hot = [{D, Id} || {Id, D} <- HotResults],
    Cold = [{D, Id} || {Id, D} <- ColdResults],

    %% Merge by ID (prefer hot layer for duplicates)
    HotIds = sets:from_list([Id || {_D, Id} <- Hot]),
    FilteredCold = [{D, Id} || {D, Id} <- Cold,
                               not sets:is_element(Id, HotIds)],

    %% Combine and filter deleted
    Combined = Hot ++ FilteredCold,
    Filtered = [{D, Id} || {D, Id} <- Combined,
                           not sets:is_element(Id, Deletes)],

    %% Sort by distance and take K
    Sorted = lists:sort(Filtered),
    TopK = lists:sublist(Sorted, K),

    %% Return in {Id, Distance} format
    [{Id, D} || {D, Id} <- TopK].

%% Extract all vectors from HNSW index
extract_all_vectors_from_hnsw(HnswIndex) ->
    Info = barrel_vectordb_hnsw:info(HnswIndex),
    case maps:get(size, Info, 0) of
        0 -> [];
        _ ->
            %% Access the internal nodes map
            %% This is a bit of a hack - ideally HNSW would export this
            extract_hnsw_vectors(HnswIndex)
    end.

extract_hnsw_vectors(Index) ->
    %% Access internal structure (HNSW uses #hnsw_index record)
    case Index of
        {hnsw_index, _, _, Nodes, _, _, _} when is_map(Nodes) ->
            maps:fold(
                fun(_Id, Node, Acc) ->
                    %% Node is #hnsw_node record
                    case Node of
                        {hnsw_node, Id, QuantizedVec, _, _, _} ->
                            Vec = barrel_vectordb_hnsw:dequantize(QuantizedVec),
                            [{Id, Vec} | Acc];
                        _ -> Acc
                    end
                end,
                [],
                Nodes
            );
        _ ->
            %% Fallback: can't extract vectors
            []
    end.

%% Extract all vectors from DiskANN index (excluding deleted)
extract_all_vectors_from_diskann(DiskannIndex, Deletes) ->
    Info = barrel_vectordb_diskann:info(DiskannIndex),
    case maps:get(active_size, Info, 0) of
        0 -> [];
        _ ->
            extract_diskann_vectors(DiskannIndex, Deletes)
    end.

extract_diskann_vectors(Index, Deletes) ->
    %% Access internal structure
    case Index of
        {diskann_index, _, _, _, _, Vectors, DeletedSet, _} when is_map(Vectors) ->
            AllDeleted = sets:union(Deletes, DeletedSet),
            maps:fold(
                fun(Id, Vec, Acc) ->
                    case sets:is_element(Id, AllDeleted) of
                        true -> Acc;
                        false -> [{Id, Vec} | Acc]
                    end
                end,
                [],
                Vectors
            );
        _ ->
            []
    end.

%% Apply pending deletes to cold layer
apply_deletes_to_cold(Cold, Deletes) ->
    lists:foldl(
        fun(Id, AccCold) ->
            {ok, Updated} = barrel_vectordb_diskann:delete(AccCold, Id),
            Updated
        end,
        Cold,
        sets:to_list(Deletes)
    ).

%% Deserialize cold layer
deserialize_cold(<<0:8, Rest/binary>>) ->
    {undefined, Rest};
deserialize_cold(<<1:8, Len:32, ColdBin:Len/binary, Rest/binary>>) ->
    Cold = binary_to_term(ColdBin),
    {Cold, Rest}.
