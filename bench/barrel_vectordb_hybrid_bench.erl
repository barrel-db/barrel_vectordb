%%%-------------------------------------------------------------------
%%% @doc Benchmark: Hybrid DiskANN vs Pure HNSW
%%%
%%% Compares:
%%% - Build time
%%% - Memory usage (estimated)
%%% - Search latency
%%% - Recall@10
%%% - Compaction overhead
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_hybrid_bench).

-export([run/0, run/1]).

-define(DEFAULT_CONFIG, #{
    dimension => 32,
    num_vectors => 1000,
    num_queries => 50,
    k => 10
}).

%%====================================================================
%% API
%%====================================================================

run() ->
    run(?DEFAULT_CONFIG).

run(Config) ->
    Dim = maps:get(dimension, Config, 64),
    NumVectors = maps:get(num_vectors, Config, 5000),
    NumQueries = maps:get(num_queries, Config, 100),
    K = maps:get(k, Config, 10),

    io:format("~n"),
    io:format("============================================================~n"),
    io:format("       DiskANN Hybrid vs Pure HNSW Benchmark~n"),
    io:format("============================================================~n"),
    io:format("~n"),
    io:format("Configuration:~n"),
    io:format("  Dimension:     ~p~n", [Dim]),
    io:format("  Vectors:       ~p~n", [NumVectors]),
    io:format("  Queries:       ~p~n", [NumQueries]),
    io:format("  K (top-k):     ~p~n", [K]),
    io:format("~n"),

    %% Seed random for reproducibility
    rand:seed(exsss, {42, 42, 42}),

    %% Generate test data
    io:format("Generating test vectors...~n"),
    Vectors = [{integer_to_binary(I), random_vector(Dim)} || I <- lists:seq(1, NumVectors)],
    Queries = [random_vector(Dim) || _ <- lists:seq(1, NumQueries)],

    %% Build HNSW index
    io:format("~n--- Build Time ---~n"),
    {HnswBuildUs, HnswIndex} = timer:tc(fun() ->
        lists:foldl(
            fun({Id, Vec}, Acc) ->
                barrel_vectordb_hnsw:insert(Acc, Id, Vec)
            end,
            barrel_vectordb_hnsw:new(#{dimension => Dim, m => 16, ef_construction => 100}),
            Vectors
        )
    end),
    io:format("HNSW build:   ~.2f s~n", [HnswBuildUs / 1_000_000]),

    %% Build Hybrid index (insert + compact)
    %% PQ only makes sense for large datasets (>= 10k vectors)
    %% For smaller datasets, the quantization loss hurts more than memory savings help
    UsePQ = NumVectors >= 10000,
    PQK = case NumVectors < 50000 of
        true -> 64;
        false -> 256
    end,
    PQM = min(8, max(4, Dim div 8)),
    {HybridBuildUs, HybridIndex} = timer:tc(fun() ->
        {ok, Index0} = barrel_vectordb_index_hybrid:new(#{
            dimension => Dim,
            hot_capacity => NumVectors + 1,
            hnsw_m => 16,
            hnsw_ef_construction => 100,
            diskann_r => 16,
            diskann_l_build => 30,
            diskann_alpha => 1.2,
            %% Only enable PQ for large datasets
            use_pq => UsePQ,
            pq_m => PQM,
            pq_k => PQK
        }),
        Index1 = lists:foldl(
            fun({Id, Vec}, Acc) ->
                {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(Acc, Id, Vec),
                NewAcc
            end,
            Index0,
            Vectors
        ),
        {ok, Compacted} = barrel_vectordb_index_hybrid:compact(Index1),
        Compacted
    end),
    io:format("Hybrid build: ~.2f s (includes compaction + PQ training)~n", [HybridBuildUs / 1_000_000]),

    %% Memory usage (estimated)
    io:format("~n--- Memory Usage (Estimated) ---~n"),
    HnswMemBytes = estimate_hnsw_memory(HnswIndex, Dim),
    HybridMemBytes = estimate_hybrid_memory(HybridIndex, Dim),
    io:format("HNSW:   ~.2f MB (~.1f bytes/vector)~n",
              [HnswMemBytes / 1_048_576, HnswMemBytes / NumVectors]),
    io:format("Hybrid: ~.2f MB (~.1f bytes/vector)~n",
              [HybridMemBytes / 1_048_576, HybridMemBytes / NumVectors]),
    MemReduction = HnswMemBytes / max(1, HybridMemBytes),
    io:format("Memory reduction: ~.1fx~n", [MemReduction]),

    %% Search latency
    io:format("~n--- Search Latency (~p queries) ---~n", [NumQueries]),
    {HnswSearchUs, _} = timer:tc(fun() ->
        [barrel_vectordb_hnsw:search(HnswIndex, Q, K) || Q <- Queries]
    end),
    {HybridSearchUs, _} = timer:tc(fun() ->
        [barrel_vectordb_index_hybrid:search(HybridIndex, Q, K) || Q <- Queries]
    end),
    HnswLatencyMs = HnswSearchUs / NumQueries / 1000,
    HybridLatencyMs = HybridSearchUs / NumQueries / 1000,
    io:format("HNSW avg latency:   ~.3f ms~n", [HnswLatencyMs]),
    io:format("Hybrid avg latency: ~.3f ms~n", [HybridLatencyMs]),

    %% Recall@K
    io:format("~n--- Recall@~p ---~n", [K]),
    HnswRecalls = [measure_recall(HnswIndex, hnsw, Q, Vectors, K) || Q <- Queries],
    HybridRecalls = [measure_recall(HybridIndex, hybrid, Q, Vectors, K) || Q <- Queries],
    AvgHnswRecall = lists:sum(HnswRecalls) / length(HnswRecalls),
    AvgHybridRecall = lists:sum(HybridRecalls) / length(HybridRecalls),
    io:format("HNSW recall@~p:   ~.1f%~n", [K, AvgHnswRecall * 100]),
    io:format("Hybrid recall@~p: ~.1f%~n", [K, AvgHybridRecall * 100]),

    %% Compaction overhead
    io:format("~n--- Compaction Overhead ---~n"),
    %% Create fresh hybrid with vectors in hot layer
    {ok, FreshHybrid0} = barrel_vectordb_index_hybrid:new(#{
        dimension => Dim,
        hot_capacity => NumVectors + 1,
        diskann_r => 16,
        diskann_l_build => 30
    }),
    CompactionTestSize = min(500, NumVectors),
    FreshHybrid1 = lists:foldl(
        fun({Id, Vec}, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(Acc, Id, Vec),
            NewAcc
        end,
        FreshHybrid0,
        lists:sublist(Vectors, CompactionTestSize)
    ),
    {CompactUs, _} = timer:tc(fun() ->
        barrel_vectordb_index_hybrid:compact(FreshHybrid1)
    end),
    io:format("Compaction time (~p vectors): ~.2f s~n", [CompactionTestSize, CompactUs / 1_000_000]),

    %% Summary
    io:format("~n============================================================~n"),
    io:format("                        Summary~n"),
    io:format("============================================================~n"),
    io:format("~n"),
    io:format("| Metric              | HNSW          | Hybrid        |~n"),
    io:format("|---------------------|---------------|---------------|~n"),
    io:format("| Build time          | ~6.2f s      | ~6.2f s      |~n",
              [HnswBuildUs / 1_000_000, HybridBuildUs / 1_000_000]),
    io:format("| Memory (MB)         | ~6.2f        | ~6.2f        |~n",
              [HnswMemBytes / 1_048_576, HybridMemBytes / 1_048_576]),
    io:format("| Search latency (ms) | ~6.3f        | ~6.3f        |~n",
              [HnswLatencyMs, HybridLatencyMs]),
    io:format("| Recall@~p           | ~6.1f%       | ~6.1f%       |~n",
              [K, AvgHnswRecall * 100, AvgHybridRecall * 100]),
    io:format("~n"),
    io:format("Memory reduction: ~.1fx~n", [MemReduction]),
    io:format("~n"),

    #{
        hnsw => #{
            build_time_s => HnswBuildUs / 1_000_000,
            memory_mb => HnswMemBytes / 1_048_576,
            latency_ms => HnswLatencyMs,
            recall => AvgHnswRecall
        },
        hybrid => #{
            build_time_s => HybridBuildUs / 1_000_000,
            memory_mb => HybridMemBytes / 1_048_576,
            latency_ms => HybridLatencyMs,
            recall => AvgHybridRecall
        }
    }.

%%====================================================================
%% Internal Functions
%%====================================================================

random_vector(Dim) ->
    Vec = [rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)],
    normalize(Vec).

normalize(Vec) ->
    Norm = math:sqrt(lists:sum([V*V || V <- Vec])),
    case Norm < 0.0001 of
        true -> Vec;
        false -> [V / Norm || V <- Vec]
    end.

estimate_hnsw_memory(Index, Dim) ->
    Info = barrel_vectordb_hnsw:info(Index),
    Size = maps:get(size, Info, 0),
    Config = maps:get(config, Info, #{}),
    M = maps:get(m, Config, 16),
    MaxLayer = maps:get(max_layer, Info, 0),

    %% Per vector: quantized vector (dim+4) + norm (8) + neighbors (M * layers * 8)
    VecBytes = Dim + 4,  %% int8 quantized + scale
    NormBytes = 8,
    AvgLayers = 1 + MaxLayer / 2,  %% Average layers per node
    NeighborBytes = M * AvgLayers * 16,  %% ID refs (assume 16 bytes avg)

    Size * (VecBytes + NormBytes + NeighborBytes).

estimate_hybrid_memory(Index, Dim) ->
    Info = barrel_vectordb_index_hybrid:info(Index),
    HotSize = maps:get(hot_size, Info, 0),
    ColdSize = maps:get(cold_size, Info, 0),

    %% Hot layer: same as HNSW
    HotBytes = HotSize * (Dim + 4 + 8 + 16 * 16),

    %% Cold layer (DiskANN): check if PQ is enabled
    ColdInfo = maps:get(cold_info, Info, #{}),
    R = maps:get(r, maps:get(config, ColdInfo, #{}), 64),
    PQInfo = maps:get(pq, ColdInfo, #{enabled => false}),
    PQEnabled = maps:get(enabled, PQInfo, false),

    %% Per cold vector: ID (16) + neighbor IDs (R * 16) + vector storage
    %% With PQ: only M bytes for codes (in RAM), full vectors on disk
    %% Without PQ: full vectors in RAM (Dim * 8)
    ColdVecBytes = case PQEnabled of
        true ->
            %% With PQ: just codes in RAM (M bytes) + graph
            PQM = maps:get(m, maps:get(m, PQInfo, #{}), 8),
            16 + R * 16 + PQM;  %% ID + neighbors + PQ code
        false ->
            %% Without PQ: full vectors in RAM
            16 + R * 16 + Dim * 8
    end,
    ColdBytes = ColdSize * ColdVecBytes,

    HotBytes + ColdBytes.

measure_recall(Index, Type, Query, Vectors, K) ->
    Results = case Type of
        hnsw -> barrel_vectordb_hnsw:search(Index, Query, K);
        hybrid -> barrel_vectordb_index_hybrid:search(Index, Query, K)
    end,
    ResultIds = [Id || {Id, _} <- Results],

    %% Ground truth
    TrueTopK = brute_force_search(Vectors, Query, K),
    TrueIds = [Id || {Id, _} <- TrueTopK],

    Intersection = length([Id || Id <- ResultIds, lists:member(Id, TrueIds)]),
    Intersection / K.

brute_force_search(Vectors, Query, K) ->
    Distances = [{Id, cosine_distance(Query, Vec)} || {Id, Vec} <- Vectors],
    Sorted = lists:sort(fun({_, D1}, {_, D2}) -> D1 =< D2 end, Distances),
    lists:sublist(Sorted, K).

cosine_distance(Vec1, Vec2) ->
    Dot = lists:sum([A * B || {A, B} <- lists:zip(Vec1, Vec2)]),
    Norm1 = math:sqrt(lists:sum([V*V || V <- Vec1])),
    Norm2 = math:sqrt(lists:sum([V*V || V <- Vec2])),
    Denom = Norm1 * Norm2,
    case Denom < 1.0e-10 of
        true -> 1.0;
        false -> 1.0 - (Dot / Denom)
    end.
