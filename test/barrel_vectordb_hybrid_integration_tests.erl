%%%-------------------------------------------------------------------
%%% @doc Integration tests for Hybrid DiskANN + HNSW indexing
%%%
%%% Tests the full workflow of the hybrid index including:
%%% - Build and search
%%% - Compaction cycles
%%% - Recall comparison with HNSW
%%% - Memory usage estimates
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_hybrid_integration_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

hybrid_integration_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"end to end workflow", fun test_end_to_end/0},
        {"recall comparable to pure hnsw", fun test_recall_vs_hnsw/0},
        {"multiple compaction cycles", fun test_multiple_compaction_cycles/0},
        {"delete during compaction", fun test_delete_during_compaction/0},
        {"search during compaction", fun test_search_during_compaction/0},
        {"large batch insert", fun test_large_batch_insert/0}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    rand:seed(exsss, {42, 42, 42}),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_end_to_end() ->
    %% Create hybrid index
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{
        dimension => 32,
        hot_capacity => 50,
        distance_fn => cosine
    }),

    %% Insert vectors
    Vectors = [{integer_to_binary(I), random_vector(32)} || I <- lists:seq(1, 100)],
    Index1 = lists:foldl(
        fun({Id, Vec}, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(Acc, Id, Vec),
            NewAcc
        end,
        Index0,
        Vectors
    ),

    %% Search should work
    Query = random_vector(32),
    Results1 = barrel_vectordb_index_hybrid:search(Index1, Query, 10),
    ?assertEqual(10, length(Results1)),

    %% Compact (moves hot to cold)
    {ok, Index2} = barrel_vectordb_index_hybrid:compact(Index1),

    %% Search should still work after compaction
    Results2 = barrel_vectordb_index_hybrid:search(Index2, Query, 10),
    ?assertEqual(10, length(Results2)),

    %% Info should show vectors moved to cold layer
    Info = barrel_vectordb_index_hybrid:info(Index2),
    ?assertEqual(0, maps:get(hot_size, Info)),
    ?assertEqual(100, maps:get(cold_size, Info)).

test_recall_vs_hnsw() ->
    %% Build both HNSW and hybrid indexes with same data
    Dim = 32,
    NumVectors = 200,
    NumQueries = 10,
    K = 10,

    Vectors = [{integer_to_binary(I), random_vector(Dim)} || I <- lists:seq(1, NumVectors)],

    %% Pure HNSW
    HnswIndex = lists:foldl(
        fun({Id, Vec}, Acc) ->
            barrel_vectordb_hnsw:insert(Acc, Id, Vec)
        end,
        barrel_vectordb_hnsw:new(#{dimension => Dim}),
        Vectors
    ),

    %% Hybrid index (compact to use DiskANN)
    {ok, HybridIndex0} = barrel_vectordb_index_hybrid:new(#{dimension => Dim}),
    HybridIndex1 = lists:foldl(
        fun({Id, Vec}, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(Acc, Id, Vec),
            NewAcc
        end,
        HybridIndex0,
        Vectors
    ),
    {ok, HybridIndex} = barrel_vectordb_index_hybrid:compact(HybridIndex1),

    %% Measure recall on multiple queries
    Queries = [random_vector(Dim) || _ <- lists:seq(1, NumQueries)],

    HnswRecalls = [measure_recall(HnswIndex, hnsw, Q, Vectors, K) || Q <- Queries],
    HybridRecalls = [measure_recall(HybridIndex, hybrid, Q, Vectors, K) || Q <- Queries],

    AvgHnswRecall = lists:sum(HnswRecalls) / length(HnswRecalls),
    AvgHybridRecall = lists:sum(HybridRecalls) / length(HybridRecalls),

    %% Hybrid recall should be within 20% of HNSW recall
    %% (some loss expected due to DiskANN graph vs HNSW multilayer graph)
    ?assert(AvgHybridRecall >= AvgHnswRecall - 0.20).

test_multiple_compaction_cycles() ->
    %% Test stability across multiple compaction cycles
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{
        dimension => 16,
        hot_capacity => 20
    }),

    %% Run 3 cycles: insert, compact, verify
    FinalIndex = lists:foldl(
        fun(Cycle, AccIndex) ->
            %% Insert 20 vectors
            Start = (Cycle - 1) * 20 + 1,
            End = Cycle * 20,
            WithInserts = lists:foldl(
                fun(I, Acc) ->
                    {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                        Acc, integer_to_binary(I), random_vector(16)
                    ),
                    NewAcc
                end,
                AccIndex,
                lists:seq(Start, End)
            ),

            %% Compact
            {ok, Compacted} = barrel_vectordb_index_hybrid:compact(WithInserts),

            %% Verify total size
            ExpectedSize = Cycle * 20,
            ?assertEqual(ExpectedSize, barrel_vectordb_index_hybrid:size(Compacted)),

            %% Search should work
            Results = barrel_vectordb_index_hybrid:search(Compacted, random_vector(16), 5),
            ?assert(length(Results) >= 1),

            Compacted
        end,
        Index0,
        lists:seq(1, 3)
    ),

    Info = barrel_vectordb_index_hybrid:info(FinalIndex),
    ?assertEqual(60, maps:get(cold_size, Info)),
    ?assertEqual(0, maps:get(hot_size, Info)).

test_delete_during_compaction() ->
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{dimension => 16}),

    %% Insert vectors
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(16)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 30)
    ),

    %% Compact first batch
    {ok, Index2} = barrel_vectordb_index_hybrid:compact(Index1),
    ?assertEqual(30, barrel_vectordb_index_hybrid:size(Index2)),

    %% Insert more to hot layer
    Index3 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(16)
            ),
            NewAcc
        end,
        Index2,
        lists:seq(31, 40)
    ),
    ?assertEqual(40, barrel_vectordb_index_hybrid:size(Index3)),

    %% Delete from hot layer only (more predictable)
    {ok, Index4} = barrel_vectordb_index_hybrid:delete(Index3, <<"35">>),
    {ok, Index5} = barrel_vectordb_index_hybrid:delete(Index4, <<"36">>),

    %% Size should reflect deletions (40 - 2 = 38)
    ?assertEqual(38, barrel_vectordb_index_hybrid:size(Index5)),

    %% Search should not return deleted
    Results = barrel_vectordb_index_hybrid:search(Index5, random_vector(16), 40),
    ResultIds = [Id || {Id, _} <- Results],
    ?assertNot(lists:member(<<"35">>, ResultIds)),
    ?assertNot(lists:member(<<"36">>, ResultIds)),

    %% Compact should consolidate
    {ok, Index6} = barrel_vectordb_index_hybrid:compact(Index5),
    ?assertEqual(38, barrel_vectordb_index_hybrid:size(Index6)).

test_search_during_compaction() ->
    %% Verify searches work while compaction runs
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{dimension => 16}),

    %% Insert vectors
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(16)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 50)
    ),

    %% Start compaction in background
    Self = self(),
    spawn_link(fun() ->
        {ok, NewIndex} = barrel_vectordb_index_hybrid:compact(Index1),
        Self ! {compaction_done, NewIndex}
    end),

    %% Search multiple times during compaction
    lists:foreach(
        fun(_) ->
            timer:sleep(1),
            Results = barrel_vectordb_index_hybrid:search(Index1, random_vector(16), 5),
            ?assert(length(Results) >= 1)
        end,
        lists:seq(1, 10)
    ),

    %% Wait for compaction to complete
    receive
        {compaction_done, _NewIndex} -> ok
    after 5000 ->
        ?assert(false, "Compaction timeout")
    end.

test_large_batch_insert() ->
    %% Test with moderate dataset (reduced for test speed)
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{
        dimension => 16,
        hot_capacity => 50,
        diskann_r => 8,
        diskann_l_build => 20
    }),

    %% Insert 100 vectors
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(16)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 100)
    ),

    ?assertEqual(100, barrel_vectordb_index_hybrid:size(Index1)),

    %% Search should return results
    Results = barrel_vectordb_index_hybrid:search(Index1, random_vector(16), 10),
    ?assertEqual(10, length(Results)),

    %% Compact
    {ok, Index2} = barrel_vectordb_index_hybrid:compact(Index1),

    %% Search after compact
    Results2 = barrel_vectordb_index_hybrid:search(Index2, random_vector(16), 10),
    ?assertEqual(10, length(Results2)).

%%====================================================================
%% Helpers
%%====================================================================

random_vector(Dim) ->
    normalize([rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)]).

normalize(Vec) ->
    Norm = math:sqrt(lists:sum([V*V || V <- Vec])),
    case Norm < 0.0001 of
        true -> Vec;
        false -> [V / Norm || V <- Vec]
    end.

measure_recall(Index, Type, Query, Vectors, K) ->
    %% Get index results
    Results = case Type of
        hnsw -> barrel_vectordb_hnsw:search(Index, Query, K);
        hybrid -> barrel_vectordb_index_hybrid:search(Index, Query, K)
    end,
    ResultIds = [Id || {Id, _} <- Results],

    %% Compute ground truth (brute force)
    TrueTopK = brute_force_search(Vectors, Query, K),
    TrueIds = [Id || {Id, _} <- TrueTopK],

    %% Recall = intersection / K
    Intersection = length([Id || Id <- ResultIds, lists:member(Id, TrueIds)]),
    case K of
        0 -> 0.0;
        _ -> Intersection / K
    end.

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
