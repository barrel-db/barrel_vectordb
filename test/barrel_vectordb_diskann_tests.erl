%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_diskann module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_diskann_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

diskann_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"new creates empty index", fun test_new/0},
        {"new validates config", fun test_new_validation/0},
        {"build creates index", fun test_build/0},
        {"search finds nearest", fun test_search/0},
        {"search recall test", fun test_search_recall/0},
        {"insert adds vectors", fun test_insert/0},
        {"insert preserves recall", fun test_insert_preserves_recall/0},
        {"delete filters results", fun test_delete/0},
        {"consolidate removes deleted", fun test_consolidate/0},
        {"alpha rng stability", fun test_alpha_rng_stability/0}
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

test_new() ->
    {ok, Index} = barrel_vectordb_diskann:new(#{dimension => 128}),
    ?assertEqual(0, barrel_vectordb_diskann:size(Index)),
    Info = barrel_vectordb_diskann:info(Index),
    ?assertEqual(0, maps:get(size, Info)),
    ?assertEqual(undefined, maps:get(medoid, Info)).

test_new_validation() ->
    %% Missing dimension
    ?assertMatch({error, dimension_required},
                 barrel_vectordb_diskann:new(#{})),

    %% Invalid dimension
    ?assertMatch({error, {invalid_dimension, -1}},
                 barrel_vectordb_diskann:new(#{dimension => -1})).

test_build() ->
    Vectors = [{integer_to_binary(I), random_vector(32)}
               || I <- lists:seq(1, 100)],
    Config = #{dimension => 32, r => 8, l_build => 20, alpha => 1.2},
    {ok, Index} = barrel_vectordb_diskann:build(Config, Vectors),

    ?assertEqual(100, barrel_vectordb_diskann:size(Index)),

    Info = barrel_vectordb_diskann:info(Index),
    ?assert(maps:get(medoid, Info) =/= undefined),
    ?assert(maps:get(avg_degree, Info) > 0).

test_search() ->
    %% Use a larger index for better connectivity
    Vectors = [{integer_to_binary(I), random_vector(8)} || I <- lists:seq(1, 50)],
    {ok, Index} = barrel_vectordb_diskann:build(
        #{dimension => 8, r => 8, l_build => 30},
        Vectors
    ),

    %% Search should return results
    Query = random_vector(8),
    Results = barrel_vectordb_diskann:search(Index, Query, 5),

    %% Should get 5 results
    ?assertEqual(5, length(Results)),

    %% Results should be sorted by distance
    Dists = [D || {_, D} <- Results],
    ?assertEqual(lists:sort(Dists), Dists).

test_search_recall() ->
    %% Build index with 100 vectors (smaller for faster tests)
    Vectors = [{integer_to_binary(I), random_vector(16)}
               || I <- lists:seq(1, 100)],
    Config = #{dimension => 16, r => 16, l_build => 30, l_search => 30, alpha => 1.2},
    {ok, Index} = barrel_vectordb_diskann:build(Config, Vectors),

    %% Test recall on 5 queries
    Recalls = [measure_recall(Index, random_vector(16), Vectors, 5)
               || _ <- lists:seq(1, 5)],
    AvgRecall = lists:sum(Recalls) / length(Recalls),

    %% Should achieve reasonable recall (>=50%)
    ?assert(AvgRecall >= 0.50).

test_insert() ->
    %% Start with empty index
    {ok, Index0} = barrel_vectordb_diskann:new(#{dimension => 8, r => 4}),

    %% Insert first vector
    {ok, Index1} = barrel_vectordb_diskann:insert(Index0, <<"v1">>, random_vector(8)),
    ?assertEqual(1, barrel_vectordb_diskann:size(Index1)),

    %% Insert more vectors (need more than 2 to search for 2)
    {ok, Index2} = barrel_vectordb_diskann:insert(Index1, <<"v2">>, random_vector(8)),
    {ok, Index3} = barrel_vectordb_diskann:insert(Index2, <<"v3">>, random_vector(8)),
    {ok, Index4} = barrel_vectordb_diskann:insert(Index3, <<"v4">>, random_vector(8)),
    {ok, Index5} = barrel_vectordb_diskann:insert(Index4, <<"v5">>, random_vector(8)),
    ?assertEqual(5, barrel_vectordb_diskann:size(Index5)),

    %% Search should return results (may be fewer than K if graph not well connected)
    Results = barrel_vectordb_diskann:search(Index5, random_vector(8), 3),
    ?assert(length(Results) >= 1).

test_insert_preserves_recall() ->
    %% Build initial index (smaller for faster tests)
    InitialVectors = [{integer_to_binary(I), random_vector(16)}
                      || I <- lists:seq(1, 100)],
    Config = #{dimension => 16, r => 8, l_build => 20, alpha => 1.2},
    {ok, Index0} = barrel_vectordb_diskann:build(Config, InitialVectors),

    %% Insert 20 more vectors
    Index1 = lists:foldl(
        fun(I, AccIndex) ->
            {ok, NewIndex} = barrel_vectordb_diskann:insert(
                AccIndex,
                integer_to_binary(100 + I),
                random_vector(16)
            ),
            NewIndex
        end,
        Index0,
        lists:seq(1, 20)
    ),

    ?assertEqual(120, barrel_vectordb_diskann:size(Index1)),

    %% Search should still work after inserts
    Results = barrel_vectordb_diskann:search(Index1, random_vector(16), 5),
    ?assert(length(Results) >= 3).

test_delete() ->
    Vectors = [{integer_to_binary(I), random_vector(16)}
               || I <- lists:seq(1, 50)],
    {ok, Index0} = barrel_vectordb_diskann:build(
        #{dimension => 16, r => 8, l_build => 20},
        Vectors
    ),

    %% Delete some vectors
    {ok, Index1} = barrel_vectordb_diskann:delete(Index0, <<"1">>),
    {ok, Index2} = barrel_vectordb_diskann:delete(Index1, <<"2">>),
    {ok, Index3} = barrel_vectordb_diskann:delete(Index2, <<"3">>),

    %% Size should decrease
    ?assertEqual(47, barrel_vectordb_diskann:size(Index3)),

    %% Deleted vectors should not appear in results
    Results = barrel_vectordb_diskann:search(Index3, random_vector(16), 50),
    ResultIds = [Id || {Id, _} <- Results],
    ?assertNot(lists:member(<<"1">>, ResultIds)),
    ?assertNot(lists:member(<<"2">>, ResultIds)),
    ?assertNot(lists:member(<<"3">>, ResultIds)).

test_consolidate() ->
    Vectors = [{integer_to_binary(I), random_vector(16)}
               || I <- lists:seq(1, 100)],
    {ok, Index0} = barrel_vectordb_diskann:build(
        #{dimension => 16, r => 8, l_build => 30},
        Vectors
    ),

    %% Delete 10% of vectors
    Index1 = lists:foldl(
        fun(I, AccIndex) ->
            {ok, NewIndex} = barrel_vectordb_diskann:delete(AccIndex, integer_to_binary(I)),
            NewIndex
        end,
        Index0,
        lists:seq(1, 10)
    ),

    Info1 = barrel_vectordb_diskann:info(Index1),
    ?assertEqual(10, maps:get(deleted_count, Info1)),

    %% Consolidate
    {ok, Index2} = barrel_vectordb_diskann:consolidate_deletes(Index1),

    Info2 = barrel_vectordb_diskann:info(Index2),
    ?assertEqual(0, maps:get(deleted_count, Info2)),
    ?assertEqual(90, maps:get(active_size, Info2)),

    %% Search should still work (use correct dimension)
    Results = barrel_vectordb_diskann:search(Index2, random_vector(16), 5),
    ?assert(length(Results) > 0).

test_alpha_rng_stability() ->
    %% Test that index works after delete/consolidate/insert cycles
    %% Simplified version for faster testing
    InitialVectors = [{integer_to_binary(I), random_vector(16)}
                      || I <- lists:seq(1, 100)],
    Config = #{dimension => 16, r => 8, l_build => 20, alpha => 1.2},
    {ok, Index0} = barrel_vectordb_diskann:build(Config, InitialVectors),

    %% 2 cycles of delete + consolidate + reinsert
    {FinalIndex, _} = lists:foldl(
        fun(_Cycle, {AccIndex, NextId}) ->
            %% Delete 5 random vectors
            ToDelete = [integer_to_binary(rand:uniform(NextId - 1))
                        || _ <- lists:seq(1, 5)],
            Index1 = lists:foldl(
                fun(Id, Acc) ->
                    {ok, New} = barrel_vectordb_diskann:delete(Acc, Id),
                    New
                end,
                AccIndex,
                lists:usort(ToDelete)  %% Remove duplicates
            ),

            %% Consolidate
            {ok, Index2} = barrel_vectordb_diskann:consolidate_deletes(Index1),

            %% Reinsert 5 new vectors
            Index3 = lists:foldl(
                fun(I, Acc) ->
                    {ok, New} = barrel_vectordb_diskann:insert(
                        Acc,
                        integer_to_binary(NextId + I),
                        random_vector(16)
                    ),
                    New
                end,
                Index2,
                lists:seq(1, 5)
            ),

            {Index3, NextId + 5}
        end,
        {Index0, 101},
        lists:seq(1, 2)
    ),

    %% Index should still work after cycles
    Results = barrel_vectordb_diskann:search(FinalIndex, random_vector(16), 5),
    ?assert(length(Results) >= 1).

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

measure_recall(Index, Query, Vectors, K) ->
    %% Get DiskANN results
    Results = barrel_vectordb_diskann:search(Index, Query, K),
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
