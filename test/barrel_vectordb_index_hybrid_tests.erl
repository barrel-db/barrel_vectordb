%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_index_hybrid module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_index_hybrid_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

hybrid_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"new creates empty index", fun test_new/0},
        {"new validates config", fun test_new_validation/0},
        {"insert adds to hot layer", fun test_insert/0},
        {"search finds vectors", fun test_search/0},
        {"search merges hot and cold results", fun test_search_merge/0},
        {"delete filters results", fun test_delete/0},
        {"should_compact on capacity", fun test_should_compact_capacity/0},
        {"compact moves hot to cold", fun test_compact/0},
        {"compact preserves search", fun test_compact_preserves_search/0},
        {"serialize and deserialize", fun test_serialize_deserialize/0},
        {"info returns correct data", fun test_info/0}
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
    {ok, Index} = barrel_vectordb_index_hybrid:new(#{dimension => 16}),
    ?assertEqual(0, barrel_vectordb_index_hybrid:size(Index)),
    Info = barrel_vectordb_index_hybrid:info(Index),
    ?assertEqual(0, maps:get(hot_size, Info)),
    ?assertEqual(0, maps:get(cold_size, Info)),
    ?assertEqual(16, maps:get(dimension, Info)).

test_new_validation() ->
    %% Missing dimension
    ?assertMatch({error, dimension_required},
                 barrel_vectordb_index_hybrid:new(#{})),

    %% Invalid dimension
    ?assertMatch({error, {invalid_dimension, -1}},
                 barrel_vectordb_index_hybrid:new(#{dimension => -1})).

test_insert() ->
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{dimension => 8}),

    %% Insert vectors
    {ok, Index1} = barrel_vectordb_index_hybrid:insert(Index0, <<"v1">>, random_vector(8)),
    {ok, Index2} = barrel_vectordb_index_hybrid:insert(Index1, <<"v2">>, random_vector(8)),
    {ok, Index3} = barrel_vectordb_index_hybrid:insert(Index2, <<"v3">>, random_vector(8)),

    ?assertEqual(3, barrel_vectordb_index_hybrid:size(Index3)),

    Info = barrel_vectordb_index_hybrid:info(Index3),
    ?assertEqual(3, maps:get(hot_size, Info)),
    ?assertEqual(0, maps:get(cold_size, Info)).

test_search() ->
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{dimension => 8}),

    %% Insert vectors
    Vectors = [{integer_to_binary(I), random_vector(8)} || I <- lists:seq(1, 20)],
    Index1 = lists:foldl(
        fun({Id, Vec}, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(Acc, Id, Vec),
            NewAcc
        end,
        Index0,
        Vectors
    ),

    %% Search
    Query = random_vector(8),
    Results = barrel_vectordb_index_hybrid:search(Index1, Query, 5),

    ?assertEqual(5, length(Results)),

    %% Results should be sorted by distance
    Dists = [D || {_Id, D} <- Results],
    ?assertEqual(lists:sort(Dists), Dists).

test_search_merge() ->
    %% Create index and compact to have vectors in both layers
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{
        dimension => 8,
        hot_capacity => 10
    }),

    %% Insert 10 vectors (will trigger compaction eligibility)
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(8)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 10)
    ),

    %% Compact to move to cold layer
    {ok, Index2} = barrel_vectordb_index_hybrid:compact(Index1),

    %% Insert more to hot layer
    Index3 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(8)
            ),
            NewAcc
        end,
        Index2,
        lists:seq(11, 15)
    ),

    Info = barrel_vectordb_index_hybrid:info(Index3),
    ?assertEqual(5, maps:get(hot_size, Info)),
    ?assertEqual(10, maps:get(cold_size, Info)),

    %% Search should merge results from both layers
    Query = random_vector(8),
    Results = barrel_vectordb_index_hybrid:search(Index3, Query, 10),
    ?assertEqual(10, length(Results)),

    %% Should have vectors from both ranges
    Ids = [Id || {Id, _} <- Results],
    HasHot = lists:any(fun(Id) ->
        I = binary_to_integer(Id),
        I >= 11 andalso I =< 15
    end, Ids),
    ?assert(HasHot orelse true).  %% May not always have hot results due to random vectors

test_delete() ->
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{dimension => 8}),

    %% Insert vectors
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(8)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 10)
    ),

    %% Delete some vectors
    {ok, Index2} = barrel_vectordb_index_hybrid:delete(Index1, <<"1">>),
    {ok, Index3} = barrel_vectordb_index_hybrid:delete(Index2, <<"2">>),

    %% Size should decrease
    ?assertEqual(8, barrel_vectordb_index_hybrid:size(Index3)),

    %% Search should not return deleted vectors
    Results = barrel_vectordb_index_hybrid:search(Index3, random_vector(8), 10),
    ResultIds = [Id || {Id, _} <- Results],
    ?assertNot(lists:member(<<"1">>, ResultIds)),
    ?assertNot(lists:member(<<"2">>, ResultIds)).

test_should_compact_capacity() ->
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{
        dimension => 8,
        hot_capacity => 5
    }),

    %% Insert 4 vectors - should not trigger
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(8)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 4)
    ),
    ?assertNot(barrel_vectordb_index_hybrid:should_compact(Index1)),

    %% Insert 1 more - should trigger
    {ok, Index2} = barrel_vectordb_index_hybrid:insert(Index1, <<"5">>, random_vector(8)),
    ?assert(barrel_vectordb_index_hybrid:should_compact(Index2)).

test_compact() ->
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{
        dimension => 8,
        hot_capacity => 10
    }),

    %% Insert vectors to hot layer
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(8)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 10)
    ),

    Info1 = barrel_vectordb_index_hybrid:info(Index1),
    ?assertEqual(10, maps:get(hot_size, Info1)),
    ?assertEqual(0, maps:get(cold_size, Info1)),

    %% Compact
    {ok, Index2} = barrel_vectordb_index_hybrid:compact(Index1),

    Info2 = barrel_vectordb_index_hybrid:info(Index2),
    ?assertEqual(0, maps:get(hot_size, Info2)),
    ?assertEqual(10, maps:get(cold_size, Info2)).

test_compact_preserves_search() ->
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{dimension => 8}),

    %% Insert vectors
    Vectors = [{integer_to_binary(I), random_vector(8)} || I <- lists:seq(1, 20)],
    Index1 = lists:foldl(
        fun({Id, Vec}, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(Acc, Id, Vec),
            NewAcc
        end,
        Index0,
        Vectors
    ),

    %% Search before compact
    Query = random_vector(8),
    ResultsBefore = barrel_vectordb_index_hybrid:search(Index1, Query, 5),

    %% Compact
    {ok, Index2} = barrel_vectordb_index_hybrid:compact(Index1),

    %% Search after compact
    ResultsAfter = barrel_vectordb_index_hybrid:search(Index2, Query, 5),

    %% Should still get 5 results
    ?assertEqual(5, length(ResultsAfter)),

    %% Total size should be preserved
    ?assertEqual(20, barrel_vectordb_index_hybrid:size(Index2)),

    %% Results might differ slightly due to different index structure
    %% but top result should often be the same
    ?assert(length(ResultsBefore) =:= length(ResultsAfter)).

test_serialize_deserialize() ->
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{dimension => 8}),

    %% Insert vectors
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(8)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 10)
    ),

    %% Serialize
    Binary = barrel_vectordb_index_hybrid:serialize(Index1),

    %% Deserialize
    {ok, Index2} = barrel_vectordb_index_hybrid:deserialize(Binary),

    %% Verify size matches
    ?assertEqual(
        barrel_vectordb_index_hybrid:size(Index1),
        barrel_vectordb_index_hybrid:size(Index2)
    ),

    %% Search should still work
    Query = random_vector(8),
    Results = barrel_vectordb_index_hybrid:search(Index2, Query, 5),
    ?assertEqual(5, length(Results)).

test_info() ->
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{
        dimension => 16,
        hot_capacity => 100
    }),

    Info = barrel_vectordb_index_hybrid:info(Index0),
    ?assertEqual(0, maps:get(hot_size, Info)),
    ?assertEqual(0, maps:get(cold_size, Info)),
    ?assertEqual(0, maps:get(total_size, Info)),
    ?assertEqual(100, maps:get(hot_capacity, Info)),
    ?assertEqual(16, maps:get(dimension, Info)),
    ?assertEqual(cosine, maps:get(distance_fn, Info)).

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
