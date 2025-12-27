%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_hnsw module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_hnsw_tests).

-include_lib("eunit/include/eunit.hrl").
-include("barrel_vectordb.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

hnsw_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"new creates empty index", fun test_new/0},
        {"insert adds vectors", fun test_insert/0},
        {"search finds nearest neighbors", fun test_search/0},
        {"delete removes vectors", fun test_delete/0},
        {"serialization round trip", fun test_serialization/0},
        {"cosine distance correct", fun test_cosine_distance/0},
        {"euclidean distance correct", fun test_euclidean_distance/0},
        {"multi-layer index works", fun test_multi_layer/0}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_new() ->
    Index = barrel_vectordb_hnsw:new(),
    ?assertEqual(0, barrel_vectordb_hnsw:size(Index)),
    Info = barrel_vectordb_hnsw:info(Index),
    ?assertEqual(undefined, maps:get(entry_point, Info)),
    ?assertEqual(0, maps:get(size, Info)).

test_insert() ->
    Index0 = barrel_vectordb_hnsw:new(#{dimension => 3}),

    %% Insert first vector
    Vec1 = [1.0, 0.0, 0.0],
    Index1 = barrel_vectordb_hnsw:insert(Index0, <<"v1">>, Vec1),
    ?assertEqual(1, barrel_vectordb_hnsw:size(Index1)),

    %% Insert second vector
    Vec2 = [0.0, 1.0, 0.0],
    Index2 = barrel_vectordb_hnsw:insert(Index1, <<"v2">>, Vec2),
    ?assertEqual(2, barrel_vectordb_hnsw:size(Index2)),

    %% Insert third vector
    Vec3 = [0.0, 0.0, 1.0],
    Index3 = barrel_vectordb_hnsw:insert(Index2, <<"v3">>, Vec3),
    ?assertEqual(3, barrel_vectordb_hnsw:size(Index3)),

    %% Verify nodes exist
    ?assertMatch({ok, _}, barrel_vectordb_hnsw:get_node(Index3, <<"v1">>)),
    ?assertMatch({ok, _}, barrel_vectordb_hnsw:get_node(Index3, <<"v2">>)),
    ?assertMatch({ok, _}, barrel_vectordb_hnsw:get_node(Index3, <<"v3">>)).

test_search() ->
    %% Create index with some vectors
    Index0 = barrel_vectordb_hnsw:new(#{dimension => 3}),
    Vectors = [
        {<<"a">>, [1.0, 0.0, 0.0]},
        {<<"b">>, [0.9, 0.1, 0.0]},
        {<<"c">>, [0.0, 1.0, 0.0]},
        {<<"d">>, [0.0, 0.0, 1.0]},
        {<<"e">>, [0.5, 0.5, 0.0]}
    ],

    Index = lists:foldl(
        fun({Id, Vec}, Acc) ->
            barrel_vectordb_hnsw:insert(Acc, Id, Vec)
        end,
        Index0,
        Vectors
    ),

    %% Search for vector similar to [1.0, 0.0, 0.0]
    Query = [1.0, 0.0, 0.0],
    Results = barrel_vectordb_hnsw:search(Index, Query, 3),

    %% Should return top 3 results
    ?assertEqual(3, length(Results)),

    %% First result should be "a" (exact match)
    [{FirstId, FirstDist} | _] = Results,
    ?assertEqual(<<"a">>, FirstId),
    ?assert(FirstDist < 0.01),  %% Very close to 0

    %% Second should be "b" (very similar)
    [{_, _}, {SecondId, _} | _] = Results,
    ?assertEqual(<<"b">>, SecondId).

test_delete() ->
    Index0 = barrel_vectordb_hnsw:new(#{dimension => 3}),

    %% Insert vectors
    Index1 = barrel_vectordb_hnsw:insert(Index0, <<"v1">>, [1.0, 0.0, 0.0]),
    Index2 = barrel_vectordb_hnsw:insert(Index1, <<"v2">>, [0.0, 1.0, 0.0]),
    ?assertEqual(2, barrel_vectordb_hnsw:size(Index2)),

    %% Delete one
    Index3 = barrel_vectordb_hnsw:delete(Index2, <<"v1">>),
    ?assertEqual(1, barrel_vectordb_hnsw:size(Index3)),
    ?assertEqual(not_found, barrel_vectordb_hnsw:get_node(Index3, <<"v1">>)),
    ?assertMatch({ok, _}, barrel_vectordb_hnsw:get_node(Index3, <<"v2">>)).

test_serialization() ->
    %% Create index with vectors
    Index0 = barrel_vectordb_hnsw:new(#{dimension => 3}),
    Index1 = barrel_vectordb_hnsw:insert(Index0, <<"v1">>, [1.0, 0.0, 0.0]),
    Index2 = barrel_vectordb_hnsw:insert(Index1, <<"v2">>, [0.0, 1.0, 0.0]),

    %% Serialize
    Binary = barrel_vectordb_hnsw:serialize(Index2),
    ?assert(is_binary(Binary)),

    %% Deserialize
    {ok, Restored} = barrel_vectordb_hnsw:deserialize(Binary),
    ?assertEqual(2, barrel_vectordb_hnsw:size(Restored)),

    %% Search should work
    Results = barrel_vectordb_hnsw:search(Restored, [1.0, 0.0, 0.0], 1),
    ?assertEqual(1, length(Results)),
    [{ResultId, ResultDist}] = Results,
    ?assertEqual(<<"v1">>, ResultId),
    ?assert(is_float(ResultDist)).

test_cosine_distance() ->
    %% Same vector should have distance 0
    Vec1 = [1.0, 0.0, 0.0],
    ?assertEqual(0.0, barrel_vectordb_hnsw:cosine_distance(Vec1, Vec1)),

    %% Orthogonal vectors should have distance 1
    Vec2 = [0.0, 1.0, 0.0],
    Distance = barrel_vectordb_hnsw:cosine_distance(Vec1, Vec2),
    ?assert(abs(Distance - 1.0) < 0.0001),

    %% Opposite vectors should have distance 2
    Vec3 = [-1.0, 0.0, 0.0],
    Distance2 = barrel_vectordb_hnsw:cosine_distance(Vec1, Vec3),
    ?assert(abs(Distance2 - 2.0) < 0.0001),

    %% Similar vectors should have low distance
    Vec4 = [0.9, 0.1, 0.0],
    Distance3 = barrel_vectordb_hnsw:cosine_distance(Vec1, Vec4),
    ?assert(Distance3 < 0.1).

test_euclidean_distance() ->
    %% Same vector should have distance 0
    Vec1 = [1.0, 0.0, 0.0],
    ?assertEqual(0.0, barrel_vectordb_hnsw:euclidean_distance(Vec1, Vec1)),

    %% Unit vectors at right angles
    Vec2 = [0.0, 1.0, 0.0],
    Distance = barrel_vectordb_hnsw:euclidean_distance(Vec1, Vec2),
    Expected = math:sqrt(2.0),
    ?assert(abs(Distance - Expected) < 0.0001),

    %% Known distance
    Vec3 = [4.0, 0.0, 0.0],
    Vec4 = [0.0, 3.0, 0.0],
    Distance2 = barrel_vectordb_hnsw:euclidean_distance(Vec3, Vec4),
    ?assertEqual(5.0, Distance2).  %% 3-4-5 triangle

test_multi_layer() ->
    %% Insert enough vectors to trigger multi-layer structure
    Index0 = barrel_vectordb_hnsw:new(#{dimension => 8, m => 4, ef_construction => 20}),

    %% Generate random vectors
    Vectors = [
        {list_to_binary("v" ++ integer_to_list(I)),
         [rand:uniform() || _ <- lists:seq(1, 8)]}
        || I <- lists:seq(1, 50)
    ],

    Index = lists:foldl(
        fun({Id, Vec}, Acc) ->
            barrel_vectordb_hnsw:insert(Acc, Id, Vec)
        end,
        Index0,
        Vectors
    ),

    ?assertEqual(50, barrel_vectordb_hnsw:size(Index)),

    %% Search should still work
    QueryVec = [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5],
    Results = barrel_vectordb_hnsw:search(Index, QueryVec, 5),
    ?assertEqual(5, length(Results)),

    %% Results should be sorted by distance
    Distances = [D || {_, D} <- Results],
    SortedDistances = lists:sort(Distances),
    ?assertEqual(SortedDistances, Distances).

