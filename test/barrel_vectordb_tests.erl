%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb main API module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

api_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {foreach,
      fun setup_test/0,
      fun cleanup_test/1,
      [
        {"add and get document", fun test_add_get/0},
        {"add with explicit vector", fun test_add_vector/0},
        {"search finds similar documents", fun test_search/0},
        {"search with vector query", fun test_search_vector/0},
        {"delete removes document", fun test_delete/0},
        {"count returns correct number", fun test_count/0},
        {"stats returns store info", fun test_stats/0}
      ]
     }
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(rocksdb),
    ok.

cleanup(_) ->
    ok.

setup_test() ->
    TestDir = "/tmp/barrel_vectordb_api_test_" ++ integer_to_list(erlang:unique_integer([positive])),

    %% Ensure meck is clean before starting
    (catch meck:unload(barrel_vectordb_embed)),

    %% Mock the embedder to return deterministic vectors
    meck:new(barrel_vectordb_embed, [passthrough]),
    meck:expect(barrel_vectordb_embed, init, fun(_Config) ->
        {ok, #{providers => [], dimension => 3, batch_size => 32}}
    end),
    meck:expect(barrel_vectordb_embed, embed, fun(Text, _State) ->
        Hash = erlang:phash2(Text, 1000000),
        Vec = [
            Hash / 1000000.0,
            (Hash rem 1000) / 1000.0,
            ((Hash rem 100) / 100.0)
        ],
        {ok, Vec}
    end),
    meck:expect(barrel_vectordb_embed, embed_batch, fun(Texts, _State) ->
        Vectors = lists:map(fun(Text) ->
            Hash = erlang:phash2(Text, 1000000),
            [
                Hash / 1000000.0,
                (Hash rem 1000) / 1000.0,
                ((Hash rem 100) / 100.0)
            ]
        end, Texts),
        {ok, Vectors}
    end),
    meck:expect(barrel_vectordb_embed, info, fun(_State) ->
        #{providers => [], dimension => 3}
    end),

    %% Start the store
    {ok, Pid} = barrel_vectordb:start_link(#{
        name => test_store,
        path => TestDir,
        dimension => 3,
        hnsw => #{m => 4, ef_construction => 20}
    }),
    {Pid, TestDir}.

cleanup_test({_Pid, TestDir}) ->
    catch barrel_vectordb:stop(test_store),
    %% Make sure meck is fully unloaded
    timer:sleep(50),  %% Allow gen_server to stop
    (catch meck:unload(barrel_vectordb_embed)),
    os:cmd("rm -rf " ++ TestDir),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_add_get() ->
    %% Add a document
    ok = barrel_vectordb:add(test_store, <<"doc1">>, <<"hello world">>, #{type => greeting}),

    %% Get it back
    {ok, Doc} = barrel_vectordb:get(test_store, <<"doc1">>),
    ?assertEqual(<<"doc1">>, maps:get(key, Doc)),
    ?assertEqual(<<"hello world">>, maps:get(text, Doc)),
    ?assertEqual(#{type => greeting}, maps:get(metadata, Doc)),
    ?assert(is_list(maps:get(vector, Doc))),

    %% Non-existent document
    ?assertEqual(not_found, barrel_vectordb:get(test_store, <<"nonexistent">>)).

test_add_vector() ->
    Vector = [1.0, 0.0, 0.0],
    ok = barrel_vectordb:add_vector(test_store, <<"vec1">>, <<"explicit">>, #{}, Vector),

    {ok, Doc} = barrel_vectordb:get(test_store, <<"vec1">>),
    ?assertEqual(Vector, maps:get(vector, Doc)).

test_search() ->
    %% Add some documents
    ok = barrel_vectordb:add_vector(test_store, <<"a">>, <<"text a">>, #{type => a}, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"b">>, <<"text b">>, #{type => b}, [0.9, 0.1, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"c">>, <<"text c">>, #{type => c}, [0.0, 1.0, 0.0]),

    %% Search - the mock will convert the text query to a vector
    {ok, Results} = barrel_vectordb:search(test_store, <<"query">>, #{k => 3}),

    ?assertEqual(3, length(Results)),
    %% Each result should have required fields
    [First | _] = Results,
    ?assert(maps:is_key(key, First)),
    ?assert(maps:is_key(text, First)),
    ?assert(maps:is_key(metadata, First)),
    ?assert(maps:is_key(score, First)).

test_search_vector() ->
    %% Add documents with known vectors
    ok = barrel_vectordb:add_vector(test_store, <<"x">>, <<"x text">>, #{}, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"y">>, <<"y text">>, #{}, [0.0, 1.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"z">>, <<"z text">>, #{}, [0.0, 0.0, 1.0]),

    %% Search with vector query
    {ok, Results} = barrel_vectordb:search_vector(test_store, [1.0, 0.0, 0.0], #{k => 2}),

    ?assertEqual(2, length(Results)),
    %% First result should be "x" (exact match)
    [First | _] = Results,
    ?assertEqual(<<"x">>, maps:get(key, First)),
    ?assert(maps:get(score, First) > 0.99).

test_delete() ->
    ok = barrel_vectordb:add_vector(test_store, <<"del">>, <<"delete me">>, #{}, [0.5, 0.5, 0.0]),
    ?assertMatch({ok, _}, barrel_vectordb:get(test_store, <<"del">>)),

    ok = barrel_vectordb:delete(test_store, <<"del">>),
    ?assertEqual(not_found, barrel_vectordb:get(test_store, <<"del">>)).

test_count() ->
    ?assertEqual(0, barrel_vectordb:count(test_store)),

    ok = barrel_vectordb:add_vector(test_store, <<"c1">>, <<"t1">>, #{}, [1.0, 0.0, 0.0]),
    ?assertEqual(1, barrel_vectordb:count(test_store)),

    ok = barrel_vectordb:add_vector(test_store, <<"c2">>, <<"t2">>, #{}, [0.0, 1.0, 0.0]),
    ?assertEqual(2, barrel_vectordb:count(test_store)).

test_stats() ->
    ok = barrel_vectordb:add_vector(test_store, <<"s1">>, <<"stats test">>, #{}, [1.0, 0.0, 0.0]),

    {ok, Stats} = barrel_vectordb:stats(test_store),
    ?assertEqual(3, maps:get(dimension, Stats)),
    ?assertEqual(1, maps:get(count, Stats)),
    ?assert(maps:is_key(hnsw, Stats)),
    ?assert(maps:is_key(config, Stats)).
