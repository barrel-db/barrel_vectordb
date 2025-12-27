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
        {"update modifies document", fun test_update/0},
        {"update returns not_found", fun test_update_not_found/0},
        {"upsert inserts new", fun test_upsert_insert/0},
        {"upsert updates existing", fun test_upsert_update/0},
        {"peek returns documents", fun test_peek/0},
        {"count returns correct number", fun test_count/0},
        {"stats returns store info", fun test_stats/0},
        {"persistence reload", fun test_persistence_reload/0}
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

test_update() ->
    %% Add a document
    ok = barrel_vectordb:add_vector(test_store, <<"upd">>, <<"original">>, #{v => 1}, [1.0, 0.0, 0.0]),
    {ok, Doc1} = barrel_vectordb:get(test_store, <<"upd">>),
    ?assertEqual(<<"original">>, maps:get(text, Doc1)),
    ?assertEqual(#{v => 1}, maps:get(metadata, Doc1)),

    %% Update it
    ok = barrel_vectordb:update(test_store, <<"upd">>, <<"updated text">>, #{v => 2}),
    {ok, Doc2} = barrel_vectordb:get(test_store, <<"upd">>),
    ?assertEqual(<<"updated text">>, maps:get(text, Doc2)),
    ?assertEqual(#{v => 2}, maps:get(metadata, Doc2)).

test_update_not_found() ->
    ?assertEqual(not_found, barrel_vectordb:update(test_store, <<"nonexistent">>, <<"text">>, #{})).

test_upsert_insert() ->
    %% Upsert new document
    ?assertEqual(not_found, barrel_vectordb:get(test_store, <<"ups1">>)),
    ok = barrel_vectordb:upsert(test_store, <<"ups1">>, <<"upserted">>, #{source => upsert}),
    {ok, Doc} = barrel_vectordb:get(test_store, <<"ups1">>),
    ?assertEqual(<<"upserted">>, maps:get(text, Doc)),
    ?assertEqual(#{source => upsert}, maps:get(metadata, Doc)).

test_upsert_update() ->
    %% Add then upsert (update)
    ok = barrel_vectordb:add_vector(test_store, <<"ups2">>, <<"v1">>, #{v => 1}, [0.5, 0.5, 0.0]),
    ok = barrel_vectordb:upsert(test_store, <<"ups2">>, <<"v2">>, #{v => 2}),
    {ok, Doc} = barrel_vectordb:get(test_store, <<"ups2">>),
    ?assertEqual(<<"v2">>, maps:get(text, Doc)),
    ?assertEqual(#{v => 2}, maps:get(metadata, Doc)).

test_peek() ->
    %% Add some documents
    ok = barrel_vectordb:add_vector(test_store, <<"p1">>, <<"peek 1">>, #{n => 1}, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"p2">>, <<"peek 2">>, #{n => 2}, [0.0, 1.0, 0.0]),
    ok = barrel_vectordb:add_vector(test_store, <<"p3">>, <<"peek 3">>, #{n => 3}, [0.0, 0.0, 1.0]),

    %% Peek at 2 documents
    {ok, Docs} = barrel_vectordb:peek(test_store, 2),
    ?assertEqual(2, length(Docs)),

    %% Each doc should have required fields
    [First | _] = Docs,
    ?assert(maps:is_key(key, First)),
    ?assert(maps:is_key(text, First)),
    ?assert(maps:is_key(metadata, First)),
    ?assert(maps:is_key(vector, First)),

    %% Peek at more than exists should return all
    {ok, AllDocs} = barrel_vectordb:peek(test_store, 10),
    ?assertEqual(3, length(AllDocs)).

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

test_persistence_reload() ->
    %% This test uses its own store to test persistence across restarts
    PersistDir = "/tmp/barrel_vectordb_persist_test_" ++
                 integer_to_list(erlang:unique_integer([positive])),

    %% Create store and add documents
    {ok, _} = barrel_vectordb:start_link(#{
        name => persist_test_store,
        path => PersistDir,
        dimension => 3,
        hnsw => #{m => 4, ef_construction => 20}
    }),

    ok = barrel_vectordb:add_vector(persist_test_store, <<"p1">>, <<"text 1">>,
                                    #{n => 1}, [1.0, 0.0, 0.0]),
    ok = barrel_vectordb:add_vector(persist_test_store, <<"p2">>, <<"text 2">>,
                                    #{n => 2}, [0.0, 1.0, 0.0]),
    ok = barrel_vectordb:add_vector(persist_test_store, <<"p3">>, <<"text 3">>,
                                    #{n => 3}, [0.0, 0.0, 1.0]),

    ?assertEqual(3, barrel_vectordb:count(persist_test_store)),

    %% Stop the store
    ok = barrel_vectordb:stop(persist_test_store),
    timer:sleep(100),

    %% Restart from the same directory
    {ok, _} = barrel_vectordb:start_link(#{
        name => persist_test_store,
        path => PersistDir,
        dimension => 3,
        hnsw => #{m => 4, ef_construction => 20}
    }),

    %% Verify count is preserved
    ?assertEqual(3, barrel_vectordb:count(persist_test_store)),

    %% Verify documents can be retrieved
    {ok, Doc1} = barrel_vectordb:get(persist_test_store, <<"p1">>),
    ?assertEqual(<<"text 1">>, maps:get(text, Doc1)),
    ?assertEqual(#{n => 1}, maps:get(metadata, Doc1)),

    %% Verify search still works
    {ok, Results} = barrel_vectordb:search_vector(persist_test_store,
                                                   [1.0, 0.0, 0.0], #{k => 2}),
    ?assertEqual(2, length(Results)),
    [First | _] = Results,
    ?assertEqual(<<"p1">>, maps:get(key, First)),

    %% Cleanup
    ok = barrel_vectordb:stop(persist_test_store),
    os:cmd("rm -rf " ++ PersistDir).
