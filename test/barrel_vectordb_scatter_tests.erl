%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_scatter module
%%%
%%% These tests use meck to mock cluster components so tests can run
%%% without a full cluster setup.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_scatter_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

scatter_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
       %% search/4 tests (embed_query path)
       {"search with undefined embedder returns error", fun test_search_embedder_undefined/0},
       {"search with empty embedder map returns error", fun test_search_embedder_empty_map/0},
       {"search with valid embedder embeds and searches", fun test_search_valid_embedder/0},
       {"search propagates embed error", fun test_search_embed_error/0},

       %% search_vector/3 tests
       {"search_vector with empty shards returns empty", fun test_search_vector_empty_shards/0},
       {"search_vector with single local shard", fun test_search_vector_single_local/0},
       {"search_vector with multiple local shards merges results", fun test_search_vector_multiple_local/0},
       {"search_vector sorts by score descending", fun test_search_vector_sort_by_score/0},
       {"search_vector respects k parameter", fun test_search_vector_k_limit/0},
       {"search_vector collection not found", fun test_search_vector_collection_not_found/0},
       {"search_vector cluster client error", fun test_search_vector_cluster_error/0},

       %% search_local_shard/3 tests
       {"search_local_shard without collection returns error", fun test_local_shard_no_collection/0},
       {"search_local_shard with valid collection", fun test_local_shard_valid/0},
       {"search_local_shard store not found", fun test_local_shard_store_not_found/0},

       %% Remote shard and RPC tests
       {"search_vector with remote shard uses RPC", fun test_search_vector_remote_shard/0},
       {"search_vector with RPC failure skips shard", fun test_search_vector_rpc_failure/0},

       %% gather_results tests
       {"gather deduplicates by key", fun test_gather_dedup/0},
       {"gather keeps higher score", fun test_gather_higher_score/0},
       {"gather handles empty input", fun test_gather_empty/0}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    %% Ensure clean state
    (catch meck:unload(barrel_vectordb_cluster_client)),
    (catch meck:unload(barrel_vectordb_shard_manager)),
    (catch meck:unload(barrel_vectordb_shard_locator)),
    (catch meck:unload(barrel_embed)),
    (catch meck:unload(barrel_vectordb)),
    (catch meck:unload(rpc)),
    timer:sleep(10),
    meck:new(barrel_vectordb_cluster_client, [passthrough, no_link]),
    meck:new(barrel_vectordb_shard_manager, [passthrough, no_link]),
    meck:new(barrel_vectordb_shard_locator, [passthrough, no_link]),
    meck:new(barrel_embed, [passthrough, no_link]),
    meck:new(barrel_vectordb, [passthrough, no_link]),
    meck:new(rpc, [unstick, passthrough, no_link]),
    ok.

cleanup(_) ->
    meck:unload(barrel_vectordb_cluster_client),
    meck:unload(barrel_vectordb_shard_manager),
    meck:unload(barrel_vectordb_shard_locator),
    meck:unload(barrel_embed),
    meck:unload(barrel_vectordb),
    meck:unload(rpc),
    ok.

%%====================================================================
%% search/4 tests (embed_query path)
%%====================================================================

test_search_embedder_undefined() ->
    %% When EmbedderInfo is not a map, should return error
    Result = barrel_vectordb_scatter:search(<<"test_collection">>, <<"query">>, #{}, undefined),
    ?assertEqual({error, embedder_not_configured}, Result).

test_search_embedder_empty_map() ->
    %% When EmbedderInfo is a map but has no embedder key
    Result = barrel_vectordb_scatter:search(<<"test_collection">>, <<"query">>, #{}, #{}),
    ?assertEqual({error, embedder_not_configured}, Result).

test_search_valid_embedder() ->
    %% Mock embedder
    MockEmbedder = mock_embedder,
    MockVector = [0.1, 0.2, 0.3],
    meck:expect(barrel_embed, embed, fun(<<"query">>, mock_embedder) ->
        {ok, MockVector}
    end),

    %% Mock cluster client - collection exists with 1 shard
    CollectionMeta = {collection_meta, <<"test_collection">>, 768, 1, 1, []},
    meck:expect(barrel_vectordb_cluster_client, get_collections, fun() ->
        {ok, #{<<"test_collection">> => CollectionMeta}}
    end),

    %% Mock shard locator
    meck:expect(barrel_vectordb_shard_locator, all_shards, fun(1) -> [0] end),

    %% Mock shard manager - local store
    meck:expect(barrel_vectordb_shard_manager, get_local_store, fun({<<"test_collection">>, 0}) ->
        {ok, test_store}
    end),

    %% Mock search result
    meck:expect(barrel_vectordb, search_vector, fun(test_store, [0.1, 0.2, 0.3], _Opts) ->
        {ok, [#{key => <<"doc1">>, score => 0.95}]}
    end),

    Result = barrel_vectordb_scatter:search(<<"test_collection">>, <<"query">>, #{k => 10},
                                             #{embedder => MockEmbedder}),

    ?assertMatch({ok, [#{key := <<"doc1">>, score := 0.95}]}, Result),
    ?assert(meck:called(barrel_embed, embed, [<<"query">>, mock_embedder])).

test_search_embed_error() ->
    meck:expect(barrel_embed, embed, fun(_, _) ->
        {error, embedding_failed}
    end),

    Result = barrel_vectordb_scatter:search(<<"test">>, <<"query">>, #{}, #{embedder => mock}),
    ?assertEqual({error, embedding_failed}, Result).

%%====================================================================
%% search_vector/3 tests
%%====================================================================

test_search_vector_empty_shards() ->
    %% Collection with 0 shards effectively
    CollectionMeta = {collection_meta, <<"empty_col">>, 768, 1, 1, []},
    meck:expect(barrel_vectordb_cluster_client, get_collections, fun() ->
        {ok, #{<<"empty_col">> => CollectionMeta}}
    end),
    meck:expect(barrel_vectordb_shard_locator, all_shards, fun(1) -> [0] end),
    meck:expect(barrel_vectordb_shard_manager, get_local_store, fun(_) ->
        {error, not_found}
    end),
    meck:expect(barrel_vectordb_cluster_client, get_shard_placement, fun(_) ->
        {ok, []}  %% No placement found
    end),

    Result = barrel_vectordb_scatter:search_vector(<<"empty_col">>, [0.1, 0.2], #{k => 10}),
    ?assertEqual({ok, []}, Result).

test_search_vector_single_local() ->
    setup_single_local_shard(),

    meck:expect(barrel_vectordb, search_vector, fun(test_store, _, _) ->
        {ok, [
            #{key => <<"doc1">>, score => 0.9},
            #{key => <<"doc2">>, score => 0.8}
        ]}
    end),

    {ok, Results} = barrel_vectordb_scatter:search_vector(<<"test_col">>, [0.1, 0.2, 0.3], #{k => 10}),
    ?assertEqual(2, length(Results)).

test_search_vector_multiple_local() ->
    %% Setup collection with 2 shards
    CollectionMeta = {collection_meta, <<"test_col">>, 768, 2, 1, []},
    meck:expect(barrel_vectordb_cluster_client, get_collections, fun() ->
        {ok, #{<<"test_col">> => CollectionMeta}}
    end),
    meck:expect(barrel_vectordb_shard_locator, all_shards, fun(2) -> [0, 1] end),
    meck:expect(barrel_vectordb_shard_manager, get_local_store, fun
        ({<<"test_col">>, 0}) -> {ok, store_0};
        ({<<"test_col">>, 1}) -> {ok, store_1}
    end),

    meck:expect(barrel_vectordb, search_vector, fun
        (store_0, _, _) -> {ok, [#{key => <<"doc1">>, score => 0.9}]};
        (store_1, _, _) -> {ok, [#{key => <<"doc2">>, score => 0.8}]}
    end),

    {ok, Results} = barrel_vectordb_scatter:search_vector(<<"test_col">>, [0.1], #{k => 10}),
    ?assertEqual(2, length(Results)),
    Keys = [maps:get(key, R) || R <- Results],
    ?assert(lists:member(<<"doc1">>, Keys)),
    ?assert(lists:member(<<"doc2">>, Keys)).

test_search_vector_sort_by_score() ->
    setup_single_local_shard(),

    meck:expect(barrel_vectordb, search_vector, fun(_, _, _) ->
        {ok, [
            #{key => <<"low">>, score => 0.5},
            #{key => <<"high">>, score => 0.95},
            #{key => <<"mid">>, score => 0.7}
        ]}
    end),

    {ok, Results} = barrel_vectordb_scatter:search_vector(<<"test_col">>, [0.1], #{k => 10}),
    Scores = [maps:get(score, R) || R <- Results],
    ?assertEqual([0.95, 0.7, 0.5], Scores).

test_search_vector_k_limit() ->
    setup_single_local_shard(),

    meck:expect(barrel_vectordb, search_vector, fun(_, _, _) ->
        {ok, [
            #{key => <<"doc1">>, score => 0.9},
            #{key => <<"doc2">>, score => 0.8},
            #{key => <<"doc3">>, score => 0.7},
            #{key => <<"doc4">>, score => 0.6},
            #{key => <<"doc5">>, score => 0.5}
        ]}
    end),

    {ok, Results} = barrel_vectordb_scatter:search_vector(<<"test_col">>, [0.1], #{k => 2}),
    ?assertEqual(2, length(Results)),
    %% Should be top 2 by score
    ?assertEqual(0.9, maps:get(score, hd(Results))).

test_search_vector_collection_not_found() ->
    meck:expect(barrel_vectordb_cluster_client, get_collections, fun() ->
        {ok, #{}}  %% Empty collections map
    end),

    Result = barrel_vectordb_scatter:search_vector(<<"nonexistent">>, [0.1], #{k => 10}),
    ?assertEqual({error, collection_not_found}, Result).

test_search_vector_cluster_error() ->
    meck:expect(barrel_vectordb_cluster_client, get_collections, fun() ->
        {error, timeout}
    end),

    Result = barrel_vectordb_scatter:search_vector(<<"test">>, [0.1], #{k => 10}),
    ?assertEqual({error, timeout}, Result).

%%====================================================================
%% search_local_shard/3 tests
%%====================================================================

test_local_shard_no_collection() ->
    %% No collection in opts
    Result = barrel_vectordb_scatter:search_local_shard(0, [0.1, 0.2], #{}),
    ?assertEqual({error, collection_not_specified}, Result).

test_local_shard_valid() ->
    meck:expect(barrel_vectordb_shard_manager, get_local_store, fun({<<"test_col">>, 0}) ->
        {ok, local_store}
    end),
    meck:expect(barrel_vectordb, search_vector, fun(local_store, [0.1, 0.2], Opts) ->
        ?assertEqual(<<"test_col">>, maps:get(collection, Opts)),
        {ok, [#{key => <<"doc1">>, score => 0.9}]}
    end),

    Result = barrel_vectordb_scatter:search_local_shard(0, [0.1, 0.2], #{collection => <<"test_col">>}),
    ?assertMatch({ok, [#{key := <<"doc1">>}]}, Result).

test_local_shard_store_not_found() ->
    meck:expect(barrel_vectordb_shard_manager, get_local_store, fun(_) ->
        {error, not_found}
    end),

    Result = barrel_vectordb_scatter:search_local_shard(0, [0.1], #{collection => <<"test">>}),
    ?assertEqual({error, not_found}, Result).

%%====================================================================
%% Remote shard and RPC tests
%%====================================================================

test_search_vector_remote_shard() ->
    %% Setup collection with remote shard
    CollectionMeta = {collection_meta, <<"test_col">>, 768, 1, 1, []},
    meck:expect(barrel_vectordb_cluster_client, get_collections, fun() ->
        {ok, #{<<"test_col">> => CollectionMeta}}
    end),
    meck:expect(barrel_vectordb_shard_locator, all_shards, fun(1) -> [0] end),
    meck:expect(barrel_vectordb_shard_manager, get_local_store, fun(_) ->
        {error, not_found}  %% Not local
    end),
    meck:expect(barrel_vectordb_cluster_client, get_shard_placement, fun(<<"test_col">>) ->
        {ok, [{0, {barrel_vectordb, 'remote@node'}, []}]}
    end),

    %% Mock RPC call to remote node
    meck:expect(rpc, call, fun('remote@node', barrel_vectordb_scatter, search_local_shard,
                               [0, [0.1, 0.2, 0.3], _Opts], _Timeout) ->
        {ok, [#{key => <<"remote_doc">>, score => 0.88}]}
    end),

    {ok, Results} = barrel_vectordb_scatter:search_vector(<<"test_col">>, [0.1, 0.2, 0.3], #{k => 10}),
    ?assertEqual(1, length(Results)),
    ?assertEqual(<<"remote_doc">>, maps:get(key, hd(Results))).

test_search_vector_rpc_failure() ->
    %% Setup collection with remote shard that fails
    CollectionMeta = {collection_meta, <<"test_col">>, 768, 1, 1, []},
    meck:expect(barrel_vectordb_cluster_client, get_collections, fun() ->
        {ok, #{<<"test_col">> => CollectionMeta}}
    end),
    meck:expect(barrel_vectordb_shard_locator, all_shards, fun(1) -> [0] end),
    meck:expect(barrel_vectordb_shard_manager, get_local_store, fun(_) ->
        {error, not_found}
    end),
    meck:expect(barrel_vectordb_cluster_client, get_shard_placement, fun(<<"test_col">>) ->
        {ok, [{0, {barrel_vectordb, 'remote@node'}, []}]}
    end),

    %% RPC fails with badrpc
    meck:expect(rpc, call, fun(_, _, _, _, _) ->
        {badrpc, nodedown}
    end),

    %% Should return empty results (failed shard is skipped)
    {ok, Results} = barrel_vectordb_scatter:search_vector(<<"test_col">>, [0.1], #{k => 10}),
    ?assertEqual([], Results).

%%====================================================================
%% gather_results tests
%%====================================================================

test_gather_dedup() ->
    setup_single_local_shard(),

    meck:expect(barrel_vectordb, search_vector, fun(_, _, _) ->
        {ok, [
            #{key => <<"doc1">>, score => 0.9},
            #{key => <<"doc1">>, score => 0.85}  %% Duplicate
        ]}
    end),

    {ok, Results} = barrel_vectordb_scatter:search_vector(<<"test_col">>, [0.1], #{k => 10}),
    Keys = [maps:get(key, R) || R <- Results],
    ?assertEqual([<<"doc1">>], Keys).

test_gather_higher_score() ->
    setup_single_local_shard(),

    meck:expect(barrel_vectordb, search_vector, fun(_, _, _) ->
        {ok, [
            #{key => <<"doc1">>, score => 0.5, data => <<"low">>},
            #{key => <<"doc1">>, score => 0.9, data => <<"high">>}
        ]}
    end),

    {ok, [Result]} = barrel_vectordb_scatter:search_vector(<<"test_col">>, [0.1], #{k => 10}),
    ?assertEqual(0.9, maps:get(score, Result)),
    ?assertEqual(<<"high">>, maps:get(data, Result)).

test_gather_empty() ->
    setup_single_local_shard(),

    meck:expect(barrel_vectordb, search_vector, fun(_, _, _) ->
        {ok, []}
    end),

    {ok, Results} = barrel_vectordb_scatter:search_vector(<<"test_col">>, [0.1], #{k => 10}),
    ?assertEqual([], Results).

%%====================================================================
%% Helpers
%%====================================================================

setup_single_local_shard() ->
    CollectionMeta = {collection_meta, <<"test_col">>, 768, 1, 1, []},
    meck:expect(barrel_vectordb_cluster_client, get_collections, fun() ->
        {ok, #{<<"test_col">> => CollectionMeta}}
    end),
    meck:expect(barrel_vectordb_shard_locator, all_shards, fun(1) -> [0] end),
    meck:expect(barrel_vectordb_shard_manager, get_local_store, fun({<<"test_col">>, 0}) ->
        {ok, test_store}
    end).
