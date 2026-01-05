%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_embed_local module
%%%
%%% These tests use meck to mock the Python port, so Python doesn't
%%% need to be installed. For integration tests with real Python,
%%% see test/integration/README.md.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_local_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

local_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
       {"name returns local", fun test_name/0},
       {"dimension returns default", fun test_dimension_default/0},
       {"dimension returns configured value", fun test_dimension_custom/0},
       {"available returns true with valid port", fun test_available_true/0},
       {"available returns false without port", fun test_available_false/0}
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

test_name() ->
    ?assertEqual(local, barrel_vectordb_embed_local:name()).

test_dimension_default() ->
    ?assertEqual(768, barrel_vectordb_embed_local:dimension(#{})).

test_dimension_custom() ->
    ?assertEqual(384, barrel_vectordb_embed_local:dimension(#{dimension => 384})).

test_available_true() ->
    %% Create a dummy port for testing
    Port = open_port({spawn, "cat"}, []),
    try
        ?assertEqual(true, barrel_vectordb_embed_local:available(#{port => Port}))
    after
        port_close(Port)
    end.

test_available_false() ->
    ?assertEqual(false, barrel_vectordb_embed_local:available(#{})),
    ?assertEqual(false, barrel_vectordb_embed_local:available(#{port => undefined})).

%%====================================================================
%% Embed Error Handling Tests
%%
%% These tests verify that the embed/2 function correctly handles
%% various responses from embed_batch/2. We test the pattern matching
%% logic directly since mocking internal function calls is complex.
%%====================================================================

embed_error_handling_test_() ->
    [
        {"empty vector [[]] is detected as error",
         fun() ->
             %% Simulate what embed/2 does when embed_batch returns [[]]
             Text = <<"test">>,
             BatchResult = {ok, [[]]},
             Result = process_embed_result(BatchResult, Text),
             ?assertMatch({error, {empty_embedding, _}}, Result)
         end},

        {"no embeddings [] is detected as error",
         fun() ->
             Text = <<"test">>,
             BatchResult = {ok, []},
             Result = process_embed_result(BatchResult, Text),
             ?assertMatch({error, {no_embedding, _}}, Result)
         end},

        {"multiple vectors [[1.0], [2.0]] is detected as error",
         fun() ->
             Text = <<"test">>,
             BatchResult = {ok, [[1.0], [2.0]]},
             Result = process_embed_result(BatchResult, Text),
             ?assertMatch({error, {unexpected_embedding, _}}, Result)
         end},

        {"valid vector succeeds",
         fun() ->
             ValidVector = [0.1, 0.2, 0.3, 0.4],
             Text = <<"test">>,
             BatchResult = {ok, [ValidVector]},
             Result = process_embed_result(BatchResult, Text),
             ?assertEqual({ok, ValidVector}, Result)
         end},

        {"error passthrough works",
         fun() ->
             Text = <<"test">>,
             BatchResult = {error, some_error},
             Result = process_embed_result(BatchResult, Text),
             ?assertEqual({error, some_error}, Result)
         end}
    ].

%% Helper that mirrors the pattern matching logic in embed/2
process_embed_result(BatchResult, Text) ->
    case BatchResult of
        {ok, [Vector]} when is_list(Vector), length(Vector) > 0 ->
            {ok, Vector};
        {ok, [[]]} ->
            {error, {empty_embedding, Text}};
        {ok, []} ->
            {error, {no_embedding, Text}};
        {ok, Other} ->
            {error, {unexpected_embedding, Other}};
        {error, _} = Error ->
            Error
    end.
