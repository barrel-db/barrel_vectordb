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
