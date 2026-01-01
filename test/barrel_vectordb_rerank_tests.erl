%%%-------------------------------------------------------------------
%%% @doc Unit tests for barrel_vectordb_rerank
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_rerank_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Tests
%%====================================================================

available_true_test() ->
    %% Create a real port to test with
    Port = open_port({spawn, "cat"}, []),
    try
        State = #{port => Port, model => <<"test">>, timeout => 5000},
        ?assertEqual(true, barrel_vectordb_rerank:available(State))
    after
        catch port_close(Port)
    end.

available_false_test() ->
    ?assertEqual(false, barrel_vectordb_rerank:available(#{})).

available_no_port_test() ->
    State = #{model => <<"test">>, timeout => 5000},
    ?assertEqual(false, barrel_vectordb_rerank:available(State)).

stop_with_port_test() ->
    Port = open_port({spawn, "cat"}, []),
    State = #{port => Port, model => <<"test">>, timeout => 5000},
    ?assertEqual(ok, barrel_vectordb_rerank:stop(State)),
    %% Port should be closed now
    ?assertEqual(undefined, erlang:port_info(Port)).

stop_without_port_test() ->
    ?assertEqual(ok, barrel_vectordb_rerank:stop(#{})).

rerank_not_initialized_test() ->
    Query = <<"test query">>,
    Docs = [<<"doc1">>, <<"doc2">>],
    ?assertEqual({error, not_initialized}, barrel_vectordb_rerank:rerank(Query, Docs, #{})).
