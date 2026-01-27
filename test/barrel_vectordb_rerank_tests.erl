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
    %% available/1 checks if the server pid is alive
    Self = self(),
    ?assertEqual(true, barrel_vectordb_rerank:available(Self)).

available_false_test() ->
    %% A non-existent pid should return false
    %% Use a known dead pid (the init process's first spawned process is long dead)
    DeadPid = list_to_pid("<0.0.1>"),
    ?assertEqual(false, barrel_vectordb_rerank:available(DeadPid)).
