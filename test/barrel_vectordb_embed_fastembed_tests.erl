%%%-------------------------------------------------------------------
%%% @doc Unit tests for barrel_vectordb_embed_fastembed
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_fastembed_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Tests
%%====================================================================

name_test() ->
    ?assertEqual(fastembed, barrel_vectordb_embed_fastembed:name()).

dimension_default_test() ->
    ?assertEqual(384, barrel_vectordb_embed_fastembed:dimension(#{})).

dimension_custom_test() ->
    ?assertEqual(768, barrel_vectordb_embed_fastembed:dimension(#{dimension => 768})).

available_true_test() ->
    %% Create a real port to test with
    Port = open_port({spawn, "cat"}, []),
    try
        Config = #{port => Port},
        ?assertEqual(true, barrel_vectordb_embed_fastembed:available(Config))
    after
        catch port_close(Port)
    end.

available_false_test() ->
    ?assertEqual(false, barrel_vectordb_embed_fastembed:available(#{})).
