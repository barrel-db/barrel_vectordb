%%%-------------------------------------------------------------------
%%% @doc Unit tests for barrel_vectordb_embed_splade
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_splade_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Tests
%%====================================================================

name_test() ->
    ?assertEqual(splade, barrel_vectordb_embed_splade:name()).

dimension_default_test() ->
    ?assertEqual(30522, barrel_vectordb_embed_splade:dimension(#{})).

dimension_custom_test() ->
    ?assertEqual(50000, barrel_vectordb_embed_splade:dimension(#{vocab_size => 50000})).

available_true_test() ->
    %% Create a real port to test with
    Port = open_port({spawn, "cat"}, []),
    try
        Config = #{port => Port},
        ?assertEqual(true, barrel_vectordb_embed_splade:available(Config))
    after
        catch port_close(Port)
    end.

available_false_test() ->
    ?assertEqual(false, barrel_vectordb_embed_splade:available(#{})).
