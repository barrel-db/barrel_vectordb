%%%-------------------------------------------------------------------
%%% @doc Unit tests for barrel_vectordb_embed_colbert
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_colbert_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Tests
%%====================================================================

name_test() ->
    ?assertEqual(colbert, barrel_vectordb_embed_colbert:name()).

dimension_default_test() ->
    ?assertEqual(128, barrel_vectordb_embed_colbert:dimension(#{})).

dimension_custom_test() ->
    ?assertEqual(96, barrel_vectordb_embed_colbert:dimension(#{dimension => 96})).

available_true_test() ->
    %% Create a real port to test with
    Port = open_port({spawn, "cat"}, []),
    try
        Config = #{port => Port},
        ?assertEqual(true, barrel_vectordb_embed_colbert:available(Config))
    after
        catch port_close(Port)
    end.

available_false_test() ->
    ?assertEqual(false, barrel_vectordb_embed_colbert:available(#{})).

maxsim_score_test() ->
    %% Test MaxSim scoring
    QueryVecs = [[1.0, 0.0], [0.0, 1.0]],
    DocVecs = [[0.5, 0.5], [1.0, 0.0], [0.0, 1.0]],
    %% Query[0] = [1.0, 0.0] -> max dot with Doc[1] = [1.0, 0.0] = 1.0
    %% Query[1] = [0.0, 1.0] -> max dot with Doc[2] = [0.0, 1.0] = 1.0
    %% Total = 1.0 + 1.0 = 2.0
    Score = barrel_vectordb_embed_colbert:maxsim_score(QueryVecs, DocVecs),
    ?assertEqual(2.0, Score).

maxsim_score_partial_match_test() ->
    %% Test with partial matches
    QueryVecs = [[1.0, 0.0]],
    DocVecs = [[0.5, 0.5]],
    %% Dot product = 1.0 * 0.5 + 0.0 * 0.5 = 0.5
    Score = barrel_vectordb_embed_colbert:maxsim_score(QueryVecs, DocVecs),
    ?assertEqual(0.5, Score).
