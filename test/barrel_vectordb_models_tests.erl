%%%-------------------------------------------------------------------
%%% @doc Unit tests for barrel_vectordb_models
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_models_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Setup
%%====================================================================

setup() ->
    %% Clear cache before each test
    catch persistent_term:erase({barrel_vectordb_models, models_cache}),
    ok.

cleanup(_) ->
    catch persistent_term:erase({barrel_vectordb_models, models_cache}),
    ok.

%%====================================================================
%% Test Generators
%%====================================================================

models_test_() ->
    {foreach,
        fun setup/0,
        fun cleanup/1,
        [
            {"types returns all model types", fun test_types/0},
            {"list all models", fun test_list_all/0},
            {"list text models", fun test_list_text/0},
            {"list sparse models", fun test_list_sparse/0},
            {"list late_interaction models", fun test_list_late_interaction/0},
            {"list image models", fun test_list_image/0},
            {"list rerank models", fun test_list_rerank/0},
            {"list unknown type returns error", fun test_list_unknown_type/0},
            {"info for known model", fun test_info_known_model/0},
            {"info for unknown model", fun test_info_unknown_model/0},
            {"info accepts string", fun test_info_string/0},
            {"default text model", fun test_default_text/0},
            {"dimensions for known model", fun test_dimensions_known/0},
            {"dimensions for model without dims", fun test_dimensions_no_dims/0},
            {"reload refreshes cache", fun test_reload/0}
        ]
    }.

%%====================================================================
%% Tests
%%====================================================================

test_types() ->
    Types = barrel_vectordb_models:types(),
    ?assertEqual(5, length(Types)),
    ?assert(lists:member(text, Types)),
    ?assert(lists:member(sparse, Types)),
    ?assert(lists:member(late_interaction, Types)),
    ?assert(lists:member(image, Types)),
    ?assert(lists:member(rerank, Types)).

test_list_all() ->
    {ok, All} = barrel_vectordb_models:list(),
    ?assert(is_map(All)),
    ?assert(maps:is_key(text, All)),
    ?assert(maps:is_key(sparse, All)),
    ?assert(maps:is_key(late_interaction, All)),
    ?assert(maps:is_key(image, All)),
    ?assert(maps:is_key(rerank, All)).

test_list_text() ->
    {ok, Models} = barrel_vectordb_models:list(text),
    ?assert(is_list(Models)),
    ?assert(length(Models) > 0),
    %% Check that default model is in the list
    Names = [maps:get(<<"name">>, M) || M <- Models],
    ?assert(lists:member(<<"BAAI/bge-base-en-v1.5">>, Names)).

test_list_sparse() ->
    {ok, Models} = barrel_vectordb_models:list(sparse),
    ?assert(is_list(Models)),
    ?assert(length(Models) > 0),
    %% Check BM25 is present
    Names = [maps:get(<<"name">>, M) || M <- Models],
    ?assert(lists:member(<<"bm25">>, Names)).

test_list_late_interaction() ->
    {ok, Models} = barrel_vectordb_models:list(late_interaction),
    ?assert(is_list(Models)),
    ?assert(length(Models) > 0),
    %% Check ColBERT is present
    Names = [maps:get(<<"name">>, M) || M <- Models],
    ?assert(lists:member(<<"colbert-ir/colbertv2.0">>, Names)).

test_list_image() ->
    {ok, Models} = barrel_vectordb_models:list(image),
    ?assert(is_list(Models)),
    ?assert(length(Models) > 0),
    %% Check CLIP is present
    Names = [maps:get(<<"name">>, M) || M <- Models],
    ?assert(lists:member(<<"Qdrant/clip-ViT-B-32-vision">>, Names)).

test_list_rerank() ->
    {ok, Models} = barrel_vectordb_models:list(rerank),
    ?assert(is_list(Models)),
    ?assert(length(Models) > 0).

test_list_unknown_type() ->
    Result = barrel_vectordb_models:list(unknown_type),
    ?assertMatch({error, {unknown_type, unknown_type}}, Result).

test_info_known_model() ->
    {ok, Info} = barrel_vectordb_models:info(<<"BAAI/bge-base-en-v1.5">>),
    ?assertEqual(<<"BAAI/bge-base-en-v1.5">>, maps:get(<<"name">>, Info)),
    ?assertEqual(768, maps:get(<<"dimensions">>, Info)),
    ?assertEqual(true, maps:get(<<"default">>, Info, false)).

test_info_unknown_model() ->
    Result = barrel_vectordb_models:info(<<"unknown/model">>),
    ?assertEqual({error, model_not_found}, Result).

test_info_string() ->
    {ok, Info} = barrel_vectordb_models:info("BAAI/bge-base-en-v1.5"),
    ?assertEqual(<<"BAAI/bge-base-en-v1.5">>, maps:get(<<"name">>, Info)).

test_default_text() ->
    {ok, Model} = barrel_vectordb_models:default(text),
    ?assertEqual(<<"BAAI/bge-base-en-v1.5">>, maps:get(<<"name">>, Model)),
    ?assertEqual(true, maps:get(<<"default">>, Model)).

test_dimensions_known() ->
    {ok, Dims} = barrel_vectordb_models:dimensions(<<"BAAI/bge-base-en-v1.5">>),
    ?assertEqual(768, Dims).

test_dimensions_no_dims() ->
    %% Rerank models don't have dimensions
    Result = barrel_vectordb_models:dimensions(<<"BAAI/bge-reranker-base">>),
    ?assertEqual({error, no_dimensions}, Result).

test_reload() ->
    %% First load
    {ok, _} = barrel_vectordb_models:list(),

    %% Verify cached
    ?assertNotEqual(undefined, persistent_term:get({barrel_vectordb_models, models_cache}, undefined)),

    %% Reload
    ok = barrel_vectordb_models:reload(),

    %% Should still work
    {ok, Models} = barrel_vectordb_models:list(text),
    ?assert(length(Models) > 0).
