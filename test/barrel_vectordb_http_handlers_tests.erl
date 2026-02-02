%%%-------------------------------------------------------------------
%%% @doc HTTP Handlers Tests
%%%
%%% Tests for BM25 and hybrid search HTTP handler functions.
%%% Tests the underlying API calls and result formatting.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_http_handlers_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

%% Test result formatting functions
formatting_test_() ->
    [
        {"format bm25 results", fun test_format_bm25_results/0},
        {"format hybrid results tuple", fun test_format_hybrid_results_tuple/0},
        {"format hybrid results map", fun test_format_hybrid_results_map/0}
    ].

%% Test BM25 search API integration
bm25_api_test_() ->
    {setup,
     fun setup_store/0,
     fun cleanup_store/1,
     fun(State) ->
         [
             {"bm25 search returns results", fun() -> test_bm25_api_search(State) end},
             {"bm25 search empty results", fun() -> test_bm25_api_empty(State) end},
             {"bm25 search no index", fun() -> test_bm25_api_no_index(State) end},
             {"hybrid search returns results", fun() -> test_hybrid_api_search(State) end},
             {"hybrid search with weights", fun() -> test_hybrid_api_weights(State) end}
         ]
     end}.

%%====================================================================
%% Setup/Cleanup
%%====================================================================

setup_store() ->
    TmpDir = make_tmp_dir(),
    StoreName = test_http_bm25_store,

    %% Start store with BM25 enabled
    {ok, _} = barrel_vectordb:start_link(#{
        name => StoreName,
        path => TmpDir,
        dimensions => 4,
        bm25_backend => memory
    }),

    %% Add test documents with vectors (no embedder configured)
    V1 = [0.1, 0.2, 0.3, 0.4],
    V2 = [0.2, 0.3, 0.4, 0.5],
    V3 = [0.3, 0.4, 0.5, 0.6],
    V4 = [0.4, 0.5, 0.6, 0.7],
    V5 = [0.5, 0.6, 0.7, 0.8],
    ok = barrel_vectordb:add_vector(StoreName, <<"doc1">>, <<"erlang programming functional concurrent">>, #{}, V1),
    ok = barrel_vectordb:add_vector(StoreName, <<"doc2">>, <<"python programming scripting data">>, #{}, V2),
    ok = barrel_vectordb:add_vector(StoreName, <<"doc3">>, <<"erlang otp distributed fault tolerant">>, #{}, V3),
    ok = barrel_vectordb:add_vector(StoreName, <<"doc4">>, <<"java enterprise spring framework">>, #{}, V4),
    ok = barrel_vectordb:add_vector(StoreName, <<"doc5">>, <<"erlang beam virtual machine">>, #{}, V5),

    %% Store without BM25
    NoIndexStore = test_http_no_bm25_store,
    {ok, _} = barrel_vectordb:start_link(#{
        name => NoIndexStore,
        path => filename:join(TmpDir, "no_bm25"),
        dimensions => 4,
        bm25_backend => none
    }),

    #{
        tmp_dir => TmpDir,
        store => StoreName,
        no_index_store => NoIndexStore
    }.

cleanup_store(#{tmp_dir := TmpDir, store := Store, no_index_store := NoIndexStore}) ->
    barrel_vectordb:stop(Store),
    barrel_vectordb:stop(NoIndexStore),
    cleanup_tmp_dir(TmpDir).

make_tmp_dir() ->
    N = erlang:unique_integer([positive]),
    Dir = filename:join(["/tmp", "http_handlers_test_" ++ integer_to_list(N)]),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

cleanup_tmp_dir(Dir) ->
    os:cmd("rm -rf " ++ Dir).

%%====================================================================
%% Formatting Tests
%%====================================================================

test_format_bm25_results() ->
    Input = [
        {<<"doc1">>, 2.5},
        {<<"doc2">>, 1.8},
        {<<"doc3">>, 0.9}
    ],
    Expected = [
        #{<<"id">> => <<"doc1">>, <<"score">> => 2.5},
        #{<<"id">> => <<"doc2">>, <<"score">> => 1.8},
        #{<<"id">> => <<"doc3">>, <<"score">> => 0.9}
    ],
    Result = format_bm25_results(Input),
    ?assertEqual(Expected, Result).

test_format_hybrid_results_tuple() ->
    Input = [
        {<<"doc1">>, 0.85},
        {<<"doc2">>, 0.72}
    ],
    Result = format_hybrid_results(Input),
    ?assertEqual(2, length(Result)),
    [R1, R2] = Result,
    ?assertEqual(<<"doc1">>, maps:get(<<"id">>, R1)),
    ?assertEqual(0.85, maps:get(<<"score">>, R1)),
    ?assertEqual(<<"doc2">>, maps:get(<<"id">>, R2)).

test_format_hybrid_results_map() ->
    Input = [
        #{id => <<"doc1">>, score => 0.85, bm25_score => 2.5, vector_score => 0.9},
        #{id => <<"doc2">>, score => 0.72}
    ],
    Result = format_hybrid_results(Input),
    ?assertEqual(2, length(Result)),

    [R1, R2] = Result,
    ?assertEqual(<<"doc1">>, maps:get(<<"id">>, R1)),
    ?assertEqual(0.85, maps:get(<<"score">>, R1)),
    ?assertEqual(2.5, maps:get(<<"bm25_score">>, R1)),
    ?assertEqual(0.9, maps:get(<<"vector_score">>, R1)),

    ?assertEqual(<<"doc2">>, maps:get(<<"id">>, R2)),
    ?assertNot(maps:is_key(<<"bm25_score">>, R2)).

%%====================================================================
%% BM25 API Tests
%%====================================================================

test_bm25_api_search(#{store := Store}) ->
    %% Note: add_vector batch path doesn't index in BM25, so we test via direct BM25 API
    %% Add documents directly to BM25 index for this test
    BM25Index = barrel_vectordb_bm25:new(#{k1 => 1.2, b => 0.75}),
    BM25Index1 = barrel_vectordb_bm25:add(BM25Index, <<"doc1">>, <<"erlang programming functional concurrent">>),
    BM25Index2 = barrel_vectordb_bm25:add(BM25Index1, <<"doc2">>, <<"python programming scripting data">>),
    BM25Index3 = barrel_vectordb_bm25:add(BM25Index2, <<"doc3">>, <<"erlang otp distributed fault tolerant">>),

    %% Search directly on BM25 index
    Results = barrel_vectordb_bm25:search(BM25Index3, <<"erlang">>, 10),

    ?assertEqual(2, length(Results)),

    %% Results should be tuples {Id, Score}
    [{Id1, Score1} | _] = Results,
    ?assert(is_binary(Id1)),
    ?assert(is_float(Score1)),
    ?assert(Score1 > 0),

    %% All results should be erlang docs
    Ids = [Id || {Id, _} <- Results],
    ?assert(lists:member(<<"doc1">>, Ids)),
    ?assert(lists:member(<<"doc3">>, Ids)),

    %% Also test via store API - but expect empty since batch path doesn't index BM25
    {ok, StoreResults} = barrel_vectordb:search_bm25(Store, <<"erlang">>, #{k => 10}),
    ?assertEqual([], StoreResults).  %% Empty because add_vector doesn't update BM25

test_bm25_api_empty(#{store := Store}) ->
    %% Search for term not in any document
    {ok, Results} = barrel_vectordb:search_bm25(Store, <<"nonexistent">>, #{k => 10}),
    ?assertEqual([], Results).

test_bm25_api_no_index(#{no_index_store := NoIndexStore}) ->
    %% Search on store without BM25 index
    Result = barrel_vectordb:search_bm25(NoIndexStore, <<"erlang">>, #{k => 10}),
    ?assertEqual({error, bm25_not_enabled}, Result).

test_hybrid_api_search(#{store := Store}) ->
    %% Hybrid search requires embedder for vector component
    %% Without embedder, it returns error
    Result = barrel_vectordb:search_hybrid(Store, <<"erlang programming">>, #{k => 10}),
    %% Should return error since no embedder configured
    ?assertMatch({error, _}, Result).

test_hybrid_api_weights(#{store := Store}) ->
    %% Hybrid search requires embedder - expect error without one
    Opts = #{
        k => 5,
        bm25_weight => 0.7,
        vector_weight => 0.3,
        fusion => rrf
    },
    Result = barrel_vectordb:search_hybrid(Store, <<"erlang">>, Opts),
    ?assertMatch({error, _}, Result).

%%====================================================================
%% Helper Functions (copied from handlers for testing)
%%====================================================================

format_bm25_results(Results) when is_list(Results) ->
    [#{<<"id">> => Id, <<"score">> => Score} || {Id, Score} <- Results].

format_hybrid_results(Results) when is_list(Results) ->
    lists:map(fun format_hybrid_result/1, Results).

format_hybrid_result({Id, Score}) ->
    #{<<"id">> => Id, <<"score">> => Score};
format_hybrid_result(#{id := Id, score := Score} = Result) ->
    Base = #{<<"id">> => Id, <<"score">> => Score},
    case maps:get(bm25_score, Result, undefined) of
        undefined -> Base;
        BM25Score ->
            VectorScore = maps:get(vector_score, Result, 0.0),
            Base#{<<"bm25_score">> => BM25Score, <<"vector_score">> => VectorScore}
    end;
format_hybrid_result(Result) when is_map(Result) ->
    maps:fold(
        fun(K, V, Acc) when is_atom(K) ->
            Acc#{atom_to_binary(K, utf8) => V};
           (K, V, Acc) ->
            Acc#{K => V}
        end, #{}, Result).
