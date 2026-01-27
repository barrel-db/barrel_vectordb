%%%-------------------------------------------------------------------
%%% @doc Integration tests for barrel_vectordb_rerank
%%%
%%% These tests require a working Python environment with transformers
%%% and torch installed. Tests will be skipped if dependencies are not
%%% available.
%%%
%%% Setup:
%%%   ./scripts/setup_python_venv.sh
%%%
%%% Run:
%%%   rebar3 eunit --module=barrel_vectordb_rerank_integration_tests
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_rerank_integration_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Descriptions
%%====================================================================

rerank_integration_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(State) ->
         case State of
             skip ->
                 [];
             _ ->
                 %% Use 60 second timeout for each test (model inference can be slow)
                 [
                     {timeout, 60, {"Initialize reranker", fun() -> test_rerank_init(State) end}},
                     {timeout, 60, {"Basic reranking", fun() -> test_rerank_basic(State) end}},
                     {timeout, 60, {"Rerank with top_k", fun() -> test_rerank_top_k(State) end}},
                     {timeout, 60, {"Rerank empty documents", fun() -> test_rerank_empty_docs(State) end}},
                     {timeout, 60, {"Rerank single document", fun() -> test_rerank_single_doc(State) end}}
                 ]
         end
     end}.

%%====================================================================
%% Setup / Cleanup
%%====================================================================

setup() ->
    Python = get_python(),
    case check_python_deps(Python) of
        ok ->
            %% Initialize the python queue (required for rerank concurrency control)
            barrel_embed_python_queue:init(),
            case barrel_vectordb_rerank:init(#{python => Python}) of
                {ok, State} ->
                    State;
                {error, Reason} ->
                    io:format(standard_error,
                              "~n*** Skipping rerank integration tests: ~p~n", [Reason]),
                    skip
            end;
        {error, Reason} ->
            io:format(standard_error,
                      "~n*** Skipping rerank integration tests: ~s~n", [Reason]),
            skip
    end.

cleanup(skip) ->
    ok;
cleanup(State) ->
    barrel_vectordb_rerank:stop(State).

%%====================================================================
%% Tests
%%====================================================================

test_rerank_init(State) ->
    %% Just verify the state is valid from setup
    ?assertEqual(true, barrel_vectordb_rerank:available(State)).

test_rerank_basic(State) ->
    Query = <<"What is machine learning?">>,
    Documents = [
        <<"Machine learning is a subset of artificial intelligence that enables systems to learn from data.">>,
        <<"Python is a popular programming language used for web development.">>,
        <<"Deep learning uses neural networks with many layers to process complex patterns.">>
    ],

    {ok, Results} = barrel_vectordb_rerank:rerank(Query, Documents, State),

    %% Should return all 3 documents
    ?assertEqual(3, length(Results)),

    %% Results should be sorted by score descending
    Scores = [Score || {_Idx, Score} <- Results],
    ?assertEqual(Scores, lists:reverse(lists:sort(Scores))),

    %% The ML-related documents should rank higher than Python doc
    %% (Index 0 = ML, Index 1 = Python, Index 2 = Deep Learning)
    TopIndices = [Idx || {Idx, _Score} <- lists:sublist(Results, 2)],
    ?assertNot(lists:member(1, TopIndices)),

    ok.

test_rerank_top_k(State) ->
    Query = <<"database query optimization">>,
    Documents = [
        <<"SQL databases use indexes to speed up queries.">>,
        <<"Cooking recipes require fresh ingredients.">>,
        <<"Query optimization improves database performance.">>,
        <<"Weather forecasts predict rain tomorrow.">>,
        <<"NoSQL databases offer flexible schemas.">>
    ],

    %% Request only top 2 results
    {ok, Results} = barrel_vectordb_rerank:rerank(Query, Documents, #{top_k => 2}, State),

    %% Should return exactly 2 results
    ?assertEqual(2, length(Results)),

    %% Results should be sorted by score descending
    Scores = [Score || {_Idx, Score} <- Results],
    ?assertEqual(Scores, lists:reverse(lists:sort(Scores))),

    ok.

test_rerank_empty_docs(State) ->
    Query = <<"test query">>,
    Documents = [],

    {ok, Results} = barrel_vectordb_rerank:rerank(Query, Documents, State),

    ?assertEqual([], Results),

    ok.

test_rerank_single_doc(State) ->
    Query = <<"search query">>,
    Documents = [<<"This is the only document.">>],

    {ok, Results} = barrel_vectordb_rerank:rerank(Query, Documents, State),

    ?assertEqual(1, length(Results)),
    [{0, Score}] = Results,
    ?assert(is_float(Score)),

    ok.

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Get the Python executable to use.
%% Tries project venv first, then falls back to system python.
get_python() ->
    %% Try project venv first (relative to test run directory)
    VenvPaths = [
        ".venv/bin/python",
        "../.venv/bin/python",
        "../../.venv/bin/python"
    ],
    case find_python(VenvPaths) of
        {ok, Path} ->
            Path;
        not_found ->
            "python3"
    end.

find_python([]) ->
    not_found;
find_python([Path | Rest]) ->
    case filelib:is_file(Path) of
        true -> {ok, Path};
        false -> find_python(Rest)
    end.

%% @doc Check if Python has required dependencies.
check_python_deps(Python) ->
    %% Try to import transformers and torch
    Cmd = Python ++ " -c \"import transformers; import torch; print('ok')\" 2>/dev/null",
    case os:cmd(Cmd) of
        "ok\n" ->
            ok;
        _ ->
            {error, "Python dependencies not available. Run: ./scripts/setup_python_venv.sh"}
    end.
