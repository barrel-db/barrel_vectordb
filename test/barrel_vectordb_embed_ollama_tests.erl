%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_embed_ollama module
%%%
%%% These tests use meck to mock HTTP responses, so Ollama doesn't
%%% need to be running. For integration tests with a real Ollama
%%% server, see test/integration/README.md.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_ollama_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

ollama_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
       {"name returns ollama", fun test_name/0},
       {"init with defaults", fun test_init_defaults/0},
       {"init with custom config", fun test_init_custom/0},
       {"init fails without hackney", fun test_init_no_hackney/0},
       {"available returns true when server responds", fun test_available_true/0},
       {"available returns false when server down", fun test_available_false/0},
       {"embed single text", fun test_embed/0},
       {"embed with custom model", fun test_embed_custom_model/0},
       {"embed handles error response", fun test_embed_error/0},
       {"embed handles connection failure", fun test_embed_connection_fail/0},
       {"embed_batch processes multiple texts", fun test_embed_batch/0},
       {"embed_batch fails if any text fails", fun test_embed_batch_partial_fail/0},
       {"dimension returns configured value", fun test_dimension/0}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    %% Mock hackney for HTTP requests
    meck:new(hackney, [passthrough]),
    meck:new(application, [unstick, passthrough]),
    meck:expect(application, ensure_all_started, fun(hackney) -> {ok, [hackney]} end),
    ok.

cleanup(_) ->
    meck:unload(hackney),
    meck:unload(application),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_name() ->
    ?assertEqual(ollama, barrel_vectordb_embed_ollama:name()).

test_init_defaults() ->
    {ok, Config} = barrel_vectordb_embed_ollama:init(#{}),
    ?assertEqual(<<"http://localhost:11434">>, maps:get(url, Config)),
    ?assertEqual(<<"nomic-embed-text">>, maps:get(model, Config)),
    ?assertEqual(30000, maps:get(timeout, Config)),
    ?assertEqual(768, maps:get(dimension, Config)).

test_init_custom() ->
    {ok, Config} = barrel_vectordb_embed_ollama:init(#{
        url => <<"http://gpu-server:11434">>,
        model => <<"mxbai-embed-large">>,
        timeout => 60000,
        dimension => 1024
    }),
    ?assertEqual(<<"http://gpu-server:11434">>, maps:get(url, Config)),
    ?assertEqual(<<"mxbai-embed-large">>, maps:get(model, Config)),
    ?assertEqual(60000, maps:get(timeout, Config)),
    ?assertEqual(1024, maps:get(dimension, Config)).

test_init_no_hackney() ->
    meck:expect(application, ensure_all_started, fun(hackney) ->
        {error, {hackney, {"could not start", enoent}}}
    end),
    Result = barrel_vectordb_embed_ollama:init(#{}),
    ?assertMatch({error, {hackney_start_failed, _}}, Result).

test_available_true() ->
    meck:expect(hackney, request, fun(get, Url, [], <<>>, _Opts) ->
        ?assertEqual(<<"http://localhost:11434/api/tags">>, Url),
        {ok, 200, [], make_ref()}
    end),
    meck:expect(hackney, skip_body, fun(_) -> ok end),

    {ok, Config} = barrel_vectordb_embed_ollama:init(#{}),
    ?assertEqual(true, barrel_vectordb_embed_ollama:available(Config)).

test_available_false() ->
    meck:expect(hackney, request, fun(get, _Url, [], <<>>, _Opts) ->
        {error, econnrefused}
    end),

    {ok, Config} = barrel_vectordb_embed_ollama:init(#{}),
    ?assertEqual(false, barrel_vectordb_embed_ollama:available(Config)).

test_embed() ->
    MockVector = [0.1, 0.2, 0.3],
    meck:expect(hackney, request, fun(post, Url, _Headers, Body, _Opts) ->
        ?assertEqual(<<"http://localhost:11434/api/embeddings">>, Url),
        Decoded = jsx:decode(Body, [return_maps]),
        ?assertEqual(<<"nomic-embed-text">>, maps:get(<<"model">>, Decoded)),
        ?assertEqual(<<"hello world">>, maps:get(<<"prompt">>, Decoded)),
        {ok, 200, [], make_ref()}
    end),
    meck:expect(hackney, body, fun(_) ->
        {ok, jsx:encode(#{<<"embedding">> => MockVector})}
    end),

    {ok, Config} = barrel_vectordb_embed_ollama:init(#{}),
    {ok, Vector} = barrel_vectordb_embed_ollama:embed(<<"hello world">>, Config),
    ?assertEqual(MockVector, Vector).

test_embed_custom_model() ->
    MockVector = lists:duplicate(1024, 0.5),
    meck:expect(hackney, request, fun(post, _Url, _Headers, Body, _Opts) ->
        Decoded = jsx:decode(Body, [return_maps]),
        ?assertEqual(<<"mxbai-embed-large">>, maps:get(<<"model">>, Decoded)),
        {ok, 200, [], make_ref()}
    end),
    meck:expect(hackney, body, fun(_) ->
        {ok, jsx:encode(#{<<"embedding">> => MockVector})}
    end),

    {ok, Config} = barrel_vectordb_embed_ollama:init(#{model => <<"mxbai-embed-large">>}),
    {ok, Vector} = barrel_vectordb_embed_ollama:embed(<<"test">>, Config),
    ?assertEqual(1024, length(Vector)).

test_embed_error() ->
    meck:expect(hackney, request, fun(post, _Url, _Headers, _Body, _Opts) ->
        {ok, 500, [], make_ref()}
    end),
    meck:expect(hackney, body, fun(_) ->
        {ok, <<"Internal Server Error">>}
    end),

    {ok, Config} = barrel_vectordb_embed_ollama:init(#{}),
    Result = barrel_vectordb_embed_ollama:embed(<<"test">>, Config),
    ?assertMatch({error, {http_error, 500, _}}, Result).

test_embed_connection_fail() ->
    meck:expect(hackney, request, fun(post, _Url, _Headers, _Body, _Opts) ->
        {error, econnrefused}
    end),

    {ok, Config} = barrel_vectordb_embed_ollama:init(#{}),
    Result = barrel_vectordb_embed_ollama:embed(<<"test">>, Config),
    ?assertEqual({error, {request_failed, econnrefused}}, Result).

test_embed_batch() ->
    CallCount = counters:new(1, []),
    meck:expect(hackney, request, fun(post, _Url, _Headers, Body, _Opts) ->
        counters:add(CallCount, 1, 1),
        Decoded = jsx:decode(Body, [return_maps]),
        Prompt = maps:get(<<"prompt">>, Decoded),
        {ok, 200, [], {ref, Prompt}}
    end),
    meck:expect(hackney, body, fun({ref, Prompt}) ->
        %% Generate deterministic vector based on prompt
        Hash = erlang:phash2(Prompt, 1000),
        Vec = [Hash / 1000.0, (Hash + 1) / 1000.0, (Hash + 2) / 1000.0],
        {ok, jsx:encode(#{<<"embedding">> => Vec})}
    end),

    {ok, Config} = barrel_vectordb_embed_ollama:init(#{}),
    Texts = [<<"one">>, <<"two">>, <<"three">>],
    {ok, Vectors} = barrel_vectordb_embed_ollama:embed_batch(Texts, Config),

    ?assertEqual(3, length(Vectors)),
    ?assertEqual(3, counters:get(CallCount, 1)),
    %% Each vector should be different
    [V1, V2, V3] = Vectors,
    ?assertNotEqual(V1, V2),
    ?assertNotEqual(V2, V3).

test_embed_batch_partial_fail() ->
    CallCount = counters:new(1, []),
    meck:expect(hackney, request, fun(post, _Url, _Headers, _Body, _Opts) ->
        counters:add(CallCount, 1, 1),
        case counters:get(CallCount, 1) of
            2 -> {error, timeout};
            _ -> {ok, 200, [], make_ref()}
        end
    end),
    meck:expect(hackney, body, fun(_) ->
        {ok, jsx:encode(#{<<"embedding">> => [0.1, 0.2, 0.3]})}
    end),

    {ok, Config} = barrel_vectordb_embed_ollama:init(#{}),
    Texts = [<<"one">>, <<"two">>, <<"three">>],
    Result = barrel_vectordb_embed_ollama:embed_batch(Texts, Config),

    ?assertEqual({error, {request_failed, timeout}}, Result).

test_dimension() ->
    ?assertEqual(768, barrel_vectordb_embed_ollama:dimension(#{})),
    ?assertEqual(1024, barrel_vectordb_embed_ollama:dimension(#{dimension => 1024})).
