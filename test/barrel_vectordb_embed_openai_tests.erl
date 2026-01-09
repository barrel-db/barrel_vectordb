%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_embed_openai module
%%%
%%% These tests use meck to mock HTTP responses, so OpenAI API doesn't
%%% need to be called. For integration tests with real OpenAI API,
%%% see test/integration/README.md.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_openai_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

openai_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
       {"name returns openai", fun test_name/0},
       {"init with api key in config", fun test_init_with_config_key/0},
       {"init with api key in env", fun test_init_with_env_key/0},
       {"init fails without api key", fun test_init_no_api_key/0},
       {"init with custom config", fun test_init_custom/0},
       {"init fails without hackney", fun test_init_no_hackney/0},
       {"available returns true when API responds", fun test_available_true/0},
       {"available returns false when API errors", fun test_available_false/0},
       {"available returns false without api key", fun test_available_no_key/0},
       {"embed single text", fun test_embed/0},
       {"embed with custom model", fun test_embed_custom_model/0},
       {"embed handles error response", fun test_embed_error/0},
       {"embed handles auth error", fun test_embed_auth_error/0},
       {"embed handles connection failure", fun test_embed_connection_fail/0},
       {"embed_batch processes multiple texts", fun test_embed_batch/0},
       {"embed_batch preserves order", fun test_embed_batch_order/0},
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
    %% Store original env value and clear it for tests
    OriginalApiKey = os:getenv("OPENAI_API_KEY"),
    os:unsetenv("OPENAI_API_KEY"),
    {OriginalApiKey}.

cleanup({OriginalApiKey}) ->
    meck:unload(hackney),
    meck:unload(application),
    %% Restore original env value
    case OriginalApiKey of
        false -> ok;
        Key -> os:putenv("OPENAI_API_KEY", Key)
    end,
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_name() ->
    ?assertEqual(openai, barrel_vectordb_embed_openai:name()).

test_init_with_config_key() ->
    {ok, Config} = barrel_vectordb_embed_openai:init(#{api_key => <<"sk-test123">>}),
    ?assertEqual(<<"sk-test123">>, maps:get(api_key, Config)),
    ?assertEqual(<<"https://api.openai.com/v1">>, maps:get(url, Config)),
    ?assertEqual(<<"text-embedding-3-small">>, maps:get(model, Config)),
    ?assertEqual(30000, maps:get(timeout, Config)),
    ?assertEqual(1536, maps:get(dimension, Config)).

test_init_with_env_key() ->
    os:putenv("OPENAI_API_KEY", "sk-from-env"),
    {ok, Config} = barrel_vectordb_embed_openai:init(#{}),
    os:unsetenv("OPENAI_API_KEY"),
    ?assertEqual(<<"sk-from-env">>, maps:get(api_key, Config)).

test_init_no_api_key() ->
    Result = barrel_vectordb_embed_openai:init(#{}),
    ?assertEqual({error, api_key_not_configured}, Result).

test_init_custom() ->
    {ok, Config} = barrel_vectordb_embed_openai:init(#{
        api_key => <<"sk-custom">>,
        url => <<"https://custom.openai.azure.com/v1">>,
        model => <<"text-embedding-3-large">>,
        timeout => 60000,
        dimension => 3072
    }),
    ?assertEqual(<<"https://custom.openai.azure.com/v1">>, maps:get(url, Config)),
    ?assertEqual(<<"text-embedding-3-large">>, maps:get(model, Config)),
    ?assertEqual(60000, maps:get(timeout, Config)),
    ?assertEqual(3072, maps:get(dimension, Config)).

test_init_no_hackney() ->
    meck:expect(application, ensure_all_started, fun(hackney) ->
        {error, {hackney, {"could not start", enoent}}}
    end),
    Result = barrel_vectordb_embed_openai:init(#{api_key => <<"sk-test">>}),
    ?assertMatch({error, {hackney_start_failed, _}}, Result).

test_available_true() ->
    meck:expect(hackney, request, fun(get, Url, _Headers, <<>>, _Opts) ->
        ?assertEqual(<<"https://api.openai.com/v1/models">>, Url),
        {ok, 200, [], make_ref()}
    end),
    meck:expect(hackney, skip_body, fun(_) -> ok end),

    {ok, Config} = barrel_vectordb_embed_openai:init(#{api_key => <<"sk-test">>}),
    ?assertEqual(true, barrel_vectordb_embed_openai:available(Config)).

test_available_false() ->
    meck:expect(hackney, request, fun(get, _Url, _Headers, <<>>, _Opts) ->
        {ok, 401, [], make_ref()}
    end),
    meck:expect(hackney, skip_body, fun(_) -> ok end),

    {ok, Config} = barrel_vectordb_embed_openai:init(#{api_key => <<"sk-invalid">>}),
    ?assertEqual(false, barrel_vectordb_embed_openai:available(Config)).

test_available_no_key() ->
    %% Config without api_key
    ?assertEqual(false, barrel_vectordb_embed_openai:available(#{})).

test_embed() ->
    MockVector = [0.1, 0.2, 0.3],
    meck:expect(hackney, request, fun(post, Url, Headers, Body, _Opts) ->
        ?assertEqual(<<"https://api.openai.com/v1/embeddings">>, Url),
        %% Verify auth header
        ?assert(lists:any(fun({<<"Authorization">>, V}) ->
            V =:= <<"Bearer sk-test">>
        end, Headers)),
        %% Verify body
        Decoded = json:decode(Body),
        ?assertEqual(<<"text-embedding-3-small">>, maps:get(<<"model">>, Decoded)),
        ?assertEqual([<<"hello world">>], maps:get(<<"input">>, Decoded)),
        {ok, 200, [], make_ref()}
    end),
    meck:expect(hackney, body, fun(_) ->
        {ok, iolist_to_binary(json:encode(#{
            <<"data">> => [
                #{<<"embedding">> => MockVector, <<"index">> => 0}
            ]
        }))}
    end),

    {ok, Config} = barrel_vectordb_embed_openai:init(#{api_key => <<"sk-test">>}),
    {ok, Vector} = barrel_vectordb_embed_openai:embed(<<"hello world">>, Config),
    ?assertEqual(MockVector, Vector).

test_embed_custom_model() ->
    MockVector = lists:duplicate(3072, 0.5),
    meck:expect(hackney, request, fun(post, _Url, _Headers, Body, _Opts) ->
        Decoded = json:decode(Body),
        ?assertEqual(<<"text-embedding-3-large">>, maps:get(<<"model">>, Decoded)),
        {ok, 200, [], make_ref()}
    end),
    meck:expect(hackney, body, fun(_) ->
        {ok, iolist_to_binary(json:encode(#{
            <<"data">> => [#{<<"embedding">> => MockVector, <<"index">> => 0}]
        }))}
    end),

    {ok, Config} = barrel_vectordb_embed_openai:init(#{
        api_key => <<"sk-test">>,
        model => <<"text-embedding-3-large">>
    }),
    {ok, Vector} = barrel_vectordb_embed_openai:embed(<<"test">>, Config),
    ?assertEqual(3072, length(Vector)).

test_embed_error() ->
    meck:expect(hackney, request, fun(post, _Url, _Headers, _Body, _Opts) ->
        {ok, 500, [], make_ref()}
    end),
    meck:expect(hackney, body, fun(_) ->
        {ok, <<"{\"error\": {\"message\": \"Internal Server Error\"}}">>}
    end),

    {ok, Config} = barrel_vectordb_embed_openai:init(#{api_key => <<"sk-test">>}),
    Result = barrel_vectordb_embed_openai:embed(<<"test">>, Config),
    ?assertMatch({error, {http_error, 500, _}}, Result).

test_embed_auth_error() ->
    meck:expect(hackney, request, fun(post, _Url, _Headers, _Body, _Opts) ->
        {ok, 401, [], make_ref()}
    end),
    meck:expect(hackney, body, fun(_) ->
        {ok, <<"{\"error\": {\"message\": \"Invalid API key\"}}">>}
    end),

    {ok, Config} = barrel_vectordb_embed_openai:init(#{api_key => <<"sk-invalid">>}),
    Result = barrel_vectordb_embed_openai:embed(<<"test">>, Config),
    ?assertMatch({error, {http_error, 401, _}}, Result).

test_embed_connection_fail() ->
    meck:expect(hackney, request, fun(post, _Url, _Headers, _Body, _Opts) ->
        {error, econnrefused}
    end),

    {ok, Config} = barrel_vectordb_embed_openai:init(#{api_key => <<"sk-test">>}),
    Result = barrel_vectordb_embed_openai:embed(<<"test">>, Config),
    ?assertEqual({error, {request_failed, econnrefused}}, Result).

test_embed_batch() ->
    meck:expect(hackney, request, fun(post, _Url, _Headers, Body, _Opts) ->
        Decoded = json:decode(Body),
        Input = maps:get(<<"input">>, Decoded),
        ?assertEqual([<<"one">>, <<"two">>, <<"three">>], Input),
        {ok, 200, [], make_ref()}
    end),
    meck:expect(hackney, body, fun(_) ->
        {ok, iolist_to_binary(json:encode(#{
            <<"data">> => [
                #{<<"embedding">> => [0.1, 0.2], <<"index">> => 0},
                #{<<"embedding">> => [0.3, 0.4], <<"index">> => 1},
                #{<<"embedding">> => [0.5, 0.6], <<"index">> => 2}
            ]
        }))}
    end),

    {ok, Config} = barrel_vectordb_embed_openai:init(#{api_key => <<"sk-test">>}),
    Texts = [<<"one">>, <<"two">>, <<"three">>],
    {ok, Vectors} = barrel_vectordb_embed_openai:embed_batch(Texts, Config),

    ?assertEqual(3, length(Vectors)),
    ?assertEqual([0.1, 0.2], lists:nth(1, Vectors)),
    ?assertEqual([0.3, 0.4], lists:nth(2, Vectors)),
    ?assertEqual([0.5, 0.6], lists:nth(3, Vectors)).

test_embed_batch_order() ->
    %% OpenAI may return results out of order, verify we sort by index
    meck:expect(hackney, request, fun(post, _Url, _Headers, _Body, _Opts) ->
        {ok, 200, [], make_ref()}
    end),
    meck:expect(hackney, body, fun(_) ->
        %% Return out of order
        {ok, iolist_to_binary(json:encode(#{
            <<"data">> => [
                #{<<"embedding">> => [0.5, 0.6], <<"index">> => 2},
                #{<<"embedding">> => [0.1, 0.2], <<"index">> => 0},
                #{<<"embedding">> => [0.3, 0.4], <<"index">> => 1}
            ]
        }))}
    end),

    {ok, Config} = barrel_vectordb_embed_openai:init(#{api_key => <<"sk-test">>}),
    {ok, Vectors} = barrel_vectordb_embed_openai:embed_batch([<<"a">>, <<"b">>, <<"c">>], Config),

    %% Should be sorted by original order
    ?assertEqual([0.1, 0.2], lists:nth(1, Vectors)),
    ?assertEqual([0.3, 0.4], lists:nth(2, Vectors)),
    ?assertEqual([0.5, 0.6], lists:nth(3, Vectors)).

test_dimension() ->
    ?assertEqual(1536, barrel_vectordb_embed_openai:dimension(#{})),
    ?assertEqual(3072, barrel_vectordb_embed_openai:dimension(#{dimension => 3072})).
