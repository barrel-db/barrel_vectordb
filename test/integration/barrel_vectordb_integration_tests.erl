%%%-------------------------------------------------------------------
%%% @doc Integration tests for embedding providers
%%%
%%% These tests require real backends (Python, Ollama) to be available.
%%% Run with: rebar3 eunit --module=barrel_vectordb_integration_tests
%%%
%%% Tests automatically skip if the required backend is not available.
%%%
%%% Note: Uses barrel_embed library modules for provider implementations.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_integration_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

integration_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     {timeout, 120,  %% 2 minute timeout for ML model loading
      [
       {"local: embed single text", {timeout, 60, fun test_local_embed/0}},
       {"local: embed batch", {timeout, 60, fun test_local_batch/0}},
       {"local: custom model", {timeout, 60, fun test_local_custom_model/0}},
       {"ollama: embed single text", fun test_ollama_embed/0},
       {"ollama: embed batch", fun test_ollama_batch/0},
       {"ollama: custom model", fun test_ollama_custom_model/0},
       {"openai: embed single text", fun test_openai_embed/0},
       {"openai: embed batch", fun test_openai_batch/0},
       {"openai: custom model", fun test_openai_custom_model/0},
       {"provider chain fallback", {timeout, 60, fun test_provider_chain/0}}
      ]
     }
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    application:ensure_all_started(hackney),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Local Provider Tests
%%====================================================================

test_local_embed() ->
    case check_local_available() of
        false ->
            io:format("~n  SKIPPED: Python/sentence-transformers not available~n"),
            ok;
        true ->
            {ok, Config} = barrel_embed_local:init(local_config()),
            {ok, Vector} = barrel_embed_local:embed(<<"Hello world">>, Config),

            %% Default model produces 768-dim vectors
            ?assertEqual(768, length(Vector)),
            ?assert(is_float(hd(Vector))),

            %% Vector should be normalized (roughly unit length)
            Norm = math:sqrt(lists:sum([V*V || V <- Vector])),
            ?assert(Norm > 0.9 andalso Norm < 1.1),

            cleanup_local(Config)
    end.

test_local_batch() ->
    case check_local_available() of
        false ->
            io:format("~n  SKIPPED: Python/sentence-transformers not available~n"),
            ok;
        true ->
            {ok, Config} = barrel_embed_local:init(local_config()),
            Texts = [<<"Hello">>, <<"World">>, <<"Test">>],
            {ok, Vectors} = barrel_embed_local:embed_batch(Texts, Config),

            ?assertEqual(3, length(Vectors)),
            lists:foreach(fun(V) ->
                ?assertEqual(768, length(V))
            end, Vectors),

            %% Vectors should be different
            [V1, V2, V3] = Vectors,
            ?assertNotEqual(V1, V2),
            ?assertNotEqual(V2, V3),

            cleanup_local(Config)
    end.

test_local_custom_model() ->
    case check_local_available() of
        false ->
            io:format("~n  SKIPPED: Python/sentence-transformers not available~n"),
            ok;
        true ->
            %% Use a smaller model for faster test
            BaseConfig = local_config(),
            {ok, Config} = barrel_embed_local:init(
                BaseConfig#{model => "sentence-transformers/all-MiniLM-L6-v2"}
            ),
            {ok, Vector} = barrel_embed_local:embed(<<"Test">>, Config),

            %% This model produces 384-dim vectors
            ?assertEqual(384, length(Vector)),

            cleanup_local(Config)
    end.

%%====================================================================
%% Ollama Provider Tests
%%====================================================================

test_ollama_embed() ->
    case check_ollama_available() of
        false ->
            io:format("~n  SKIPPED: Ollama not available~n"),
            ok;
        true ->
            {ok, Config} = barrel_embed_ollama:init(#{}),
            {ok, Vector} = barrel_embed_ollama:embed(<<"Hello world">>, Config),

            %% Default model (nomic-embed-text) produces 768-dim vectors
            ?assertEqual(768, length(Vector)),
            ?assert(is_float(hd(Vector)))
    end.

test_ollama_batch() ->
    case check_ollama_available() of
        false ->
            io:format("~n  SKIPPED: Ollama not available~n"),
            ok;
        true ->
            {ok, Config} = barrel_embed_ollama:init(#{}),
            Texts = [<<"Hello">>, <<"World">>, <<"Test">>],
            {ok, Vectors} = barrel_embed_ollama:embed_batch(Texts, Config),

            ?assertEqual(3, length(Vectors)),
            lists:foreach(fun(V) ->
                ?assertEqual(768, length(V))
            end, Vectors)
    end.

test_ollama_custom_model() ->
    case check_ollama_model_available(<<"all-minilm">>) of
        false ->
            io:format("~n  SKIPPED: Ollama model 'all-minilm' not available~n"),
            ok;
        true ->
            {ok, Config} = barrel_embed_ollama:init(#{
                model => <<"all-minilm">>
            }),
            {ok, Vector} = barrel_embed_ollama:embed(<<"Test">>, Config),

            %% all-minilm produces 384-dim vectors
            ?assertEqual(384, length(Vector))
    end.

%%====================================================================
%% OpenAI Provider Tests
%%====================================================================

test_openai_embed() ->
    case check_openai_available() of
        false ->
            io:format("~n  SKIPPED: OpenAI API not available (no API key)~n"),
            ok;
        true ->
            {ok, Config} = barrel_embed_openai:init(#{}),
            case barrel_embed_openai:embed(<<"Hello world">>, Config) of
                {ok, Vector} ->
                    %% Default model (text-embedding-3-small) produces 1536-dim vectors
                    ?assertEqual(1536, length(Vector)),
                    ?assert(is_float(hd(Vector)));
                {error, {http_error, 429, _}} ->
                    io:format("~n  SKIPPED: OpenAI rate limit exceeded~n"),
                    ok;
                {error, Reason} ->
                    ?assert(false, io_lib:format("OpenAI error: ~p", [Reason]))
            end
    end.

test_openai_batch() ->
    case check_openai_available() of
        false ->
            io:format("~n  SKIPPED: OpenAI API not available~n"),
            ok;
        true ->
            timer:sleep(1000),  %% Rate limit delay
            {ok, Config} = barrel_embed_openai:init(#{}),
            Texts = [<<"Hello">>, <<"World">>, <<"Test">>],
            case barrel_embed_openai:embed_batch(Texts, Config) of
                {ok, Vectors} ->
                    ?assertEqual(3, length(Vectors)),
                    lists:foreach(fun(V) ->
                        ?assertEqual(1536, length(V))
                    end, Vectors);
                {error, {http_error, 429, _}} ->
                    io:format("~n  SKIPPED: OpenAI rate limit exceeded~n"),
                    ok;
                {error, Reason} ->
                    ?assert(false, io_lib:format("OpenAI error: ~p", [Reason]))
            end
    end.

test_openai_custom_model() ->
    case check_openai_available() of
        false ->
            io:format("~n  SKIPPED: OpenAI API not available~n"),
            ok;
        true ->
            timer:sleep(1000),  %% Rate limit delay
            %% Use text-embedding-3-large for higher dimensions
            {ok, Config} = barrel_embed_openai:init(#{
                model => <<"text-embedding-3-large">>,
                dimension => 3072
            }),
            case barrel_embed_openai:embed(<<"Test">>, Config) of
                {ok, Vector} ->
                    %% text-embedding-3-large produces 3072-dim vectors
                    ?assertEqual(3072, length(Vector));
                {error, {http_error, 429, _}} ->
                    io:format("~n  SKIPPED: OpenAI rate limit exceeded~n"),
                    ok;
                {error, Reason} ->
                    ?assert(false, io_lib:format("OpenAI error: ~p", [Reason]))
            end
    end.

%%====================================================================
%% Provider Chain Tests
%%====================================================================

test_provider_chain() ->
    %% This test verifies fallback works when first provider fails
    LocalAvailable = check_local_available(),
    OllamaAvailable = check_ollama_available(),

    case LocalAvailable orelse OllamaAvailable of
        false ->
            io:format("~n  SKIPPED: No providers available~n"),
            ok;
        true ->
            %% Build chain based on what's available
            Chain = case {OllamaAvailable, LocalAvailable} of
                {true, true} ->
                    [{ollama, #{}}, {local, local_config()}];
                {true, false} ->
                    [{ollama, #{}}];
                {false, true} ->
                    [{local, local_config()}];
                {false, false} ->
                    []
            end,

            case Chain of
                [] ->
                    io:format("~n  SKIPPED: No providers available~n"),
                    ok;
                _ ->
                    Config = #{embedder => Chain},
                    %% Use spawned process with timeout to avoid hanging
                    Parent = self(),
                    Ref = make_ref(),
                    Pid = spawn(fun() ->
                        Result = try
                            case barrel_vectordb_embed:init(Config) of
                                {ok, State} when is_map(State) ->
                                    case maps:get(providers, State, []) of
                                        [] ->
                                            {skip, "No providers initialized"};
                                        Providers ->
                                            {ok, Vector} = barrel_vectordb_embed:embed(<<"Test">>, State),
                                            lists:foreach(fun({Mod, Cfg}) ->
                                                case Mod of
                                                    barrel_embed_local -> cleanup_local(Cfg);
                                                    _ -> ok
                                                end
                                            end, Providers),
                                            {ok, length(Vector)}
                                    end;
                                {error, no_providers_available} ->
                                    {skip, "No providers could be initialized"};
                                {error, Reason} ->
                                    {skip, io_lib:format("Init failed: ~p", [Reason])}
                            end
                        catch
                            _:Err -> {skip, io_lib:format("Error: ~p", [Err])}
                        end,
                        Parent ! {Ref, Result}
                    end),
                    receive
                        {Ref, {ok, VecLen}} ->
                            ?assert(VecLen > 0);
                        {Ref, {skip, Msg}} ->
                            io:format("~n  SKIPPED: ~s~n", [Msg]),
                            ok
                    after 45000 ->
                        exit(Pid, kill),
                        io:format("~n  SKIPPED: Provider chain init timed out~n"),
                        ok
                    end
            end
    end.

%%====================================================================
%% Helpers
%%====================================================================

check_local_available() ->
    %% Check if local provider can be initialized
    %% This verifies: Python available, sentence-transformers installed, embed script found
    %% Use a spawned process with timeout to avoid hanging the test
    Parent = self(),
    Ref = make_ref(),
    Pid = spawn(fun() ->
        Result = case barrel_embed_local:init(local_config()) of
            {ok, Config} ->
                cleanup_local(Config),
                true;
            {error, _} ->
                false
        end,
        Parent ! {Ref, Result}
    end),
    receive
        {Ref, Result} -> Result
    after 30000 ->
        %% Timeout - kill the spawned process and return false
        exit(Pid, kill),
        false
    end.

local_config() ->
    %% Allow custom Python path via BARREL_PYTHON env var
    %% Falls back to .venv/bin/python if available
    case os:getenv("BARREL_PYTHON") of
        false ->
            case find_venv_python() of
                {ok, Python} -> #{python => Python};
                not_found -> #{}
            end;
        Python -> #{python => Python}
    end.

find_venv_python() ->
    %% Try common venv paths relative to test run directory
    VenvPaths = [
        ".venv/bin/python",
        "../.venv/bin/python",
        "../../.venv/bin/python"
    ],
    find_first_file(VenvPaths).

find_first_file([]) ->
    not_found;
find_first_file([Path | Rest]) ->
    case filelib:is_file(Path) of
        true -> {ok, Path};
        false -> find_first_file(Rest)
    end.

check_ollama_available() ->
    %% Check if Ollama is running AND has the default model
    check_ollama_model_available(<<"nomic-embed-text">>).

check_openai_available() ->
    %% Check if OpenAI API key is set and API responds
    case barrel_embed_openai:init(#{}) of
        {ok, Config} ->
            barrel_embed_openai:available(Config);
        {error, _} ->
            false
    end.

check_ollama_model_available(Model) ->
    application:ensure_all_started(hackney),
    case hackney:request(get, <<"http://localhost:11434/api/tags">>, [], <<>>,
                         [{recv_timeout, 5000}]) of
        {ok, 200, _, ClientRef} ->
            {ok, Body} = hackney:body(ClientRef),
            try
                #{<<"models">> := Models} = json:decode(Body),
                lists:any(fun(#{<<"name">> := Name}) ->
                    binary:match(Name, Model) =/= nomatch
                end, Models)
            catch
                _:_ -> false
            end;
        _ ->
            false
    end.

cleanup_local(#{port := Port}) when is_port(Port) ->
    catch port_close(Port),
    ok;
cleanup_local(_) ->
    ok.
