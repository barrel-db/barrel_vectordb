%%%-------------------------------------------------------------------
%%% @doc Ollama embedding provider
%%%
%%% Uses Ollama's local API for embedding generation.
%%% Default model: nomic-embed-text (768 dimensions)
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     url => <<"http://localhost:11434">>,
%%%     model => <<"nomic-embed-text">>,
%%%     timeout => 30000
%%% }.
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_ollama).
-behaviour(barrel_vectordb_embed_provider).

%% Behaviour callbacks
-export([
    embed/2,
    embed_batch/2,
    dimension/1,
    name/0,
    init/1,
    available/1
]).

-define(DEFAULT_URL, <<"http://localhost:11434">>).
-define(DEFAULT_MODEL, <<"nomic-embed-text">>).
-define(DEFAULT_TIMEOUT, 30000).
-define(DEFAULT_DIMENSION, 768).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> ollama.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    case application:ensure_all_started(hackney) of
        {ok, _} ->
            NewConfig = maps:merge(#{
                url => ?DEFAULT_URL,
                model => ?DEFAULT_MODEL,
                timeout => ?DEFAULT_TIMEOUT,
                dimension => ?DEFAULT_DIMENSION
            }, Config),
            {ok, NewConfig};
        {error, Reason} ->
            {error, {hackney_start_failed, Reason}}
    end.

%% @doc Check if Ollama is available.
-spec available(map()) -> boolean().
available(Config) ->
    Url = maps:get(url, Config, ?DEFAULT_URL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),
    ApiUrl = <<Url/binary, "/api/tags">>,

    case hackney:request(get, ApiUrl, [], <<>>, [{recv_timeout, Timeout}]) of
        {ok, 200, _, ClientRef} ->
            hackney:skip_body(ClientRef),
            true;
        _ ->
            false
    end.

%% @doc Generate embedding for a single text.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    Url = maps:get(url, Config, ?DEFAULT_URL),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    ApiUrl = <<Url/binary, "/api/embeddings">>,
    Body = jsx:encode(#{
        <<"model">> => Model,
        <<"prompt">> => Text
    }),
    Headers = [{<<"Content-Type">>, <<"application/json">>}],

    case hackney:request(post, ApiUrl, Headers, Body, [{recv_timeout, Timeout}]) of
        {ok, 200, _RespHeaders, ClientRef} ->
            case hackney:body(ClientRef) of
                {ok, RespBody} ->
                    parse_embedding_response(RespBody);
                {error, Reason} ->
                    {error, {body_read_failed, Reason}}
            end;
        {ok, StatusCode, _RespHeaders, ClientRef} ->
            {ok, RespBody} = hackney:body(ClientRef),
            {error, {http_error, StatusCode, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

%% @doc Generate embeddings for multiple texts.
%% Ollama doesn't have native batch support, so we do sequential calls.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    Results = lists:map(
        fun(Text) ->
            embed(Text, Config)
        end,
        Texts
    ),

    case lists:partition(fun({ok, _}) -> true; (_) -> false end, Results) of
        {Successes, []} ->
            Vectors = [V || {ok, V} <- Successes],
            {ok, Vectors};
        {_, [FirstError | _]} ->
            FirstError
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
parse_embedding_response(Body) ->
    try
        Response = jsx:decode(Body, [return_maps]),
        case maps:find(<<"embedding">>, Response) of
            {ok, Embedding} when is_list(Embedding) ->
                {ok, Embedding};
            _ ->
                {error, {invalid_response, no_embedding_field}}
        end
    catch
        _:Reason ->
            {error, {json_decode_failed, Reason}}
    end.
