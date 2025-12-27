%%%-------------------------------------------------------------------
%%% @doc OpenAI embedding provider
%%%
%%% Uses OpenAI's API for embedding generation.
%%% Default model: text-embedding-3-small (1536 dimensions)
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     api_key => <<"sk-...">>,
%%%     model => <<"text-embedding-3-small">>,
%%%     dimension => 1536
%%% }.
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_openai).
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

-define(DEFAULT_URL, <<"https://api.openai.com/v1">>).
-define(DEFAULT_MODEL, <<"text-embedding-3-small">>).
-define(DEFAULT_TIMEOUT, 30000).
-define(DEFAULT_DIMENSION, 1536).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> openai.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
%% Requires an `api_key' in the configuration.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    case maps:find(api_key, Config) of
        {ok, _ApiKey} ->
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
            end;
        error ->
            {error, missing_api_key}
    end.

%% @doc Check if OpenAI is available (requires API key).
-spec available(map()) -> boolean().
available(Config) ->
    case maps:find(api_key, Config) of
        {ok, ApiKey} when is_binary(ApiKey), byte_size(ApiKey) > 0 ->
            true;
        _ ->
            false
    end.

%% @doc Generate embedding for a single text.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    Url = maps:get(url, Config, ?DEFAULT_URL),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    ApiKey = maps:get(api_key, Config),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    ApiUrl = <<Url/binary, "/embeddings">>,
    Body = jsx:encode(#{
        <<"model">> => Model,
        <<"input">> => Text
    }),
    Headers = [
        {<<"Content-Type">>, <<"application/json">>},
        {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}
    ],

    case hackney:request(post, ApiUrl, Headers, Body, [{recv_timeout, Timeout}]) of
        {ok, 200, _RespHeaders, ClientRef} ->
            case hackney:body(ClientRef) of
                {ok, RespBody} ->
                    parse_single_response(RespBody);
                {error, Reason} ->
                    {error, {body_read_failed, Reason}}
            end;
        {ok, 429, RespHeaders, ClientRef} ->
            hackney:skip_body(ClientRef),
            RetryAfter = get_retry_after(RespHeaders),
            {error, {rate_limited, RetryAfter}};
        {ok, StatusCode, _RespHeaders, ClientRef} ->
            {ok, RespBody} = hackney:body(ClientRef),
            {error, {http_error, StatusCode, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

%% @doc Generate embeddings for multiple texts.
%% OpenAI supports native batch embedding.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    Url = maps:get(url, Config, ?DEFAULT_URL),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    ApiKey = maps:get(api_key, Config),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    ApiUrl = <<Url/binary, "/embeddings">>,
    Body = jsx:encode(#{
        <<"model">> => Model,
        <<"input">> => Texts
    }),
    Headers = [
        {<<"Content-Type">>, <<"application/json">>},
        {<<"Authorization">>, <<"Bearer ", ApiKey/binary>>}
    ],

    case hackney:request(post, ApiUrl, Headers, Body, [{recv_timeout, Timeout}]) of
        {ok, 200, _RespHeaders, ClientRef} ->
            case hackney:body(ClientRef) of
                {ok, RespBody} ->
                    parse_batch_response(RespBody);
                {error, Reason} ->
                    {error, {body_read_failed, Reason}}
            end;
        {ok, 429, RespHeaders, ClientRef} ->
            hackney:skip_body(ClientRef),
            RetryAfter = get_retry_after(RespHeaders),
            {error, {rate_limited, RetryAfter}};
        {ok, StatusCode, _RespHeaders, ClientRef} ->
            {ok, RespBody} = hackney:body(ClientRef),
            {error, {http_error, StatusCode, RespBody}};
        {error, Reason} ->
            {error, {request_failed, Reason}}
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
parse_single_response(Body) ->
    try
        Response = jsx:decode(Body, [return_maps]),
        case maps:find(<<"data">>, Response) of
            {ok, [#{<<"embedding">> := Embedding} | _]} ->
                {ok, Embedding};
            _ ->
                {error, {invalid_response, no_embedding_data}}
        end
    catch
        _:Reason ->
            {error, {json_decode_failed, Reason}}
    end.

%% @private
parse_batch_response(Body) ->
    try
        Response = jsx:decode(Body, [return_maps]),
        case maps:find(<<"data">>, Response) of
            {ok, DataList} when is_list(DataList) ->
                %% Sort by index to maintain order
                Sorted = lists:sort(
                    fun(#{<<"index">> := I1}, #{<<"index">> := I2}) -> I1 =< I2 end,
                    DataList
                ),
                Embeddings = [E || #{<<"embedding">> := E} <- Sorted],
                {ok, Embeddings};
            _ ->
                {error, {invalid_response, no_data_field}}
        end
    catch
        _:Reason ->
            {error, {json_decode_failed, Reason}}
    end.

%% @private
get_retry_after(Headers) ->
    case lists:keyfind(<<"retry-after">>, 1, Headers) of
        {_, Value} ->
            try binary_to_integer(Value)
            catch _:_ -> 60
            end;
        false ->
            60
    end.
