%%%-------------------------------------------------------------------
%%% @doc Embeddings coordinator
%%%
%%% Manages a chain of embedding providers with automatic fallback.
%%% Tries each provider in order until one succeeds.
%%%
%%% == Configuration ==
%%% ```
%%% %% Single provider
%%% Embedder = {local, #{}}.
%%%
%%% %% Provider chain with fallback
%%% Embedder = [
%%%     {ollama, #{url => <<"http://localhost:11434">>, model => <<"nomic-embed-text">>}},
%%%     {local, #{}}  %% fallback
%%% ].
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed).

-define(DEFAULT_DIMENSION, 768).
-define(DEFAULT_BATCH_SIZE, 32).

%% API
-export([
    init/1,
    embed/2,
    embed_batch/2,
    embed_batch/3,
    dimension/1,
    info/1
]).

%% Types
-type provider() :: {atom(), map()}.
-type provider_chain() :: [provider()] | provider().
-type embed_state() :: #{
    providers := [provider()],
    dimension := pos_integer(),
    batch_size := pos_integer()
}.

-export_type([provider/0, provider_chain/0, embed_state/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Initialize embedding state from configuration.
%% @param Config Map with embedder configuration
%% @returns Initialized embedding state
-spec init(map()) -> {ok, embed_state()} | {error, term()}.
init(Config) ->
    EmbedderConfig = maps:get(embedder, Config, default_embedder()),
    Dimension = maps:get(dimensions, Config, ?DEFAULT_DIMENSION),
    BatchSize = maps:get(batch_size, Config, ?DEFAULT_BATCH_SIZE),

    %% Normalize to provider chain
    Providers = normalize_providers(EmbedderConfig),

    %% Initialize each provider
    InitializedProviders = lists:filtermap(
        fun({ProviderName, ProviderConfig}) ->
            Module = provider_module(ProviderName),
            case init_provider(Module, ProviderConfig) of
                {ok, NewConfig} ->
                    {true, {Module, NewConfig}};
                {error, Reason} ->
                    error_logger:warning_msg(
                        "Failed to initialize provider ~p: ~p~n",
                        [ProviderName, Reason]
                    ),
                    false
            end
        end,
        Providers
    ),

    case InitializedProviders of
        [] ->
            {error, no_providers_available};
        _ ->
            {ok, #{
                providers => InitializedProviders,
                dimension => Dimension,
                batch_size => BatchSize
            }}
    end.

%% @doc Generate embedding for a single text.
-spec embed(binary(), embed_state()) -> {ok, [float()]} | {error, term()}.
embed(Text, #{providers := Providers}) when is_binary(Text) ->
    try_providers_embed(Providers, Text);
embed(Text, State) when is_list(Text) ->
    embed(list_to_binary(Text), State).

%% @doc Generate embeddings for multiple texts.
-spec embed_batch([binary()], embed_state()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, State) ->
    embed_batch(Texts, #{}, State).

%% @doc Generate embeddings for multiple texts with options.
-spec embed_batch([binary()], map(), embed_state()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Options, #{providers := Providers, batch_size := DefaultBatchSize}) ->
    BatchSize = maps:get(batch_size, Options, DefaultBatchSize),
    try_providers_embed_batch(Providers, Texts, BatchSize).

%% @doc Get the dimension of embeddings.
-spec dimension(embed_state()) -> pos_integer().
dimension(#{dimension := Dimension}) ->
    Dimension.

%% @doc Get information about the current embedding configuration.
-spec info(embed_state()) -> map().
info(#{providers := Providers, dimension := Dimension}) ->
    ProviderInfo = [
        #{module => Module, name => Module:name()}
        || {Module, _Config} <- Providers
    ],
    #{
        providers => ProviderInfo,
        dimension => Dimension
    }.

%%====================================================================
%% Internal Functions
%%====================================================================

%% Default embedder configuration - local CPU
default_embedder() ->
    {local, #{}}.

%% Normalize embedder config to provider chain
normalize_providers({Name, Config}) when is_atom(Name) ->
    [{Name, Config}];
normalize_providers(Providers) when is_list(Providers) ->
    Providers.

%% Map provider name to module
provider_module(local) -> barrel_vectordb_embed_local;
provider_module(ollama) -> barrel_vectordb_embed_ollama;
provider_module(openai) -> barrel_vectordb_embed_openai;
provider_module(anthropic) -> barrel_vectordb_embed_anthropic;
provider_module(Module) when is_atom(Module) -> Module.

%% Initialize a provider
init_provider(Module, Config) ->
    case erlang:function_exported(Module, init, 1) of
        true ->
            try
                Module:init(Config)
            catch
                _:Reason -> {error, Reason}
            end;
        false ->
            {ok, Config}
    end.

%% Try providers in order for single embed
try_providers_embed([], _Text) ->
    {error, all_providers_failed};
try_providers_embed([{Module, Config} | Rest], Text) ->
    case barrel_vectordb_embed_provider:check_available(Module, Config) of
        true ->
            case barrel_vectordb_embed_provider:call_embed(Module, Text, Config) of
                {ok, Vector} ->
                    {ok, Vector};
                {error, Reason} ->
                    error_logger:info_msg(
                        "Provider ~p failed: ~p, trying next~n",
                        [Module, Reason]
                    ),
                    try_providers_embed(Rest, Text)
            end;
        false ->
            error_logger:info_msg("Provider ~p not available, skipping~n", [Module]),
            try_providers_embed(Rest, Text)
    end.

%% Try providers in order for batch embed
try_providers_embed_batch([], _Texts, _BatchSize) ->
    {error, all_providers_failed};
try_providers_embed_batch([{Module, Config} | Rest], Texts, BatchSize) ->
    case barrel_vectordb_embed_provider:check_available(Module, Config) of
        true ->
            case do_batch_embed(Module, Config, Texts, BatchSize) of
                {ok, Vectors} ->
                    {ok, Vectors};
                {error, Reason} ->
                    error_logger:info_msg(
                        "Provider ~p batch failed: ~p, trying next~n",
                        [Module, Reason]
                    ),
                    try_providers_embed_batch(Rest, Texts, BatchSize)
            end;
        false ->
            try_providers_embed_batch(Rest, Texts, BatchSize)
    end.

%% Execute batch embedding with chunking
do_batch_embed(Module, Config, Texts, BatchSize) ->
    Batches = chunk_list(Texts, BatchSize),
    do_batch_embed_loop(Module, Config, Batches, []).

do_batch_embed_loop(_Module, _Config, [], Acc) ->
    {ok, lists:flatten(lists:reverse(Acc))};
do_batch_embed_loop(Module, Config, [Batch | Rest], Acc) ->
    case barrel_vectordb_embed_provider:call_embed_batch(Module, Batch, Config) of
        {ok, Vectors} ->
            do_batch_embed_loop(Module, Config, Rest, [Vectors | Acc]);
        {error, _} = Error ->
            Error
    end.

%% Split list into chunks
chunk_list(List, Size) ->
    chunk_list(List, Size, []).

chunk_list([], _Size, Acc) ->
    lists:reverse(Acc);
chunk_list(List, Size, Acc) ->
    {Chunk, Rest} = safe_split(Size, List),
    chunk_list(Rest, Size, [Chunk | Acc]).

safe_split(N, List) when length(List) =< N ->
    {List, []};
safe_split(N, List) ->
    lists:split(N, List).
