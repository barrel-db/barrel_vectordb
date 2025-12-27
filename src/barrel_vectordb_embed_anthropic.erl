%%%-------------------------------------------------------------------
%%% @doc Anthropic embedding provider (stub for future implementation)
%%%
%%% Placeholder for when Anthropic releases an embedding API.
%%% Currently not available - will return error.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_anthropic).
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

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> anthropic.

%% @doc Get dimension for this provider (placeholder).
-spec dimension(map()) -> pos_integer().
dimension(_Config) ->
    %% Placeholder dimension - will be updated when API is available
    768.

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    %% Just return config, actual initialization will happen when API is available
    {ok, Config}.

%% @doc Check if Anthropic embeddings are available.
%% Anthropic doesn't have an embedding API yet.
-spec available(map()) -> boolean().
available(_Config) ->
    false.

%% @doc Generate embedding for a single text.
%% Not yet implemented.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(_Text, _Config) ->
    {error, {not_implemented, "Anthropic embedding API not yet available"}}.

%% @doc Generate embeddings for multiple texts.
%% Not yet implemented.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(_Texts, _Config) ->
    {error, {not_implemented, "Anthropic embedding API not yet available"}}.
