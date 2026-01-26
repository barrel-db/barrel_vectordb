%%%-------------------------------------------------------------------
%%% @doc Embeddings coordinator - backward compatibility wrapper
%%%
%%% Delegates all embedding operations to barrel_embed.
%%% This module exists for backward compatibility - apps using
%%% barrel_vectordb continue to work without any changes.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed).

%% API - delegates to barrel_embed
-export([init/1, embed/2, embed_batch/2, embed_batch/3, dimension/1, info/1]).

%% Re-export types for backward compatibility
-export_type([provider/0, provider_chain/0, embed_state/0]).

-type provider() :: barrel_embed:provider().
-type provider_chain() :: barrel_embed:provider_chain().
-type embed_state() :: barrel_embed:embed_state().

%% @doc Initialize embedding state from configuration.
-spec init(map()) -> {ok, embed_state() | undefined} | {error, term()}.
init(Config) -> barrel_embed:init(Config).

%% @doc Generate embedding for a single text.
-spec embed(binary(), embed_state() | undefined) -> {ok, [float()]} | {error, term()}.
embed(Text, State) -> barrel_embed:embed(Text, State).

%% @doc Generate embeddings for multiple texts.
-spec embed_batch([binary()], embed_state() | undefined) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, State) -> barrel_embed:embed_batch(Texts, State).

%% @doc Generate embeddings for multiple texts with options.
-spec embed_batch([binary()], map(), embed_state() | undefined) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Opts, State) -> barrel_embed:embed_batch(Texts, Opts, State).

%% @doc Get the dimension of embeddings.
-spec dimension(embed_state() | undefined) -> pos_integer() | undefined.
dimension(State) -> barrel_embed:dimension(State).

%% @doc Get information about the current embedding configuration.
-spec info(embed_state() | undefined) -> map().
info(State) -> barrel_embed:info(State).
