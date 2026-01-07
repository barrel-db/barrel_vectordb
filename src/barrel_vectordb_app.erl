%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb application module
%%%
%%% This module implements the OTP application behaviour for barrel_vectordb.
%%% It starts the top-level supervisor which manages the vector database
%%% components.
%%%
%%% Clustering can be enabled via:
%%% - Config: {enable_cluster, true} + {cluster_options, #{...}}
%%% - API: barrel_vectordb:start_cluster(Options)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_app).
-behaviour(application).

-export([start/2, stop/1]).
-export([cluster_enabled/0, cluster_options/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the barrel_vectordb application.
%% @private
start(_StartType, _StartArgs) ->
    %% Initialize Python execution queue (ETS-based rate limiter)
    ok = barrel_vectordb_python_queue:init(),
    %% Start cluster dependencies if enabled
    ok = maybe_start_cluster_deps(),
    barrel_vectordb_sup:start_link().

%% @doc Stop the barrel_vectordb application.
%% @private
stop(_State) ->
    ok.

%% @doc Check if clustering is enabled via config.
-spec cluster_enabled() -> boolean().
cluster_enabled() ->
    application:get_env(barrel_vectordb, enable_cluster, false).

%% @doc Get cluster options from config.
-spec cluster_options() -> map().
cluster_options() ->
    application:get_env(barrel_vectordb, cluster_options, #{}).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Start Ra (and optionally Cowboy) if cluster is enabled.
maybe_start_cluster_deps() ->
    case cluster_enabled() of
        true ->
            %% Start Ra for Raft consensus (includes aten for failure detection)
            ok = start_app(ra),
            %% Start Cowboy if HTTP is configured
            Opts = cluster_options(),
            case maps:is_key(http, Opts) orelse maps:is_key(https, Opts) of
                true -> ok = start_app(cowboy);
                false -> ok
            end;
        false ->
            ok
    end.

%% @private Start an application, handling already_started.
start_app(App) ->
    case application:ensure_all_started(App) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, Reason} -> {error, Reason}
    end.
