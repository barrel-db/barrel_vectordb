%% @doc Supervisor for barrel_vectordb cluster components.
%%
%% Manages the cluster infrastructure:
%% - Health monitoring (aten-based)
%% - Shard management
%% - Shard replication
%% - Shard coordination
%% - Discovery
%%
%% @end
-module(barrel_vectordb_mesh_sup).
-behaviour(supervisor).

-export([start_link/0, start_link/1]).
-export([init/1]).

start_link() ->
    start_link(#{}).

start_link(Config) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Config).

init(_Config) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },

    Children = [
        %% Health monitoring (aten-based failure detection)
        #{
            id => barrel_vectordb_health,
            start => {barrel_vectordb_health, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [barrel_vectordb_health]
        },
        %% Shard manager (local shard stores)
        #{
            id => barrel_vectordb_shard_manager,
            start => {barrel_vectordb_shard_manager, start_link, []},
            restart => permanent,
            shutdown => 10000,
            type => worker,
            modules => [barrel_vectordb_shard_manager]
        },
        %% Shard replicator (async write replication)
        #{
            id => barrel_vectordb_shard_replicator,
            start => {barrel_vectordb_shard_replicator, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [barrel_vectordb_shard_replicator]
        },
        %% Shard coordinator (rebalancing on failure)
        #{
            id => barrel_vectordb_shard_coordinator,
            start => {barrel_vectordb_shard_coordinator, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [barrel_vectordb_shard_coordinator]
        },
        %% Discovery (seed/dns/manual)
        #{
            id => barrel_vectordb_discovery,
            start => {barrel_vectordb_discovery, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [barrel_vectordb_discovery]
        }
    ],

    {ok, {SupFlags, Children}}.
