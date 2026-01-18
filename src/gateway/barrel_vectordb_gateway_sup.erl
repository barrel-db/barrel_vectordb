%%%-------------------------------------------------------------------
%%% @doc Gateway supervisor
%%%
%%% Supervises all gateway components:
%%% - System RocksDB manager
%%% - Rate limiter (ETS-based)
%%% - Cowboy HTTP listener
%%%
%%% Started conditionally when gateway is enabled in configuration.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_gateway_sup).
-behaviour(supervisor).

%% API
-export([start_link/0, start_link/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the gateway supervisor with default configuration.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(get_gateway_config()).

%% @doc Start the gateway supervisor with explicit configuration.
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, Config).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init(Config) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },

    Children = [
        system_db_child(Config),
        rate_limiter_child(),
        stores_supervisor_child()
        | cowboy_children(Config)
    ],

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Get gateway configuration from application environment.
get_gateway_config() ->
    case application:get_env(barrel_vectordb, gateway) of
        {ok, Config} when is_map(Config) -> Config;
        _ -> #{}
    end.

%% @private System RocksDB manager child spec.
system_db_child(Config) ->
    SystemDbPath = maps:get(system_db_path, Config, undefined),
    Opts = case SystemDbPath of
        undefined -> #{};
        Path -> #{path => Path}
    end,
    #{
        id => barrel_vectordb_system_db,
        start => {barrel_vectordb_system_db, start_link, [Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_vectordb_system_db]
    }.

%% @private Rate limiter child spec.
rate_limiter_child() ->
    #{
        id => barrel_vectordb_gateway_rate,
        start => {barrel_vectordb_gateway_rate, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [barrel_vectordb_gateway_rate]
    }.

%% @private Store supervisor child spec (for standalone mode).
stores_supervisor_child() ->
    #{
        id => barrel_vectordb_gateway_stores,
        start => {barrel_vectordb_gateway_stores, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [barrel_vectordb_gateway_stores]
    }.

%% @private Cowboy HTTP listener children (empty list if not configured).
%% We use a wrapper gen_server that starts cowboy in its init/1.
cowboy_children(Config) ->
    case maps:get(port, Config, undefined) of
        undefined ->
            [];
        _Port ->
            [#{
                id => barrel_vectordb_gateway_listener,
                start => {barrel_vectordb_gateway_listener, start_link, [Config]},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [barrel_vectordb_gateway_listener]
            }]
    end.
