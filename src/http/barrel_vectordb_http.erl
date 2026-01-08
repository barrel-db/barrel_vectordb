%% @doc Standalone HTTP server for barrel_vectordb.
%%
%% Only started when `http` key is present in cluster_options.
%% When barrel_vectordb is embedded in barrel_memory, this is not used -
%% barrel_memory embeds the routes directly.
%%
%% @end
-module(barrel_vectordb_http).

-export([start_link/0, start_link/1]).
-export([stop/0]).

%% @doc Start HTTP server with config from application env
start_link() ->
    ClusterOpts = application:get_env(barrel_vectordb, cluster_options, #{}),
    HttpConfig = maps:get(http, ClusterOpts, undefined),
    case HttpConfig of
        undefined ->
            ignore;
        Config when is_map(Config) ->
            start_link(Config)
    end.

%% @doc Start HTTP server with explicit config
start_link(Config) ->
    IP = maps:get(ip, Config, {0, 0, 0, 0}),
    Port = maps:get(port, Config, 8080),
    NumAcceptors = maps:get(num_acceptors, Config, 100),

    Routes = barrel_vectordb_http_routes:routes(),
    Dispatch = cowboy_router:compile([{'_', Routes}]),

    TransportOpts = #{
        socket_opts => [{ip, IP}, {port, Port}],
        num_acceptors => NumAcceptors
    },
    ProtocolOpts = #{env => #{dispatch => Dispatch}},

    cowboy:start_clear(barrel_vectordb_http_listener, TransportOpts, ProtocolOpts).

%% @doc Stop HTTP server
stop() ->
    cowboy:stop_listener(barrel_vectordb_http_listener).
