%% @doc Cluster coordinator for barrel_vectordb.
%%
%% Provides the main API for cluster operations:
%% - start_cluster/1: Start a new cluster
%% - cluster_join/1: Join existing cluster
%% - cluster_leave/0: Leave cluster gracefully
%% - cluster_status/0: Get cluster status
%%
%% @end
-module(barrel_vectordb_mesh).

-compile({no_auto_import, [nodes/0]}).

-export([start_cluster/0, start_cluster/1]).
-export([cluster_join/1, cluster_leave/0]).
-export([is_clustered/0, is_leader/0, leader/0]).
-export([nodes/0, healthy_nodes/0, local_node/0, node_id/0]).
-export([cluster_status/0]).

%% @doc Start a new cluster (first node)
start_cluster() ->
    start_cluster(#{}).

start_cluster(Config) ->
    case is_clustered() of
        true ->
            {error, already_clustered};
        false ->
            %% Ensure cluster deps are started
            ok = ensure_cluster_deps(),
            %% Start the mesh supervisor
            case start_mesh_sup() of
                {ok, _} ->
                    %% Start Ra cluster
                    case barrel_vectordb_ra:start(Config) of
                        {ok, ServerId} ->
                            %% Register ourselves in the cluster
                            NodeId = node_id(),
                            NodeInfo = make_node_info(),
                            barrel_vectordb_cluster_client:join_node(NodeId, NodeInfo),
                            {ok, ServerId};
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end
    end.

%% @doc Join an existing cluster via seed node
cluster_join(SeedNode) when is_atom(SeedNode) ->
    case is_clustered() of
        true ->
            {error, already_clustered};
        false ->
            %% Ensure cluster deps are started
            ok = ensure_cluster_deps(),
            %% Start the mesh supervisor
            case start_mesh_sup() of
                {ok, _} ->
                    %% Connect to seed node
                    case connect_to_node(SeedNode) of
                        ok ->
                            %% Join Ra cluster
                            case barrel_vectordb_ra:join(SeedNode) of
                                {ok, _ServerId} ->
                                    %% Register ourselves
                                    NodeId = node_id(),
                                    NodeInfo = make_node_info(),
                                    barrel_vectordb_cluster_client:join_node(NodeId, NodeInfo),
                                    %% Register seed for health monitoring
                                    barrel_vectordb_health:register_node(SeedNode),
                                    ok;
                                Error ->
                                    Error
                            end;
                        Error ->
                            Error
                    end;
                Error ->
                    Error
            end
    end;

cluster_join(SeedNodes) when is_list(SeedNodes) ->
    %% Try each seed node until one works
    try_join_seeds(SeedNodes).

%% @doc Leave the cluster gracefully
cluster_leave() ->
    case is_clustered() of
        false ->
            {error, not_clustered};
        true ->
            NodeId = node_id(),
            %% Remove from cluster state
            barrel_vectordb_cluster_client:leave_node(NodeId),
            %% Leave Ra cluster
            Result = barrel_vectordb_ra:leave(),
            %% Stop mesh supervisor
            stop_mesh_sup(),
            Result
    end.

%% @doc Check if this node is part of a cluster
is_clustered() ->
    case whereis(barrel_vectordb_mesh_sup) of
        undefined ->
            false;
        _Pid ->
            %% Check if Ra server is running
            case catch ra:members(barrel_vectordb_ra:server_id()) of
                {ok, _, _} -> true;
                _ -> false
            end
    end.

%% @doc Check if this node is the Ra leader
is_leader() ->
    ServerId = barrel_vectordb_ra:server_id(),
    case catch ra:members(ServerId) of
        {ok, _, Leader} ->
            Leader =:= ServerId;
        _ ->
            false
    end.

%% @doc Get the current leader
leader() ->
    ServerId = barrel_vectordb_ra:server_id(),
    case catch ra:members(ServerId) of
        {ok, _, Leader} ->
            {ok, Leader};
        Error ->
            Error
    end.

%% @doc Get all cluster nodes from Ra state
nodes() ->
    barrel_vectordb_cluster_client:get_nodes().

%% @doc Get healthy nodes (via aten)
healthy_nodes() ->
    case is_clustered() of
        false ->
            [node()];
        true ->
            barrel_vectordb_health:healthy_nodes()
    end.

%% @doc Get local node info
local_node() ->
    make_node_info().

%% @doc Get local node ID (Ra server ID)
node_id() ->
    barrel_vectordb_ra:server_id().

%% @doc Get cluster status
cluster_status() ->
    case is_clustered() of
        false ->
            #{
                state => standalone,
                node => node(),
                nodes => [node()],
                leader => undefined,
                is_leader => false
            };
        true ->
            NodesResult = nodes(),
            LeaderResult = leader(),
            NodesList = case NodesResult of
                {ok, N} -> maps:keys(N);
                _ -> []
            end,
            LeaderNode = case LeaderResult of
                {ok, L} -> L;
                _ -> undefined
            end,
            #{
                state => member,
                node => node(),
                node_id => node_id(),
                nodes => NodesList,
                healthy_nodes => healthy_nodes(),
                leader => LeaderNode,
                is_leader => is_leader()
            }
    end.

%% Internal functions

ensure_cluster_deps() ->
    case application:ensure_all_started(ra) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        Error -> Error
    end.

start_mesh_sup() ->
    case whereis(barrel_vectordb_mesh_sup) of
        undefined ->
            supervisor:start_child(barrel_vectordb_sup, #{
                id => barrel_vectordb_mesh_sup,
                start => {barrel_vectordb_mesh_sup, start_link, []},
                restart => permanent,
                shutdown => infinity,
                type => supervisor,
                modules => [barrel_vectordb_mesh_sup]
            });
        Pid ->
            {ok, Pid}
    end.

stop_mesh_sup() ->
    case whereis(barrel_vectordb_mesh_sup) of
        undefined ->
            ok;
        _Pid ->
            supervisor:terminate_child(barrel_vectordb_sup, barrel_vectordb_mesh_sup),
            supervisor:delete_child(barrel_vectordb_sup, barrel_vectordb_mesh_sup)
    end.

make_node_info() ->
    ClusterOpts = application:get_env(barrel_vectordb, cluster_options, #{}),
    HttpConfig = maps:get(http, ClusterOpts, #{}),
    Port = maps:get(port, HttpConfig, 8080),
    {ok, Hostname} = inet:gethostname(),
    #{
        node => node(),
        address => iolist_to_binary([Hostname, ":", integer_to_list(Port)]),
        status => active,
        last_seen => erlang:system_time(second)
    }.

connect_to_node(Node) ->
    case net_kernel:connect_node(Node) of
        true -> ok;
        false -> {error, connection_failed};
        ignored -> {error, not_distributed}
    end.

try_join_seeds([]) ->
    {error, no_seeds_available};
try_join_seeds([Seed | Rest]) ->
    case cluster_join(Seed) of
        ok -> ok;
        {error, _} -> try_join_seeds(Rest)
    end.
