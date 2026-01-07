%% @doc Ra server wrapper for barrel_vectordb cluster.
%%
%% Handles starting/joining/leaving the Raft cluster.
%%
%% @end
-module(barrel_vectordb_ra).

-export([start/0, start/1, join/1, leave/0]).
-export([server_id/0, server_id/1, cluster_name/0]).

-define(CLUSTER_NAME, barrel_vectordb).
-define(RA_SYSTEM, default).

%% @doc Start a new Ra cluster as the first node
start() ->
    start(#{}).

start(Config) ->
    ensure_ra_started(),
    DataDir = ra_data_dir(),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    ServerId = server_id(),
    MachineConfig = #{
        config => Config
    },
    Machine = {module, barrel_vectordb_ra_sm, MachineConfig},

    %% ra:start_cluster/4 signature: (System, ClusterName, Machine, ServerIds)
    case ra:start_cluster(?RA_SYSTEM, ?CLUSTER_NAME, Machine, [ServerId]) of
        {ok, Started, _Failed} when length(Started) > 0 ->
            {ok, ServerId};
        {ok, [], Failed} ->
            {error, {cluster_start_failed, Failed}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Join an existing cluster via a seed node
join(SeedNode) when is_atom(SeedNode) ->
    ensure_ra_started(),
    DataDir = ra_data_dir(),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    LocalServerId = server_id(),
    SeedServerId = server_id(SeedNode),

    Machine = {module, barrel_vectordb_ra_sm, #{}},

    %% First start local Ra server
    case ra:start_server(?RA_SYSTEM, ?CLUSTER_NAME, LocalServerId, Machine, [SeedServerId]) of
        ok ->
            %% Then add ourselves to the cluster via the seed node
            case ra:add_member(SeedServerId, LocalServerId) of
                {ok, _, _} ->
                    {ok, LocalServerId};
                {timeout, _} ->
                    %% Retry once
                    case ra:add_member(SeedServerId, LocalServerId) of
                        {ok, _, _} -> {ok, LocalServerId};
                        Error -> Error
                    end;
                Error ->
                    Error
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Leave the cluster gracefully
leave() ->
    ServerId = server_id(),
    case ra:leave_and_terminate(?RA_SYSTEM, ServerId, ServerId) of
        ok -> ok;
        {timeout, _} -> {error, timeout};
        {error, Reason} -> {error, Reason}
    end.

%% @doc Get the server ID for the local node
server_id() ->
    server_id(node()).

%% @doc Get the server ID for a given node
server_id(Node) ->
    {?CLUSTER_NAME, Node}.

%% @doc Get the cluster name
cluster_name() ->
    ?CLUSTER_NAME.

%% Internal functions

ensure_ra_started() ->
    case application:ensure_all_started(ra) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        Error -> Error
    end.

ra_data_dir() ->
    ClusterOpts = application:get_env(barrel_vectordb, cluster_options, #{}),
    BaseDir = maps:get(data_dir, ClusterOpts,
        application:get_env(barrel_vectordb, path, "/tmp/barrel_vectordb")),
    filename:join(BaseDir, "ra").
