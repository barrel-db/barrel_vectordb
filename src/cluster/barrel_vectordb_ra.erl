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
    _ = ensure_ra_started(),
    DataDir = ra_data_dir(),
    logger:info("Ra data dir: ~p", [DataDir]),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    ServerId = server_id(),
    logger:info("Server ID: ~p", [ServerId]),
    MachineConfig = #{
        config => Config
    },
    Machine = {module, barrel_vectordb_ra_sm, MachineConfig},

    %% ra:start_cluster/4 signature: (System, ClusterName, Machine, ServerIds)
    logger:info("Calling ra:start_cluster(~p, ~p, Machine, ~p)", [?RA_SYSTEM, ?CLUSTER_NAME, [ServerId]]),
    Result = ra:start_cluster(?RA_SYSTEM, ?CLUSTER_NAME, Machine, [ServerId]),
    logger:info("ra:start_cluster returned: ~p", [Result]),
    case Result of
        {ok, Started, _Failed} when length(Started) > 0 ->
            {ok, ServerId};
        {ok, [], Failed} ->
            {error, {cluster_start_failed, Failed}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Join an existing cluster via a seed node
join(SeedNode) when is_atom(SeedNode) ->
    logger:info("join/1 called with SeedNode: ~p", [SeedNode]),
    _ = ensure_ra_started(),
    DataDir = ra_data_dir(),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),

    LocalServerId = server_id(),
    SeedServerId = server_id(SeedNode),
    logger:info("LocalServerId: ~p, SeedServerId: ~p", [LocalServerId, SeedServerId]),

    Machine = {module, barrel_vectordb_ra_sm, #{}},

    %% First start local Ra server (or check if already running)
    logger:info("Starting local Ra server with initial peer: ~p", [SeedServerId]),
    case start_local_server(LocalServerId, Machine, SeedServerId) of
        ok ->
            %% Add ourselves to the cluster via the seed node with retries
            add_to_cluster_with_retry(SeedServerId, LocalServerId, 5);
        {error, Reason} ->
            logger:error("Failed to start local Ra server: ~p", [Reason]),
            {error, Reason}
    end.

%% @private Start local Ra server, handling already_started
start_local_server(LocalServerId, Machine, SeedServerId) ->
    case ra:start_server(?RA_SYSTEM, ?CLUSTER_NAME, LocalServerId, Machine, [SeedServerId]) of
        ok ->
            logger:info("Local Ra server started"),
            ok;
        {error, {already_started, _}} ->
            logger:info("Local Ra server already running"),
            ok;
        {error, {shutdown, {failed_to_start_child, _, {already_started, _}}}} ->
            logger:info("Local Ra server already running (supervisor)"),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Add to cluster with exponential backoff retry for concurrent joins
add_to_cluster_with_retry(_SeedServerId, _LocalServerId, 0) ->
    logger:error("Exhausted retries adding to cluster"),
    {error, max_retries_exceeded};
add_to_cluster_with_retry(SeedServerId, LocalServerId, Retries) ->
    logger:info("Adding to cluster via ~p (retries left: ~p)", [SeedServerId, Retries]),
    case ra:add_member(SeedServerId, LocalServerId) of
        {ok, _, _} ->
            logger:info("Successfully added to cluster"),
            {ok, LocalServerId};
        {error, cluster_change_not_permitted} ->
            %% Another node is joining, wait and retry with backoff
            Delay = (6 - Retries) * 1000 + rand:uniform(1000),
            logger:info("Cluster change in progress, retrying in ~pms", [Delay]),
            timer:sleep(Delay),
            add_to_cluster_with_retry(SeedServerId, LocalServerId, Retries - 1);
        {timeout, _} ->
            logger:warning("Timeout adding to cluster, retrying"),
            timer:sleep(500),
            add_to_cluster_with_retry(SeedServerId, LocalServerId, Retries - 1);
        {error, already_member} ->
            %% We're already in the cluster (maybe from a previous partial join)
            logger:info("Already a member of the cluster"),
            {ok, LocalServerId};
        Error ->
            logger:error("Failed to add to cluster: ~p", [Error]),
            Error
    end.

%% @doc Leave the cluster gracefully
leave() ->
    ServerId = server_id(),
    case ra:leave_and_terminate(?RA_SYSTEM, ServerId, ServerId) of
        ok -> ok;
        timeout -> {error, timeout};
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
        {ok, _} ->
            %% Start the default Ra system with our data directory
            start_ra_system();
        {error, {already_started, _}} ->
            %% Ra app already started, ensure system is started
            start_ra_system();
        Error ->
            Error
    end.

start_ra_system() ->
    DataDir = ra_data_dir(),
    ok = filelib:ensure_dir(filename:join(DataDir, "dummy")),
    %% Start the default Ra system
    Config = #{
        name => ?RA_SYSTEM,
        data_dir => DataDir,
        wal_data_dir => DataDir,
        names => ra_system:derive_names(?RA_SYSTEM)
    },
    case ra_system:start(Config) of
        {ok, _} ->
            logger:info("Started Ra system '~p' with data_dir: ~p", [?RA_SYSTEM, DataDir]),
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            logger:error("Failed to start Ra system: ~p", [Reason]),
            {error, Reason}
    end.

ra_data_dir() ->
    ClusterOpts = application:get_env(barrel_vectordb, cluster_options, #{}),
    BaseDir = maps:get(data_dir, ClusterOpts,
        application:get_env(barrel_vectordb, path, "/tmp/barrel_vectordb")),
    filename:join(BaseDir, "ra").
