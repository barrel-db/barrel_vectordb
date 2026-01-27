%% @doc Manages local shard stores for clustered barrel_vectordb.
%%
%% Each shard is a local barrel_vectordb store with a role (leader/follower).
%% Leaders handle writes and replicate to followers.
%%
%% @end
-module(barrel_vectordb_shard_manager).
-behaviour(gen_server).

-export([start_link/0]).
-export([create_collection_shards/3, delete_collection_shards/1]).
-export([broadcast_create_shards/3, broadcast_delete_shards/1]).
-export([update_assignments/2, leader_changed/2]).
-export([get_local_store/1, get_local_stores/1]).
-export([is_local_shard/1, is_local_leader/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    %% Map of ShardId => #{store => StoreName, role => leader | follower}
    shards = #{} :: map(),
    data_dir :: string()
}).

%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Create local shards for a collection (called via Ra effect)
create_collection_shards(CollectionName, CollectionMeta, Placement) ->
    gen_server:call(?MODULE, {create_shards, CollectionName, CollectionMeta, Placement}, 60000).

%% @doc Delete all local shards for a collection (called via Ra effect)
delete_collection_shards(CollectionName) ->
    gen_server:call(?MODULE, {delete_shards, CollectionName}, 60000).

%% @doc Update shard assignments (called via Ra effect)
update_assignments(CollectionName, Assignments) ->
    gen_server:cast(?MODULE, {update_assignments, CollectionName, Assignments}).

%% @doc Handle leader change for a shard (called via Ra effect)
leader_changed(ShardId, NewLeader) ->
    gen_server:cast(?MODULE, {leader_changed, ShardId, NewLeader}).

%% @doc Get the local store for a shard
get_local_store(ShardId) ->
    gen_server:call(?MODULE, {get_store, ShardId}).

%% @doc Get all local stores for a collection
get_local_stores(CollectionName) ->
    gen_server:call(?MODULE, {get_stores, CollectionName}).

%% @doc Check if shard is local
is_local_shard(ShardId) ->
    gen_server:call(?MODULE, {is_local, ShardId}).

%% @doc Check if we're the leader for a shard
is_local_leader(ShardId) ->
    gen_server:call(?MODULE, {is_leader, ShardId}).

%% @doc Broadcast shard creation to all relevant nodes (called from Ra leader)
broadcast_create_shards(CollectionName, CollectionMeta, Placement) ->
    %% First create local shards
    create_collection_shards(CollectionName, CollectionMeta, Placement),
    %% Then RPC to all other nodes that should have shards
    LocalNode = node(),
    TargetNodes = lists:usort([
        Node || {_ShardIdx, {_, LeaderNode}, Replicas} <- Placement,
                Node <- [LeaderNode | [N || {_, N} <- Replicas]],
                Node =/= LocalNode
    ]),
    logger:info("Broadcasting shard creation for ~p to nodes: ~p", [CollectionName, TargetNodes]),
    lists:foreach(
        fun(Node) ->
            spawn(fun() ->
                case rpc:call(Node, ?MODULE, create_collection_shards,
                              [CollectionName, CollectionMeta, Placement], 30000) of
                    {badrpc, Reason} ->
                        logger:warning("Failed to create shards on ~p: ~p", [Node, Reason]);
                    ok ->
                        logger:info("Created shards on ~p", [Node])
                end
            end)
        end,
        TargetNodes),
    ok.

%% @doc Broadcast shard deletion to all nodes (called from Ra leader)
broadcast_delete_shards(CollectionName) ->
    %% First delete local shards
    delete_collection_shards(CollectionName),
    %% Then RPC to all other nodes
    LocalNode = node(),
    case barrel_vectordb_cluster_client:get_nodes() of
        {ok, NodesMap} ->
            TargetNodes = [N || {_, N} <- maps:keys(NodesMap), N =/= LocalNode],
            lists:foreach(
                fun(Node) ->
                    spawn(fun() ->
                        rpc:call(Node, ?MODULE, delete_collection_shards,
                                 [CollectionName], 30000)
                    end)
                end,
                TargetNodes);
        _ ->
            ok
    end,
    ok.

%% gen_server callbacks

init([]) ->
    ClusterOpts = application:get_env(barrel_vectordb, cluster_options, #{}),
    DataDir = maps:get(data_dir, ClusterOpts,
        application:get_env(barrel_vectordb, path, "/tmp/barrel_vectordb")),
    {ok, #state{data_dir = DataDir}}.

handle_call({create_shards, CollectionName, CollectionMeta, Placement}, _From, State) ->
    LocalNodeId = barrel_vectordb_mesh:node_id(),

    %% Find shards assigned to this node
    LocalShards = lists:filter(
        fun({_ShardIdx, Leader, Replicas}) ->
            Leader =:= LocalNodeId orelse lists:member(LocalNodeId, Replicas)
        end,
        Placement),

    %% Create stores for local shards
    NewShards = lists:foldl(
        fun({ShardIdx, Leader, _Replicas}, Acc) ->
            ShardId = {CollectionName, ShardIdx},
            Role = case Leader of
                LocalNodeId -> leader;
                _ -> follower
            end,
            case create_shard_store(State#state.data_dir, CollectionName, ShardIdx, CollectionMeta) of
                {ok, StoreName} ->
                    maps:put(ShardId, #{store => StoreName, role => Role}, Acc);
                {error, _Reason} ->
                    Acc
            end
        end,
        State#state.shards,
        LocalShards),

    {reply, ok, State#state{shards = NewShards}};

handle_call({delete_shards, CollectionName}, _From, State) ->
    %% Find and stop all shards for this collection
    {ToDelete, ToKeep} = maps:fold(
        fun({ColName, _ShardIdx} = ShardId, Info, {Del, Keep}) ->
            case ColName of
                CollectionName -> {[{ShardId, Info} | Del], Keep};
                _ -> {Del, maps:put(ShardId, Info, Keep)}
            end
        end,
        {[], #{}},
        State#state.shards),

    %% Stop the stores
    lists:foreach(
        fun({_ShardId, #{store := StoreName}}) ->
            catch barrel_vectordb:stop(StoreName)
        end,
        ToDelete),

    {reply, ok, State#state{shards = ToKeep}};

handle_call({get_store, ShardId}, _From, State) ->
    case maps:get(ShardId, State#state.shards, undefined) of
        undefined ->
            {reply, {error, not_found}, State};
        #{store := StoreName} ->
            {reply, {ok, StoreName}, State}
    end;

handle_call({get_stores, CollectionName}, _From, State) ->
    Stores = maps:fold(
        fun({ColName, _ShardIdx}, #{store := StoreName}, Acc) ->
            case ColName of
                CollectionName -> [StoreName | Acc];
                _ -> Acc
            end
        end,
        [],
        State#state.shards),
    {reply, {ok, Stores}, State};

handle_call({is_local, ShardId}, _From, State) ->
    {reply, maps:is_key(ShardId, State#state.shards), State};

handle_call({is_leader, ShardId}, _From, State) ->
    case maps:get(ShardId, State#state.shards, undefined) of
        undefined ->
            {reply, false, State};
        #{role := leader} ->
            {reply, true, State};
        _ ->
            {reply, false, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({update_assignments, CollectionName, Assignments}, State) ->
    LocalNodeId = barrel_vectordb_mesh:node_id(),
    CollectionMeta = get_collection_meta(CollectionName),

    NewShards = lists:foldl(
        fun({ShardIdx, Leader, Replicas}, Acc) ->
            ShardId = {CollectionName, ShardIdx},
            IsLocal = Leader =:= LocalNodeId orelse lists:member(LocalNodeId, Replicas),
            Role = case Leader of
                LocalNodeId -> leader;
                _ -> follower
            end,

            case {IsLocal, maps:get(ShardId, Acc, undefined)} of
                {true, undefined} ->
                    %% New local shard, create it
                    case create_shard_store(State#state.data_dir, CollectionName, ShardIdx, CollectionMeta) of
                        {ok, StoreName} ->
                            maps:put(ShardId, #{store => StoreName, role => Role}, Acc);
                        _ ->
                            Acc
                    end;
                {true, #{store := StoreName}} ->
                    %% Existing shard, maybe role changed
                    maps:put(ShardId, #{store => StoreName, role => Role}, Acc);
                {false, undefined} ->
                    %% Not local, don't have it
                    Acc;
                {false, #{store := StoreName}} ->
                    %% Was local, no longer assigned to us - stop it
                    catch barrel_vectordb:stop(StoreName),
                    maps:remove(ShardId, Acc)
            end
        end,
        State#state.shards,
        Assignments),

    {noreply, State#state{shards = NewShards}};

handle_cast({leader_changed, ShardId, NewLeader}, State) ->
    LocalNodeId = barrel_vectordb_mesh:node_id(),
    case maps:get(ShardId, State#state.shards, undefined) of
        undefined ->
            {noreply, State};
        ShardInfo ->
            NewRole = case NewLeader of
                LocalNodeId -> leader;
                _ -> follower
            end,
            NewShards = maps:put(ShardId, ShardInfo#{role => NewRole}, State#state.shards),
            {noreply, State#state{shards = NewShards}}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Stop all local stores
    maps:foreach(
        fun(_ShardId, #{store := StoreName}) ->
            catch barrel_vectordb:stop(StoreName)
        end,
        State#state.shards),
    ok.

%% Internal functions

create_shard_store(DataDir, CollectionName, ShardIdx, CollectionMeta) ->
    %% Extract fields from collection_meta record
    %% Record format: {collection_meta, Name, Dimension, NumShards, RF, CreatedAt, Status, Backend, BackendConfig}
    Dimension = element(3, CollectionMeta),
    Backend = element(8, CollectionMeta),
    BackendConfig = element(9, CollectionMeta),

    ShardName = iolist_to_binary([CollectionName, <<"_shard_">>, integer_to_binary(ShardIdx)]),
    StoreName = binary_to_atom(<<"barrel_vectordb_store_", ShardName/binary>>, utf8),
    StorePath = filename:join([DataDir, binary_to_list(CollectionName), "shard_" ++ integer_to_list(ShardIdx)]),

    StoreConfig = #{
        name => StoreName,
        path => StorePath,
        dimensions => Dimension,
        backend => Backend
    },

    %% Add backend-specific config
    StoreConfig1 = case Backend of
        hnsw -> StoreConfig#{hnsw => BackendConfig};
        faiss -> StoreConfig#{faiss => BackendConfig};
        diskann ->
            DC = case maps:is_key(base_path, BackendConfig) of
                true -> BackendConfig;
                false -> BackendConfig#{base_path => filename:join(StorePath, "diskann")}
            end,
            StoreConfig#{diskann => DC}
    end,

    case barrel_vectordb:start_link(StoreConfig1) of
        {ok, _Pid} ->
            {ok, StoreName};
        {error, {already_started, _}} ->
            {ok, StoreName};
        {error, Reason} ->
            {error, Reason}
    end.

%% @private Get full collection metadata for a collection
get_collection_meta(CollectionName) ->
    case barrel_vectordb_cluster_client:get_collections() of
        {ok, Collections} ->
            case maps:get(CollectionName, Collections, undefined) of
                undefined ->
                    %% Return default meta structure
                    %% {collection_meta, Name, Dimension, NumShards, RF, CreatedAt, Status, Backend, BackendConfig}
                    {collection_meta, CollectionName, 768, 1, 1, 0, active, hnsw, #{}};
                Meta ->
                    Meta
            end;
        _ ->
            {collection_meta, CollectionName, 768, 1, 1, 0, active, hnsw, #{}}
    end.
