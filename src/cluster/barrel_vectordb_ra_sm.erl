%% @doc Ra state machine for barrel_vectordb cluster.
%%
%% Manages cluster membership, collection metadata, and shard assignments
%% through Raft consensus.
%%
%% @end
-module(barrel_vectordb_ra_sm).
-behaviour(ra_machine).

-export([init/1, apply/3, state_enter/2, tick/2]).

%% Types
-type node_id() :: {atom(), node()}.
-type shard_id() :: {CollectionName :: binary(), ShardIdx :: non_neg_integer()}.

-record(node_info, {
    node :: node(),
    address :: binary(),
    status :: joining | active | leaving | down,
    last_seen :: integer()
}).

-record(collection_meta, {
    name :: binary(),
    dimension :: pos_integer(),
    num_shards :: pos_integer(),
    replication_factor :: pos_integer(),
    created_at :: integer(),
    status :: creating | active | deleting
}).

-record(shard_assignment, {
    shard_id :: shard_id(),
    leader :: node_id(),
    replicas :: [node_id()],
    version :: pos_integer()
}).

-record(cluster_state, {
    nodes = #{} :: #{node_id() => #node_info{}},
    collections = #{} :: #{binary() => #collection_meta{}},
    shards = #{} :: #{shard_id() => #shard_assignment{}},
    config = #{} :: map()
}).

-export_type([node_id/0, shard_id/0]).

%% Ra machine callbacks

init(Config) ->
    #cluster_state{config = Config}.

%% Cluster membership commands

apply(_Meta, {join_cluster, NodeId, NodeInfo}, State) ->
    case maps:is_key(NodeId, State#cluster_state.nodes) of
        true ->
            {State, {error, already_member}, []};
        false ->
            Nodes = maps:put(NodeId, NodeInfo#node_info{status = active}, State#cluster_state.nodes),
            NewState = State#cluster_state{nodes = Nodes},
            Effects = [{mod_call, barrel_vectordb_cluster_events, node_joined, [NodeId, NodeInfo]}],
            {NewState, ok, Effects}
    end;

apply(_Meta, {leave_cluster, NodeId}, State) ->
    case maps:take(NodeId, State#cluster_state.nodes) of
        {_NodeInfo, Nodes} ->
            NewState = State#cluster_state{nodes = Nodes},
            %% Trigger shard reassignment for shards owned by this node
            Effects = [{mod_call, barrel_vectordb_shard_coordinator, handle_node_leave, [NodeId]}],
            {NewState, ok, Effects};
        error ->
            {State, {error, not_member}, []}
    end;

apply(_Meta, {update_node_status, NodeId, Status}, State) ->
    case maps:get(NodeId, State#cluster_state.nodes, undefined) of
        undefined ->
            {State, {error, not_found}, []};
        NodeInfo ->
            Updated = NodeInfo#node_info{status = Status},
            Nodes = maps:put(NodeId, Updated, State#cluster_state.nodes),
            {State#cluster_state{nodes = Nodes}, ok, []}
    end;

apply(_Meta, {heartbeat, NodeId, Timestamp}, State) ->
    case maps:get(NodeId, State#cluster_state.nodes, undefined) of
        undefined ->
            {State, {error, not_found}, []};
        NodeInfo ->
            Updated = NodeInfo#node_info{last_seen = Timestamp},
            Nodes = maps:put(NodeId, Updated, State#cluster_state.nodes),
            {State#cluster_state{nodes = Nodes}, ok, []}
    end;

%% Collection commands

apply(_Meta, {create_collection, Name, Config, Placement}, State) ->
    case maps:is_key(Name, State#cluster_state.collections) of
        true ->
            {State, {error, already_exists}, []};
        false ->
            Meta = #collection_meta{
                name = Name,
                dimension = maps:get(dimension, Config, 768),
                num_shards = maps:get(shards, Config, 1),
                replication_factor = maps:get(replication_factor, Config, 1),
                created_at = erlang:system_time(second),
                status = creating
            },
            Collections = maps:put(Name, Meta, State#cluster_state.collections),
            %% Create shard assignments from placement
            Shards = lists:foldl(
                fun({ShardIdx, Leader, Replicas}, Acc) ->
                    ShardId = {Name, ShardIdx},
                    Assignment = #shard_assignment{
                        shard_id = ShardId,
                        leader = Leader,
                        replicas = Replicas,
                        version = 1
                    },
                    maps:put(ShardId, Assignment, Acc)
                end,
                State#cluster_state.shards,
                Placement),
            NewState = State#cluster_state{collections = Collections, shards = Shards},
            %% Effect to create local shards on each node
            Effects = [{mod_call, barrel_vectordb_shard_manager, create_collection_shards, [Name, Meta, Placement]}],
            {NewState, {ok, Meta}, Effects}
    end;

apply(_Meta, {update_collection_status, Name, Status}, State) ->
    case maps:get(Name, State#cluster_state.collections, undefined) of
        undefined ->
            {State, {error, not_found}, []};
        Meta ->
            Updated = Meta#collection_meta{status = Status},
            Collections = maps:put(Name, Updated, State#cluster_state.collections),
            {State#cluster_state{collections = Collections}, ok, []}
    end;

apply(_Meta, {delete_collection, Name}, State) ->
    case maps:take(Name, State#cluster_state.collections) of
        {_ColMeta, Collections} ->
            %% Remove all shard assignments for this collection
            Shards = maps:filter(
                fun({ColName, _ShardIdx}, _) -> ColName =/= Name end,
                State#cluster_state.shards),
            NewState = State#cluster_state{collections = Collections, shards = Shards},
            Effects = [{mod_call, barrel_vectordb_shard_manager, delete_collection_shards, [Name]}],
            {NewState, ok, Effects};
        error ->
            {State, {error, not_found}, []}
    end;

%% Shard assignment commands

apply(_Meta, {assign_shards, CollectionName, Assignments}, State) ->
    Shards = lists:foldl(
        fun({ShardIdx, Leader, Replicas}, Acc) ->
            ShardId = {CollectionName, ShardIdx},
            OldVersion = case maps:get(ShardId, Acc, undefined) of
                undefined -> 0;
                #shard_assignment{version = V} -> V
            end,
            Assignment = #shard_assignment{
                shard_id = ShardId,
                leader = Leader,
                replicas = Replicas,
                version = OldVersion + 1
            },
            maps:put(ShardId, Assignment, Acc)
        end,
        State#cluster_state.shards,
        Assignments),
    NewState = State#cluster_state{shards = Shards},
    Effects = [{mod_call, barrel_vectordb_shard_manager, update_assignments, [CollectionName, Assignments]}],
    {NewState, ok, Effects};

apply(_Meta, {promote_replica, ShardId, NewLeader}, State) ->
    case maps:get(ShardId, State#cluster_state.shards, undefined) of
        undefined ->
            {State, {error, shard_not_found}, []};
        #shard_assignment{replicas = Replicas, version = V} = Assignment ->
            %% Move new leader to front of replicas list
            NewReplicas = [NewLeader | lists:delete(NewLeader, Replicas)],
            Updated = Assignment#shard_assignment{
                leader = NewLeader,
                replicas = NewReplicas,
                version = V + 1
            },
            Shards = maps:put(ShardId, Updated, State#cluster_state.shards),
            NewState = State#cluster_state{shards = Shards},
            Effects = [{mod_call, barrel_vectordb_shard_manager, leader_changed, [ShardId, NewLeader]}],
            {NewState, ok, Effects}
    end;

%% Catch-all
apply(_Meta, _Command, State) ->
    {State, {error, unknown_command}, []}.

%% State enter callback - runs when node becomes leader/follower
state_enter(leader, State) ->
    %% Start shard coordinator when we become leader
    [{mod_call, barrel_vectordb_shard_coordinator, activate, [State]}];
state_enter(follower, _State) ->
    %% Deactivate coordinator when we become follower
    [{mod_call, barrel_vectordb_shard_coordinator, deactivate, []}];
state_enter(_, _State) ->
    [].

%% Tick callback for periodic operations
tick(_TimeMs, _State) ->
    [].
