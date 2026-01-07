%% @doc Coordinates shard rebalancing on node failures.
%%
%% Only active on the Ra leader node. Handles:
%% - Promoting new leaders when a leader fails
%% - Removing failed replicas
%% - Adding replacement replicas to maintain replication factor
%%
%% @end
-module(barrel_vectordb_shard_coordinator).
-behaviour(gen_server).

-export([start_link/0]).
-export([activate/1, deactivate/0]).
-export([handle_node_leave/1]).
-export([calculate_placement/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    active = false :: boolean(),
    cluster_state :: term()
}).

%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Activate coordinator (called when node becomes Ra leader)
activate(ClusterState) ->
    gen_server:cast(?MODULE, {activate, ClusterState}).

%% @doc Deactivate coordinator (called when node becomes Ra follower)
deactivate() ->
    gen_server:cast(?MODULE, deactivate).

%% @doc Handle a node leaving - reassign its shards
handle_node_leave(NodeId) ->
    gen_server:cast(?MODULE, {node_leave, NodeId}).

%% @doc Calculate shard placement for a new collection
calculate_placement(NumShards, ReplicationFactor, Nodes) ->
    NodeList = lists:sort(Nodes),
    NumNodes = length(NodeList),
    if
        NumNodes < ReplicationFactor ->
            {error, {insufficient_nodes, NumNodes, ReplicationFactor}};
        true ->
            Placement = lists:map(
                fun(ShardIdx) ->
                    ReplicaNodes = select_replicas(NodeList, ShardIdx, ReplicationFactor),
                    [Leader | _] = ReplicaNodes,
                    {ShardIdx, Leader, ReplicaNodes}
                end,
                lists:seq(0, NumShards - 1)),
            {ok, Placement}
    end.

%% gen_server callbacks

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({activate, ClusterState}, State) ->
    %% We became the leader, activate coordination
    {noreply, State#state{active = true, cluster_state = ClusterState}};

handle_cast(deactivate, State) ->
    %% We're no longer leader
    {noreply, State#state{active = false, cluster_state = undefined}};

handle_cast({node_leave, NodeId}, #state{active = true} = State) ->
    %% Only process if we're the active coordinator (leader)
    handle_node_failure(NodeId),
    {noreply, State};

handle_cast({node_leave, _NodeId}, State) ->
    %% Not active, ignore
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% Internal functions

select_replicas(NodeList, ShardIdx, ReplicationFactor) ->
    NumNodes = length(NodeList),
    %% Use consistent selection: start at ShardIdx position, take RF nodes
    StartIdx = ShardIdx rem NumNodes,
    Indices = [(StartIdx + I) rem NumNodes || I <- lists:seq(0, ReplicationFactor - 1)],
    [lists:nth(I + 1, NodeList) || I <- Indices].

handle_node_failure(FailedNodeId) ->
    %% Get all shards where failed node is leader
    case barrel_vectordb_cluster_client:get_shards() of
        {ok, Shards} ->
            maps:foreach(
                fun(ShardId, Assignment) ->
                    Leader = element(3, Assignment),  %% leader field
                    Replicas = element(4, Assignment), %% replicas field
                    case Leader of
                        FailedNodeId ->
                            %% This shard needs a new leader
                            promote_new_leader(ShardId, FailedNodeId, Replicas);
                        _ ->
                            %% Check if failed node is a replica
                            case lists:member(FailedNodeId, Replicas) of
                                true ->
                                    %% Remove from replicas, maybe add replacement
                                    remove_failed_replica(ShardId, FailedNodeId, Replicas);
                                false ->
                                    ok
                            end
                    end
                end,
                Shards);
        _ ->
            ok
    end.

promote_new_leader(ShardId, FailedNodeId, Replicas) ->
    %% Find next available replica
    AvailableReplicas = lists:delete(FailedNodeId, Replicas),
    case AvailableReplicas of
        [NewLeader | _] ->
            barrel_vectordb_cluster_client:command({promote_replica, ShardId, NewLeader});
        [] ->
            %% No replicas available - shard is unavailable
            ok
    end.

remove_failed_replica(ShardId, FailedNodeId, Replicas) ->
    %% Get the collection to check if we need to maintain replication factor
    {CollectionName, _ShardIdx} = ShardId,
    case barrel_vectordb_cluster_client:get_collections() of
        {ok, Collections} ->
            case maps:get(CollectionName, Collections, undefined) of
                undefined ->
                    ok;
                CollMeta ->
                    RF = element(5, CollMeta),  %% replication_factor field
                    NewReplicas = lists:delete(FailedNodeId, Replicas),
                    case length(NewReplicas) < RF of
                        true ->
                            %% Need to add a new replica
                            maybe_add_replacement_replica(ShardId, NewReplicas, RF);
                        false ->
                            ok
                    end
            end;
        _ ->
            ok
    end.

maybe_add_replacement_replica(ShardId, CurrentReplicas, RF) ->
    %% Get all active nodes
    case barrel_vectordb_cluster_client:get_nodes() of
        {ok, Nodes} ->
            ActiveNodes = [NodeId || {NodeId, Info} <- maps:to_list(Nodes),
                                     element(4, Info) =:= active],
            %% Find nodes not already replicas
            Available = ActiveNodes -- CurrentReplicas,
            case Available of
                [NewReplica | _] when length(CurrentReplicas) < RF ->
                    %% Add new replica
                    NewReplicas = CurrentReplicas ++ [NewReplica],
                    [Leader | _] = NewReplicas,
                    {CollectionName, ShardIdx} = ShardId,
                    barrel_vectordb_cluster_client:command(
                        {assign_shards, CollectionName, [{ShardIdx, Leader, NewReplicas}]});
                _ ->
                    %% No available nodes
                    ok
            end;
        _ ->
            ok
    end.
