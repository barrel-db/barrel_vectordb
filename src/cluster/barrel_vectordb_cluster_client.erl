%% @doc Client for Ra cluster commands and queries.
%%
%% Provides a high-level API for interacting with the cluster state machine.
%%
%% @end
-module(barrel_vectordb_cluster_client).

-export([command/1, command/2]).
-export([query/1, leader_query/1]).
-export([create_collection/2, delete_collection/1]).
-export([join_node/2, leave_node/1]).
-export([get_nodes/0, get_collections/0, get_shards/0, get_shards/1]).

-define(DEFAULT_TIMEOUT, 5000).

%% @doc Execute a command through Ra consensus
command(Command) ->
    command(Command, ?DEFAULT_TIMEOUT).

command(Command, Timeout) ->
    ServerId = barrel_vectordb_ra:server_id(),
    case ra:process_command(ServerId, Command, Timeout) of
        {ok, Result, _Leader} ->
            Result;
        {timeout, _} ->
            {error, timeout};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Query local state (may be stale)
query(QueryFun) ->
    ServerId = barrel_vectordb_ra:server_id(),
    case ra:local_query(ServerId, QueryFun) of
        {ok, {_, Result}, _} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason};
        {timeout, _} ->
            {error, timeout}
    end.

%% @doc Query leader state (consistent read)
leader_query(QueryFun) ->
    ServerId = barrel_vectordb_ra:server_id(),
    case ra:leader_query(ServerId, QueryFun) of
        {ok, {_, Result}, _} ->
            {ok, Result};
        {error, Reason} ->
            {error, Reason};
        {timeout, _} ->
            {error, timeout}
    end.

%% High-level API

%% @doc Create a collection with shard placement
create_collection(Name, Config) ->
    %% Calculate placement based on available nodes and replication factor
    NumShards = maps:get(shards, Config, 1),
    RF = maps:get(replication_factor, Config, 1),
    case get_active_nodes() of
        {ok, Nodes} when length(Nodes) >= RF ->
            Placement = calculate_placement(NumShards, RF, Nodes),
            command({create_collection, Name, Config, Placement});
        {ok, Nodes} ->
            {error, {insufficient_nodes, length(Nodes), RF}};
        Error ->
            Error
    end.

%% @doc Delete a collection
delete_collection(Name) ->
    command({delete_collection, Name}).

%% @doc Add a node to the cluster
join_node(NodeId, NodeInfo) ->
    command({join_cluster, NodeId, NodeInfo}).

%% @doc Remove a node from the cluster
leave_node(NodeId) ->
    command({leave_cluster, NodeId}).

%% @doc Get all nodes in the cluster
get_nodes() ->
    query(fun(State) -> get_nodes_from_state(State) end).

%% @doc Get all collections
get_collections() ->
    query(fun(State) -> get_collections_from_state(State) end).

%% @doc Get all shard assignments
get_shards() ->
    query(fun(State) -> get_shards_from_state(State) end).

%% @doc Get shard assignments for a specific collection
get_shards(CollectionName) ->
    query(fun(State) -> get_collection_shards_from_state(State, CollectionName) end).

%% Internal functions

get_active_nodes() ->
    case get_nodes() of
        {ok, Nodes} ->
            Active = maps:filter(
                fun(_, Info) ->
                    element(4, Info) =:= active  %% status field
                end,
                Nodes),
            {ok, maps:keys(Active)};
        Error ->
            Error
    end.

calculate_placement(NumShards, RF, Nodes) ->
    NodeList = lists:sort(Nodes),
    NumNodes = length(NodeList),
    lists:map(
        fun(ShardIdx) ->
            %% Simple round-robin with RF replicas
            StartIdx = ShardIdx rem NumNodes,
            ReplicaNodes = take_nodes(NodeList, StartIdx, RF),
            [Leader | _Followers] = ReplicaNodes,
            {ShardIdx, Leader, ReplicaNodes}
        end,
        lists:seq(0, NumShards - 1)).

take_nodes(Nodes, StartIdx, Count) ->
    NumNodes = length(Nodes),
    Indices = [(StartIdx + I) rem NumNodes || I <- lists:seq(0, Count - 1)],
    [lists:nth(I + 1, Nodes) || I <- Indices].

%% State accessors (work with record structure)
get_nodes_from_state(State) ->
    element(2, State).  %% nodes field

get_collections_from_state(State) ->
    element(3, State).  %% collections field

get_shards_from_state(State) ->
    element(4, State).  %% shards field

get_collection_shards_from_state(State, CollectionName) ->
    AllShards = element(4, State),
    maps:filter(
        fun({ColName, _ShardIdx}, _) -> ColName =:= CollectionName end,
        AllShards).
