%% @doc Cluster event pub/sub for barrel_vectordb.
%%
%% Uses pg (process groups) for local event distribution.
%%
%% @end
-module(barrel_vectordb_cluster_events).

-export([node_joined/2, node_left/1]).
-export([shard_leader_changed/2, collection_created/1, collection_deleted/1]).
-export([subscribe/0, subscribe/1, unsubscribe/1]).

-define(PG_SCOPE, barrel_vectordb_events).

%% @doc Called when a node joins the cluster
node_joined(NodeId, NodeInfo) ->
    notify_subscribers({node_joined, NodeId, NodeInfo}).

%% @doc Called when a node leaves the cluster
node_left(NodeId) ->
    notify_subscribers({node_left, NodeId}).

%% @doc Called when shard leadership changes
shard_leader_changed(ShardId, NewLeader) ->
    notify_subscribers({shard_leader_changed, ShardId, NewLeader}).

%% @doc Called when a collection is created
collection_created(CollectionMeta) ->
    notify_subscribers({collection_created, CollectionMeta}).

%% @doc Called when a collection is deleted
collection_deleted(CollectionName) ->
    notify_subscribers({collection_deleted, CollectionName}).

%% @doc Subscribe calling process to cluster events
subscribe() ->
    subscribe(self()).

%% @doc Subscribe a process to cluster events
subscribe(Pid) ->
    ensure_pg_scope(),
    pg:join(?PG_SCOPE, cluster_events, Pid).

%% @doc Unsubscribe from cluster events
unsubscribe(Pid) ->
    pg:leave(?PG_SCOPE, cluster_events, Pid).

%% Internal

ensure_pg_scope() ->
    case pg:start(?PG_SCOPE) of
        {ok, _Pid} -> ok;
        {error, {already_started, _Pid}} -> ok
    end.

notify_subscribers(Event) ->
    case catch pg:get_members(?PG_SCOPE, cluster_events) of
        {'EXIT', _} -> ok;
        [] -> ok;
        Members ->
            [Pid ! {cluster_event, Event} || Pid <- Members],
            ok
    end.
