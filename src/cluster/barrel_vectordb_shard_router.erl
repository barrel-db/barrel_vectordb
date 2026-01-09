%% @doc Routes operations to appropriate shard(s).
%%
%% Single-document operations (add, get, delete) are routed to the
%% shard determined by consistent hash on doc_id.
%%
%% Search operations use scatter-gather across all shards.
%%
%% @end
-module(barrel_vectordb_shard_router).

-export([route_add/5, route_add_vector/6]).
-export([route_get/3, route_delete/3]).
-export([route_search/4, route_search_vector/4]).
-export([get_shard_info/2]).

%% @doc Route add operation to appropriate shard
route_add(Collection, Id, Text, Metadata, EmbedderInfo) ->
    case get_shard_info(Collection, Id) of
        {ok, {local, StoreName}} ->
            barrel_vectordb:add(StoreName, Id, Text, Metadata);
        {ok, {remote, Node, _ShardIdx}} ->
            rpc_call(Node, route_add, [Collection, Id, Text, Metadata, EmbedderInfo]);
        {error, _} = Error ->
            Error
    end.

%% @doc Route add_vector operation to appropriate shard
route_add_vector(Collection, Id, Text, Metadata, Vector, _EmbedderInfo) ->
    case get_shard_info(Collection, Id) of
        {ok, {local, StoreName}} ->
            barrel_vectordb:add_vector(StoreName, Id, Text, Metadata, Vector);
        {ok, {remote, Node, ShardIdx}} ->
            rpc_call(Node, route_add_vector, [Collection, Id, Text, Metadata, Vector, ShardIdx]);
        {error, _} = Error ->
            Error
    end.

%% @doc Route get operation to appropriate shard
route_get(Collection, Id, _Opts) ->
    case get_shard_info(Collection, Id) of
        {ok, {local, StoreName}} ->
            barrel_vectordb:get(StoreName, Id);
        {ok, {remote, Node, ShardIdx}} ->
            rpc_call(Node, route_get, [Collection, Id, ShardIdx]);
        {error, _} = Error ->
            Error
    end.

%% @doc Route delete operation to appropriate shard
route_delete(Collection, Id, _Opts) ->
    case get_shard_info(Collection, Id) of
        {ok, {local, StoreName}} ->
            barrel_vectordb:delete(StoreName, Id);
        {ok, {remote, Node, ShardIdx}} ->
            rpc_call(Node, route_delete, [Collection, Id, ShardIdx]);
        {error, _} = Error ->
            Error
    end.

%% @doc Route search to all shards (scatter-gather)
route_search(Collection, Query, Opts, EmbedderInfo) ->
    barrel_vectordb_scatter:search(Collection, Query, Opts, EmbedderInfo).

%% @doc Route vector search to all shards (scatter-gather)
route_search_vector(Collection, Vector, Opts, _EmbedderInfo) ->
    barrel_vectordb_scatter:search_vector(Collection, Vector, Opts).

%% Internal

%% @doc Get shard info for a document
-spec get_shard_info(binary(), binary()) ->
    {ok, {local, atom()} | {remote, node(), non_neg_integer()}} |
    {error, term()}.
get_shard_info(Collection, DocId) ->
    %% Get collection metadata
    case barrel_vectordb_cluster_client:get_collections() of
        {ok, Collections} ->
            case maps:get(Collection, Collections, undefined) of
                undefined ->
                    {error, collection_not_found};
                CollectionMeta ->
                    %% collection_meta record: {collection_meta, name, dimension, num_shards, ...}
                    NumShards = element(4, CollectionMeta),  %% num_shards is element 4
                    ShardIdx = barrel_vectordb_shard_locator:shard_for_key(DocId, NumShards),
                    ShardId = {Collection, ShardIdx},

                    %% Check if local
                    case barrel_vectordb_shard_manager:is_local_leader(ShardId) of
                        true ->
                            case barrel_vectordb_shard_manager:get_local_store(ShardId) of
                                {ok, StoreName} ->
                                    {ok, {local, StoreName}};
                                {error, _} ->
                                    %% Local leader but no store? Try to find remote
                                    find_remote_leader(Collection, ShardIdx)
                            end;
                        false ->
                            %% Find the leader node
                            find_remote_leader(Collection, ShardIdx)
                    end
            end;
        {error, _} = Error ->
            Error
    end.

find_remote_leader(Collection, ShardIdx) ->
    case barrel_vectordb_cluster_client:get_shard_placement(Collection) of
        {ok, Placements} ->
            case lists:keyfind(ShardIdx, 1, Placements) of
                {ShardIdx, Leader, _Replicas} ->
                    %% Leader is {Name, Node} tuple
                    {_, Node} = Leader,
                    {ok, {remote, Node, ShardIdx}};
                false ->
                    {error, shard_not_found}
            end;
        {error, _} = Error ->
            Error
    end.

rpc_call(Node, Fun, Args) ->
    case rpc:call(Node, ?MODULE, Fun, Args, 30000) of
        {badrpc, Reason} ->
            {error, {rpc_failed, Reason}};
        Result ->
            Result
    end.
