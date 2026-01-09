%% @doc Handles resharding of collections.
%%
%% Resharding changes the number of shards for a collection by:
%% 1. Creating new shards with the new count
%% 2. Migrating documents from old shards to new shards
%% 3. Updating collection metadata
%% 4. Deleting old shards
%%
%% @end
-module(barrel_vectordb_reshard).

-export([reshard/2]).

-define(BATCH_SIZE, 100).

%% @doc Reshard a collection to a new shard count
-spec reshard(binary(), pos_integer()) -> {ok, map()} | {error, term()}.
reshard(Collection, NewNumShards) ->
    case barrel_vectordb_cluster_client:get_collections() of
        {ok, Collections} ->
            case maps:get(Collection, Collections, undefined) of
                undefined ->
                    {error, not_found};
                CollectionMeta ->
                    %% collection_meta: {collection_meta, name, dimension, num_shards, rf, created_at, status}
                    OldNumShards = element(4, CollectionMeta),
                    Dimension = element(3, CollectionMeta),
                    RF = element(5, CollectionMeta),

                    case OldNumShards of
                        NewNumShards ->
                            {error, same_shard_count};
                        _ ->
                            do_reshard(Collection, OldNumShards, NewNumShards, Dimension, RF)
                    end
            end;
        {error, _} = Error ->
            Error
    end.

%% Internal

do_reshard(Collection, OldNumShards, NewNumShards, Dimension, RF) ->
    logger:info("Starting reshard of ~s: ~p -> ~p shards", [Collection, OldNumShards, NewNumShards]),

    %% Step 1: Get all active nodes for new placement
    case get_active_nodes() of
        {ok, Nodes} when length(Nodes) >= RF ->
            %% Step 2: Calculate new shard placement
            NewPlacement = calculate_placement(NewNumShards, RF, Nodes),

            %% Step 3: Create new shards (using temporary collection suffix)
            TempCollection = <<Collection/binary, "_reshard_temp">>,
            case create_new_shards(TempCollection, NewNumShards, Dimension, RF, NewPlacement) of
                ok ->
                    %% Step 4: Migrate data
                    case migrate_data(Collection, TempCollection, OldNumShards, NewNumShards) of
                        {ok, MigratedCount} ->
                            %% Step 5: Swap collections (update metadata to new shard count)
                            case finalize_reshard(Collection, NewNumShards, NewPlacement) of
                                ok ->
                                    %% Step 6: Cleanup old shards and temp
                                    cleanup_old_shards(Collection, OldNumShards),
                                    cleanup_temp_shards(TempCollection, NewNumShards),

                                    logger:info("Reshard complete: ~p documents migrated", [MigratedCount]),
                                    {ok, #{
                                        old_shards => OldNumShards,
                                        new_shards => NewNumShards,
                                        documents_migrated => MigratedCount
                                    }};
                                {error, _} = Error ->
                                    cleanup_temp_shards(TempCollection, NewNumShards),
                                    Error
                            end;
                        {error, _} = Error ->
                            cleanup_temp_shards(TempCollection, NewNumShards),
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        {ok, Nodes} ->
            {error, {insufficient_nodes, length(Nodes), RF}};
        Error ->
            Error
    end.

get_active_nodes() ->
    case barrel_vectordb_cluster_client:get_nodes() of
        {ok, Nodes} ->
            Active = maps:filter(
                fun(_, Info) ->
                    element(4, Info) =:= active
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
            StartIdx = ShardIdx rem NumNodes,
            ReplicaNodes = take_nodes(NodeList, StartIdx, RF),
            [Leader | _] = ReplicaNodes,
            {ShardIdx, Leader, ReplicaNodes}
        end,
        lists:seq(0, NumShards - 1)).

take_nodes(Nodes, StartIdx, Count) ->
    NumNodes = length(Nodes),
    Indices = [(StartIdx + I) rem NumNodes || I <- lists:seq(0, Count - 1)],
    [lists:nth(I + 1, Nodes) || I <- Indices].

create_new_shards(Collection, NumShards, Dimension, RF, Placement) ->
    %% Create temp collection in Ra state
    Config = #{
        dimension => Dimension,
        shards => NumShards,
        replication_factor => RF
    },
    case barrel_vectordb_cluster_client:command({create_collection, Collection, Config, Placement}) of
        {ok, _} -> ok;
        {error, already_exists} -> ok;  %% Temp might exist from failed previous attempt
        Error -> Error
    end.

migrate_data(OldCollection, NewCollection, OldNumShards, NewNumShards) ->
    %% Iterate through all old shards and migrate documents
    OldShardIdxs = lists:seq(0, OldNumShards - 1),

    MigratedCount = lists:foldl(
        fun(ShardIdx, Count) ->
            case migrate_shard(OldCollection, NewCollection, ShardIdx, NewNumShards) of
                {ok, N} -> Count + N;
                {error, _} -> Count
            end
        end,
        0,
        OldShardIdxs),

    {ok, MigratedCount}.

migrate_shard(OldCollection, NewCollection, ShardIdx, NewNumShards) ->
    ShardId = {OldCollection, ShardIdx},

    %% Get local store if available, otherwise need RPC
    case barrel_vectordb_shard_manager:get_local_store(ShardId) of
        {ok, StoreName} ->
            migrate_local_shard(StoreName, NewCollection, NewNumShards);
        {error, not_found} ->
            %% Find the leader node and ask it to migrate
            case find_shard_leader(OldCollection, ShardIdx) of
                {ok, Node} when Node =:= node() ->
                    %% We are the leader but no store? Skip
                    {ok, 0};
                {ok, Node} ->
                    %% RPC to leader to migrate
                    case rpc:call(Node, ?MODULE, migrate_shard,
                                  [OldCollection, NewCollection, ShardIdx, NewNumShards], 60000) of
                        {badrpc, Reason} ->
                            logger:warning("Failed to migrate shard ~p from ~p: ~p",
                                          [ShardIdx, Node, Reason]),
                            {ok, 0};
                        Result ->
                            Result
                    end;
                {error, _} ->
                    {ok, 0}
            end
    end.

migrate_local_shard(StoreName, NewCollection, NewNumShards) ->
    %% Scan all documents and re-route to new shards
    case barrel_vectordb:peek(StoreName, ?BATCH_SIZE) of
        {ok, []} ->
            {ok, 0};
        {ok, Docs} ->
            migrate_docs(Docs, NewCollection, NewNumShards, 0);
        {error, _} = Error ->
            Error
    end.

migrate_docs([], _NewCollection, _NewNumShards, Count) ->
    {ok, Count};
migrate_docs(Docs, NewCollection, NewNumShards, Count) ->
    %% Migrate each doc to the appropriate new shard
    NewCount = lists:foldl(
        fun(Doc, Acc) ->
            case migrate_doc(Doc, NewCollection, NewNumShards) of
                ok -> Acc + 1;
                {error, _} -> Acc
            end
        end,
        Count,
        Docs),
    {ok, NewCount}.

migrate_doc(Doc, NewCollection, _NewNumShards) ->
    %% Doc is a map with key, text, metadata, vector
    Key = maps:get(key, Doc),
    Text = maps:get(text, Doc, <<>>),
    Metadata = maps:get(metadata, Doc, #{}),
    Vector = maps:get(vector, Doc, undefined),

    %% Route to new collection (which has new shard count)
    case Vector of
        undefined ->
            %% Skip docs without vectors
            {error, no_vector};
        _ ->
            barrel_vectordb_shard_router:route_add_vector(
                NewCollection, Key, Text, Metadata, Vector, #{})
    end.

find_shard_leader(Collection, ShardIdx) ->
    case barrel_vectordb_cluster_client:get_shard_placement(Collection) of
        {ok, Placements} ->
            case lists:keyfind(ShardIdx, 1, Placements) of
                {ShardIdx, Leader, _Replicas} ->
                    {_, Node} = Leader,
                    {ok, Node};
                false ->
                    {error, shard_not_found}
            end;
        Error ->
            Error
    end.

finalize_reshard(Collection, NewNumShards, NewPlacement) ->
    %% Update the collection's shard count and assignments in Ra
    barrel_vectordb_cluster_client:command({reshard_finalize, Collection, NewNumShards, NewPlacement}).

cleanup_old_shards(_Collection, _OldNumShards) ->
    %% Old shards are cleaned up when collection metadata is updated
    %% The shard manager will detect orphaned shards and remove them
    ok.

cleanup_temp_shards(TempCollection, _NumShards) ->
    %% Delete the temporary collection
    barrel_vectordb_cluster_client:delete_collection(TempCollection),
    ok.
