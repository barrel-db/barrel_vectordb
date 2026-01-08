%% @doc Scatter-gather for parallel search across shards.
%%
%% Searches are sent to all shards in parallel, results are merged
%% and top-K is returned.
%%
%% @end
-module(barrel_vectordb_scatter).

-export([search/4, search_vector/3]).
-export([search_local_shard/3]).  %% Called via RPC on remote node

-define(SCATTER_TIMEOUT, 30000).

%% @doc Search with text query across all shards
search(Collection, Query, Opts, EmbedderInfo) ->
    %% First embed the query once
    case embed_query(Query, EmbedderInfo) of
        {ok, Vector} ->
            search_vector(Collection, Vector, Opts);
        {error, _} = Error ->
            Error
    end.

%% @doc Search with vector query across all shards
search_vector(Collection, Vector, Opts) ->
    K = maps:get(k, Opts, 10),
    %% Get k * 2 from each shard to have good coverage for filtering
    ShardK = K * 2,
    ShardOpts = Opts#{k => ShardK},

    case get_all_shard_stores(Collection) of
        {ok, ShardInfos} when ShardInfos =/= [] ->
            %% Scatter to all shards in parallel
            Results = scatter_search(ShardInfos, Vector, ShardOpts),
            %% Gather and merge results
            Merged = gather_results(Results),
            %% Sort by score and take top K
            Sorted = lists:sublist(
                lists:sort(fun(A, B) ->
                    maps:get(score, A, 0.0) >= maps:get(score, B, 0.0)
                end, Merged),
                K),
            {ok, Sorted};
        {ok, []} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

%% Internal

embed_query(Query, EmbedderInfo) when is_map(EmbedderInfo) ->
    %% Use the embedder from EmbedderInfo
    case maps:get(embedder, EmbedderInfo, undefined) of
        undefined ->
            {error, embedder_not_configured};
        Embedder ->
            barrel_vectordb_embedder:embed(Query, Embedder)
    end;
embed_query(_Query, _) ->
    {error, embedder_not_configured}.

get_all_shard_stores(Collection) ->
    case barrel_vectordb_cluster_client:get_collections() of
        {ok, Collections} ->
            case maps:get(Collection, Collections, undefined) of
                undefined ->
                    {error, collection_not_found};
                CollectionMeta ->
                    NumShards = element(2, CollectionMeta),
                    get_shard_stores(Collection, NumShards)
            end;
        {error, _} = Error ->
            Error
    end.

get_shard_stores(Collection, NumShards) ->
    ShardIdxs = barrel_vectordb_shard_locator:all_shards(NumShards),
    ShardInfos = lists:filtermap(
        fun(ShardIdx) ->
            ShardId = {Collection, ShardIdx},
            case barrel_vectordb_shard_manager:get_local_store(ShardId) of
                {ok, StoreName} ->
                    {true, {local, StoreName}};
                {error, not_found} ->
                    %% Find remote leader
                    case get_remote_leader(Collection, ShardIdx) of
                        {ok, {Node, ShardIdx}} ->
                            {true, {remote, Node, ShardIdx}};
                        {error, _} ->
                            false
                    end
            end
        end,
        ShardIdxs),
    {ok, ShardInfos}.

get_remote_leader(Collection, ShardIdx) ->
    case barrel_vectordb_cluster_client:get_shard_placement(Collection) of
        {ok, Placements} ->
            case lists:keyfind(ShardIdx, 1, Placements) of
                {ShardIdx, Leader, _Replicas} ->
                    {_, Node} = Leader,
                    {ok, {Node, ShardIdx}};
                false ->
                    {error, shard_not_found}
            end;
        {error, _} = Error ->
            Error
    end.

scatter_search(ShardInfos, Vector, Opts) ->
    Parent = self(),
    Ref = make_ref(),

    %% Spawn workers for each shard
    Pids = lists:map(
        fun(ShardInfo) ->
            spawn_link(fun() ->
                Result = search_shard(ShardInfo, Vector, Opts),
                Parent ! {Ref, ShardInfo, Result}
            end)
        end,
        ShardInfos),

    %% Collect results with timeout
    collect_results(Ref, length(Pids), [], ?SCATTER_TIMEOUT).

search_shard({local, StoreName}, Vector, Opts) ->
    barrel_vectordb:search_vector(StoreName, Vector, Opts);
search_shard({remote, Node, ShardIdx}, Vector, Opts) ->
    case rpc:call(Node, barrel_vectordb_scatter, search_local_shard, [ShardIdx, Vector, Opts], ?SCATTER_TIMEOUT) of
        {badrpc, Reason} ->
            {error, {rpc_failed, Reason}};
        Result ->
            Result
    end.

%% @doc Called via RPC on remote node
search_local_shard(ShardIdx, Vector, Opts) ->
    %% This is called on the remote node - we need to find the local store
    %% ShardIdx alone isn't enough, we need the collection name from opts
    Collection = maps:get(collection, Opts, undefined),
    case Collection of
        undefined ->
            {error, collection_not_specified};
        _ ->
            ShardId = {Collection, ShardIdx},
            case barrel_vectordb_shard_manager:get_local_store(ShardId) of
                {ok, StoreName} ->
                    barrel_vectordb:search_vector(StoreName, Vector, Opts);
                {error, _} = Error ->
                    Error
            end
    end.

collect_results(_Ref, 0, Results, _Timeout) ->
    Results;
collect_results(Ref, Remaining, Results, Timeout) ->
    receive
        {Ref, _ShardInfo, {ok, ShardResults}} ->
            collect_results(Ref, Remaining - 1, ShardResults ++ Results, Timeout);
        {Ref, _ShardInfo, {error, _Reason}} ->
            %% Skip failed shards
            collect_results(Ref, Remaining - 1, Results, Timeout)
    after Timeout ->
        %% Timeout - return what we have
        Results
    end.

gather_results(Results) ->
    %% Remove duplicates by key (same doc might be in multiple shards during rebalancing)
    maps:values(
        lists:foldl(
            fun(Result, Acc) ->
                Key = maps:get(key, Result),
                case maps:get(Key, Acc, undefined) of
                    undefined ->
                        maps:put(Key, Result, Acc);
                    Existing ->
                        %% Keep higher score
                        ExistingScore = maps:get(score, Existing, 0.0),
                        NewScore = maps:get(score, Result, 0.0),
                        case NewScore > ExistingScore of
                            true -> maps:put(Key, Result, Acc);
                            false -> Acc
                        end
                end
            end,
            #{},
            Results)).
