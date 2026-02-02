%% @doc Scatter-gather for parallel search across shards.
%%
%% Searches are sent to all shards in parallel, results are merged
%% and top-K is returned.
%%
%% @end
-module(barrel_vectordb_scatter).

-export([search/4, search_vector/3]).
-export([search_bm25/3, search_hybrid/4]).
-export([search_local_shard/3]).  %% Called via RPC on remote node
-export([search_local_shard_bm25/3, search_local_shard_hybrid/4]).  %% Called via RPC on remote node

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
            barrel_vectordb_embed:embed(Query, Embedder)
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
                    %% collection_meta record: {collection_meta, name, dimension, num_shards, ...}
                    NumShards = element(4, CollectionMeta),  %% num_shards is element 4
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

%%====================================================================
%% BM25 Search Functions
%%====================================================================

%% @doc BM25 text search across all shards
search_bm25(Collection, Query, Opts) ->
    K = maps:get(k, Opts, 10),
    %% Get k * 2 from each shard to have good coverage
    ShardK = K * 2,
    ShardOpts = Opts#{k => ShardK, collection => Collection},

    case get_all_shard_stores(Collection) of
        {ok, ShardInfos} when ShardInfos =/= [] ->
            %% Scatter to all shards in parallel
            Results = scatter_bm25_search(ShardInfos, Query, ShardOpts),
            %% Gather and merge results
            Merged = gather_bm25_results(Results),
            %% Sort by score (descending) and take top K
            Sorted = lists:sublist(
                lists:sort(fun({_, ScoreA}, {_, ScoreB}) ->
                    ScoreA >= ScoreB
                end, Merged),
                K),
            {ok, Sorted};
        {ok, []} ->
            {ok, []};
        {error, _} = Error ->
            Error
    end.

%% @doc Hybrid search (BM25 + vector) across all shards
search_hybrid(Collection, Query, Opts, EmbedderInfo) ->
    K = maps:get(k, Opts, 10),
    BM25Weight = maps:get(bm25_weight, Opts, 0.5),
    VectorWeight = maps:get(vector_weight, Opts, 0.5),
    Fusion = maps:get(fusion, Opts, rrf),
    ShardK = K * 2,
    ShardOpts = Opts#{k => ShardK, collection => Collection},

    %% First embed the query once
    case embed_query(Query, EmbedderInfo) of
        {ok, Vector} ->
            case get_all_shard_stores(Collection) of
                {ok, ShardInfos} when ShardInfos =/= [] ->
                    %% Scatter hybrid search to all shards in parallel
                    Results = scatter_hybrid_search(ShardInfos, Query, Vector, ShardOpts),
                    %% Gather results - each shard returns {BM25Results, VectorResults}
                    {AllBM25, AllVector} = gather_hybrid_results(Results),
                    %% Merge results using fusion algorithm
                    Merged = case Fusion of
                        rrf -> rrf_merge(AllBM25, AllVector, K, BM25Weight, VectorWeight);
                        linear -> linear_merge(AllBM25, AllVector, K, BM25Weight, VectorWeight)
                    end,
                    {ok, Merged};
                {ok, []} ->
                    {ok, []};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Called via RPC on remote node for BM25 search
search_local_shard_bm25(ShardIdx, Query, Opts) ->
    Collection = maps:get(collection, Opts, undefined),
    case Collection of
        undefined ->
            {error, collection_not_specified};
        _ ->
            ShardId = {Collection, ShardIdx},
            case barrel_vectordb_shard_manager:get_local_store(ShardId) of
                {ok, StoreName} ->
                    barrel_vectordb:search_bm25(StoreName, Query, Opts);
                {error, _} = Error ->
                    Error
            end
    end.

%% @doc Called via RPC on remote node for hybrid search
search_local_shard_hybrid(ShardIdx, Query, Vector, Opts) ->
    Collection = maps:get(collection, Opts, undefined),
    case Collection of
        undefined ->
            {error, collection_not_specified};
        _ ->
            ShardId = {Collection, ShardIdx},
            case barrel_vectordb_shard_manager:get_local_store(ShardId) of
                {ok, StoreName} ->
                    %% Run both searches and return combined results
                    BM25Results = case barrel_vectordb:search_bm25(StoreName, Query, Opts) of
                        {ok, R1} -> R1;
                        _ -> []
                    end,
                    VectorResults = case barrel_vectordb:search_vector(StoreName, Vector, Opts) of
                        {ok, R2} -> R2;
                        _ -> []
                    end,
                    {ok, {BM25Results, VectorResults}};
                {error, _} = Error ->
                    Error
            end
    end.

%% Internal BM25 functions

scatter_bm25_search(ShardInfos, Query, Opts) ->
    Parent = self(),
    Ref = make_ref(),

    %% Spawn workers for each shard
    Pids = lists:map(
        fun(ShardInfo) ->
            spawn_link(fun() ->
                Result = search_shard_bm25(ShardInfo, Query, Opts),
                Parent ! {Ref, ShardInfo, Result}
            end)
        end,
        ShardInfos),

    %% Collect results with timeout
    collect_results(Ref, length(Pids), [], ?SCATTER_TIMEOUT).

search_shard_bm25({local, StoreName}, Query, Opts) ->
    barrel_vectordb:search_bm25(StoreName, Query, Opts);
search_shard_bm25({remote, Node, ShardIdx}, Query, Opts) ->
    case rpc:call(Node, barrel_vectordb_scatter, search_local_shard_bm25, [ShardIdx, Query, Opts], ?SCATTER_TIMEOUT) of
        {badrpc, Reason} ->
            {error, {rpc_failed, Reason}};
        Result ->
            Result
    end.

gather_bm25_results(Results) ->
    %% Results are lists of {DocId, Score} tuples from each shard
    %% Deduplicate by DocId, keeping highest score
    maps:to_list(
        lists:foldl(
            fun({DocId, Score}, Acc) ->
                case maps:get(DocId, Acc, undefined) of
                    undefined ->
                        maps:put(DocId, Score, Acc);
                    ExistingScore when Score > ExistingScore ->
                        maps:put(DocId, Score, Acc);
                    _ ->
                        Acc
                end
            end,
            #{},
            lists:flatten(Results))).

scatter_hybrid_search(ShardInfos, Query, Vector, Opts) ->
    Parent = self(),
    Ref = make_ref(),

    %% Spawn workers for each shard
    Pids = lists:map(
        fun(ShardInfo) ->
            spawn_link(fun() ->
                Result = search_shard_hybrid(ShardInfo, Query, Vector, Opts),
                Parent ! {Ref, ShardInfo, Result}
            end)
        end,
        ShardInfos),

    %% Collect results with timeout
    collect_hybrid_results(Ref, length(Pids), [], ?SCATTER_TIMEOUT).

search_shard_hybrid({local, StoreName}, Query, Vector, Opts) ->
    %% Run both searches locally
    BM25Results = case barrel_vectordb:search_bm25(StoreName, Query, Opts) of
        {ok, R1} -> R1;
        _ -> []
    end,
    VectorResults = case barrel_vectordb:search_vector(StoreName, Vector, Opts) of
        {ok, R2} -> R2;
        _ -> []
    end,
    {ok, {BM25Results, VectorResults}};
search_shard_hybrid({remote, Node, ShardIdx}, Query, Vector, Opts) ->
    case rpc:call(Node, barrel_vectordb_scatter, search_local_shard_hybrid, [ShardIdx, Query, Vector, Opts], ?SCATTER_TIMEOUT) of
        {badrpc, Reason} ->
            {error, {rpc_failed, Reason}};
        Result ->
            Result
    end.

collect_hybrid_results(_Ref, 0, Results, _Timeout) ->
    Results;
collect_hybrid_results(Ref, Remaining, Results, Timeout) ->
    receive
        {Ref, _ShardInfo, {ok, {BM25Results, VectorResults}}} ->
            collect_hybrid_results(Ref, Remaining - 1, [{BM25Results, VectorResults} | Results], Timeout);
        {Ref, _ShardInfo, {error, _Reason}} ->
            %% Skip failed shards
            collect_hybrid_results(Ref, Remaining - 1, Results, Timeout)
    after Timeout ->
        %% Timeout - return what we have
        Results
    end.

gather_hybrid_results(Results) ->
    %% Separate and flatten BM25 and vector results
    {AllBM25, AllVector} = lists:foldl(
        fun({BM25, Vector}, {BM25Acc, VectorAcc}) ->
            {BM25 ++ BM25Acc, Vector ++ VectorAcc}
        end,
        {[], []},
        Results),
    {gather_bm25_results([AllBM25]), gather_results(AllVector)}.

%% RRF (Reciprocal Rank Fusion) merge algorithm
rrf_merge(BM25Results, VectorResults, K, BM25Weight, VectorWeight) ->
    RRFk = 60,  %% Standard RRF constant

    %% Build rank maps
    BM25Ranks = build_rank_map([Id || {Id, _} <- BM25Results]),
    VectorRanks = build_rank_map([maps:get(key, R) || R <- VectorResults]),

    %% Get all unique IDs
    BM25Ids = sets:from_list([Id || {Id, _} <- BM25Results]),
    VectorIds = sets:from_list([maps:get(key, R) || R <- VectorResults]),
    AllIds = sets:to_list(sets:union(BM25Ids, VectorIds)),

    %% Calculate RRF scores
    Scores = lists:map(
        fun(Id) ->
            BM25Rank = maps:get(Id, BM25Ranks, 1000),
            VectorRank = maps:get(Id, VectorRanks, 1000),
            BM25RRF = BM25Weight / (RRFk + BM25Rank),
            VectorRRF = VectorWeight / (RRFk + VectorRank),
            {Id, BM25RRF + VectorRRF}
        end,
        AllIds),

    %% Sort by score and take top K
    Sorted = lists:sort(fun({_, A}, {_, B}) -> A >= B end, Scores),
    lists:sublist([#{key => Id, score => Score} || {Id, Score} <- Sorted], K).

%% Linear fusion merge algorithm
linear_merge(BM25Results, VectorResults, K, BM25Weight, VectorWeight) ->
    %% Normalize BM25 scores to [0, 1]
    BM25Normalized = normalize_bm25_scores(BM25Results),
    %% Vector scores are already in [0, 1] range (cosine similarity)

    %% Build score maps
    BM25Scores = maps:from_list(BM25Normalized),
    VectorScores = maps:from_list([{maps:get(key, R), maps:get(score, R, 0.0)} || R <- VectorResults]),

    %% Get all unique IDs
    AllIds = sets:to_list(sets:union(
        sets:from_list(maps:keys(BM25Scores)),
        sets:from_list(maps:keys(VectorScores)))),

    %% Calculate combined scores
    Scores = lists:map(
        fun(Id) ->
            BM25Score = maps:get(Id, BM25Scores, 0.0),
            VectorScore = maps:get(Id, VectorScores, 0.0),
            Combined = BM25Weight * BM25Score + VectorWeight * VectorScore,
            {Id, Combined}
        end,
        AllIds),

    %% Sort by score and take top K
    Sorted = lists:sort(fun({_, A}, {_, B}) -> A >= B end, Scores),
    lists:sublist([#{key => Id, score => Score} || {Id, Score} <- Sorted], K).

build_rank_map(Ids) ->
    {Map, _} = lists:foldl(
        fun(Id, {Acc, Rank}) ->
            {maps:put(Id, Rank, Acc), Rank + 1}
        end,
        {#{}, 1},
        Ids),
    Map.

normalize_bm25_scores([]) ->
    [];
normalize_bm25_scores(Results) ->
    MaxScore = lists:max([Score || {_, Score} <- Results]),
    case MaxScore of
        N when N == 0.0 -> Results;
        _ -> [{Id, Score / MaxScore} || {Id, Score} <- Results]
    end.
