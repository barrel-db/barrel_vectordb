%% @doc Unit tests for barrel_vectordb cluster components.
%%
%% Tests for cluster modules that can run without a full multi-node setup.
%% For full integration tests, use CT with peer nodes.
%%
%% @end
-module(barrel_vectordb_cluster_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Shard Locator Tests
%%====================================================================

shard_locator_test_() ->
    {foreach,
     fun() -> ok end,
     fun(_) -> ok end,
     [
        {"consistent hash for same key", fun test_consistent_hash/0},
        {"different keys distribute", fun test_key_distribution/0},
        {"all shards returns correct list", fun test_all_shards/0}
     ]}.

test_consistent_hash() ->
    Key = <<"test-doc-123">>,
    NumShards = 4,
    %% Same key should always map to same shard
    Shard1 = barrel_vectordb_shard_locator:shard_for_key(Key, NumShards),
    Shard2 = barrel_vectordb_shard_locator:shard_for_key(Key, NumShards),
    Shard3 = barrel_vectordb_shard_locator:shard_for_key(Key, NumShards),
    ?assertEqual(Shard1, Shard2),
    ?assertEqual(Shard2, Shard3).

test_key_distribution() ->
    NumShards = 4,
    Keys = [<<"doc-", (integer_to_binary(N))/binary>> || N <- lists:seq(1, 100)],
    Shards = [barrel_vectordb_shard_locator:shard_for_key(K, NumShards) || K <- Keys],
    %% All shard indices should be in valid range
    ?assert(lists:all(fun(S) -> S >= 0 andalso S < NumShards end, Shards)),
    %% Should hit multiple shards (not all same)
    UniqueShards = lists:usort(Shards),
    ?assert(length(UniqueShards) > 1).

test_all_shards() ->
    ?assertEqual([0, 1, 2, 3], barrel_vectordb_shard_locator:all_shards(4)),
    ?assertEqual([0], barrel_vectordb_shard_locator:all_shards(1)),
    ?assertEqual([0, 1, 2, 3, 4, 5, 6, 7], barrel_vectordb_shard_locator:all_shards(8)).

%%====================================================================
%% Ra State Machine Tests
%%====================================================================

ra_state_machine_test_() ->
    {foreach,
     fun setup_ra_sm/0,
     fun cleanup_ra_sm/1,
     [
        {"add node to empty state", fun test_add_node/0},
        {"remove node", fun test_remove_node/0},
        {"create collection", fun test_create_collection/0},
        {"create collection with backend", fun test_create_collection_with_backend/0},
        {"delete collection", fun test_delete_collection/0}
     ]}.

setup_ra_sm() ->
    #{}.

cleanup_ra_sm(_) ->
    ok.

%% Helper to access state record fields
get_nodes({cluster_state, Nodes, _, _, _}) -> Nodes.
get_collections({cluster_state, _, Collections, _, _}) -> Collections.

test_add_node() ->
    State0 = barrel_vectordb_ra_sm:init(#{}),
    NodeId = {barrel_vectordb, 'test@localhost'},
    %% NodeInfo must be a #node_info{} record
    NodeInfo = {node_info, 'test@localhost', <<"localhost:8080">>, joining, 0},
    {State1, ok, _Effects} = barrel_vectordb_ra_sm:apply(
        #{index => 1}, {join_cluster, NodeId, NodeInfo}, State0),
    Nodes = get_nodes(State1),
    ?assert(maps:is_key(NodeId, Nodes)).

test_remove_node() ->
    State0 = barrel_vectordb_ra_sm:init(#{}),
    NodeId = {barrel_vectordb, 'test@localhost'},
    NodeInfo = {node_info, 'test@localhost', <<"localhost:8080">>, joining, 0},
    {State1, ok, _} = barrel_vectordb_ra_sm:apply(
        #{index => 1}, {join_cluster, NodeId, NodeInfo}, State0),
    {State2, ok, _} = barrel_vectordb_ra_sm:apply(
        #{index => 2}, {leave_cluster, NodeId}, State1),
    Nodes = get_nodes(State2),
    ?assertEqual(#{}, Nodes).

test_create_collection() ->
    State0 = barrel_vectordb_ra_sm:init(#{}),
    Name = <<"test_collection">>,
    Config = #{dimension => 768, shards => 4, replication_factor => 2},
    %% Placement: list of {ShardIdx, Leader, Replicas}
    Placement = [{0, {barrel_vectordb, node()}, []}],
    {State1, {ok, _Meta}, _Effects} = barrel_vectordb_ra_sm:apply(
        #{index => 1}, {create_collection, Name, Config, Placement}, State0),
    Collections = get_collections(State1),
    ?assert(maps:is_key(Name, Collections)).

test_create_collection_with_backend() ->
    State0 = barrel_vectordb_ra_sm:init(#{}),
    Name = <<"test_collection_faiss">>,
    Config = #{dimension => 768, shards => 2, replication_factor => 1,
               backend => faiss, backend_config => #{index_type => <<"HNSW32">>}},
    Placement = [{0, {barrel_vectordb, node()}, []}, {1, {barrel_vectordb, node()}, []}],
    {State1, {ok, Meta}, _Effects} = barrel_vectordb_ra_sm:apply(
        #{index => 1}, {create_collection, Name, Config, Placement}, State0),
    Collections = get_collections(State1),
    ?assert(maps:is_key(Name, Collections)),
    %% Verify backend is stored in meta (element 8 is backend)
    ?assertEqual(faiss, element(8, Meta)),
    %% Verify backend_config is stored (element 9)
    ?assertEqual(#{index_type => <<"HNSW32">>}, element(9, Meta)).

test_delete_collection() ->
    State0 = barrel_vectordb_ra_sm:init(#{}),
    Name = <<"test_collection">>,
    Config = #{dimension => 768, shards => 1, replication_factor => 1},
    Placement = [{0, {barrel_vectordb, node()}, []}],
    {State1, {ok, _}, _} = barrel_vectordb_ra_sm:apply(
        #{index => 1}, {create_collection, Name, Config, Placement}, State0),
    {State2, ok, _} = barrel_vectordb_ra_sm:apply(
        #{index => 2}, {delete_collection, Name}, State1),
    Collections = get_collections(State2),
    ?assertEqual(false, maps:is_key(Name, Collections)).

%%====================================================================
%% Scatter-Gather Tests (unit tests for result merging)
%%====================================================================

scatter_gather_test_() ->
    [
        {"gather results deduplicates by key", fun test_gather_dedup/0},
        {"gather results keeps higher score", fun test_gather_higher_score/0}
    ].

test_gather_dedup() ->
    Results = [
        #{key => <<"doc1">>, score => 0.9, text => <<"hello">>},
        #{key => <<"doc2">>, score => 0.8, text => <<"world">>},
        #{key => <<"doc1">>, score => 0.85, text => <<"hello">>}  %% duplicate
    ],
    Merged = gather_results(Results),
    %% Should only have 2 unique docs
    ?assertEqual(2, length(Merged)).

test_gather_higher_score() ->
    Results = [
        #{key => <<"doc1">>, score => 0.7, text => <<"low">>},
        #{key => <<"doc1">>, score => 0.9, text => <<"high">>}  %% higher score
    ],
    Merged = gather_results(Results),
    [Doc] = Merged,
    %% Should keep the higher score version
    ?assertEqual(0.9, maps:get(score, Doc)).

%% Helper - copy of gather_results logic for testing
gather_results(Results) ->
    maps:values(
        lists:foldl(
            fun(Result, Acc) ->
                Key = maps:get(key, Result),
                case maps:get(Key, Acc, undefined) of
                    undefined ->
                        maps:put(Key, Result, Acc);
                    Existing ->
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
%% API Mode Detection Tests
%%====================================================================

api_test_() ->
    [
        {"is_clustered returns false when mesh not running", fun test_not_clustered/0}
    ].

test_not_clustered() ->
    %% When mesh supervisor is not running, should return false
    ?assertEqual(false, barrel_vectordb:is_clustered()).

%%====================================================================
%% BM25 Cluster Tests
%%====================================================================

bm25_cluster_test_() ->
    {foreach,
     fun setup_ra_sm/0,
     fun cleanup_ra_sm/1,
     [
        {"create collection with bm25_backend=disk", fun test_create_collection_with_bm25_disk/0},
        {"create collection with bm25_backend=memory", fun test_create_collection_with_bm25_memory/0},
        {"create collection with bm25_backend=none (default)", fun test_create_collection_with_bm25_none/0}
     ]}.

test_create_collection_with_bm25_disk() ->
    State0 = barrel_vectordb_ra_sm:init(#{}),
    Name = <<"test_bm25_disk">>,
    Config = #{
        dimension => 768,
        shards => 2,
        replication_factor => 1,
        backend => diskann,
        backend_config => #{},
        bm25_backend => disk,
        bm25_config => #{hot_max_size => 10000, k1 => 1.5}
    },
    Placement = [{0, {barrel_vectordb, node()}, []}, {1, {barrel_vectordb, node()}, []}],
    {State1, {ok, Meta}, _Effects} = barrel_vectordb_ra_sm:apply(
        #{index => 1}, {create_collection, Name, Config, Placement}, State0),
    Collections = get_collections(State1),
    ?assert(maps:is_key(Name, Collections)),
    %% Verify bm25_backend is stored in meta (element 10)
    ?assertEqual(disk, element(10, Meta)),
    %% Verify bm25_config is stored (element 11)
    ?assertEqual(#{hot_max_size => 10000, k1 => 1.5}, element(11, Meta)).

test_create_collection_with_bm25_memory() ->
    State0 = barrel_vectordb_ra_sm:init(#{}),
    Name = <<"test_bm25_memory">>,
    Config = #{
        dimension => 768,
        shards => 1,
        replication_factor => 1,
        bm25_backend => memory,
        bm25_config => #{k1 => 1.2, b => 0.75}
    },
    Placement = [{0, {barrel_vectordb, node()}, []}],
    {State1, {ok, Meta}, _Effects} = barrel_vectordb_ra_sm:apply(
        #{index => 1}, {create_collection, Name, Config, Placement}, State0),
    Collections = get_collections(State1),
    ?assert(maps:is_key(Name, Collections)),
    ?assertEqual(memory, element(10, Meta)),
    ?assertEqual(#{k1 => 1.2, b => 0.75}, element(11, Meta)).

test_create_collection_with_bm25_none() ->
    State0 = barrel_vectordb_ra_sm:init(#{}),
    Name = <<"test_no_bm25">>,
    Config = #{dimension => 768, shards => 1},  %% No bm25_backend specified
    Placement = [{0, {barrel_vectordb, node()}, []}],
    {State1, {ok, Meta}, _Effects} = barrel_vectordb_ra_sm:apply(
        #{index => 1}, {create_collection, Name, Config, Placement}, State0),
    Collections = get_collections(State1),
    ?assert(maps:is_key(Name, Collections)),
    %% Default should be 'none'
    ?assertEqual(none, element(10, Meta)),
    ?assertEqual(#{}, element(11, Meta)).

%%====================================================================
%% BM25 Scatter-Gather Tests
%%====================================================================

bm25_scatter_gather_test_() ->
    [
        {"gather BM25 results deduplicates by doc_id", fun test_bm25_gather_dedup/0},
        {"gather BM25 results keeps higher score", fun test_bm25_gather_higher_score/0},
        {"RRF merge combines BM25 and vector results", fun test_rrf_merge/0},
        {"RRF merge with disjoint results", fun test_rrf_merge_disjoint/0},
        {"linear merge combines scores", fun test_linear_merge/0},
        {"normalize BM25 scores", fun test_normalize_bm25_scores/0}
    ].

test_bm25_gather_dedup() ->
    Results = [
        [{<<"doc1">>, 2.5}, {<<"doc2">>, 1.8}],
        [{<<"doc2">>, 1.5}, {<<"doc3">>, 1.2}]  %% doc2 appears in both
    ],
    Merged = gather_bm25_results(Results),
    %% Should have 3 unique docs
    ?assertEqual(3, length(Merged)),
    %% doc2 should have the higher score (1.8)
    {_, Doc2Score} = lists:keyfind(<<"doc2">>, 1, Merged),
    ?assertEqual(1.8, Doc2Score).

test_bm25_gather_higher_score() ->
    Results = [
        [{<<"doc1">>, 1.0}],
        [{<<"doc1">>, 2.5}]  %% higher score
    ],
    Merged = gather_bm25_results(Results),
    [{<<"doc1">>, Score}] = Merged,
    ?assertEqual(2.5, Score).

test_rrf_merge() ->
    BM25Results = [{<<"doc1">>, 2.5}, {<<"doc2">>, 1.8}, {<<"doc3">>, 1.2}],
    VectorResults = [
        #{key => <<"doc1">>, score => 0.9},
        #{key => <<"doc4">>, score => 0.85},
        #{key => <<"doc2">>, score => 0.7}
    ],
    Merged = rrf_merge(BM25Results, VectorResults, 3, 0.5, 0.5),
    %% doc1 appears in both, should be ranked high
    ?assertEqual(3, length(Merged)),
    [Top | _] = Merged,
    ?assertEqual(<<"doc1">>, maps:get(key, Top)).

test_rrf_merge_disjoint() ->
    %% BM25 and vector have completely different docs
    BM25Results = [{<<"bm25_doc1">>, 2.5}, {<<"bm25_doc2">>, 1.8}],
    VectorResults = [
        #{key => <<"vec_doc1">>, score => 0.9},
        #{key => <<"vec_doc2">>, score => 0.85}
    ],
    Merged = rrf_merge(BM25Results, VectorResults, 4, 0.5, 0.5),
    %% Should include all 4 docs
    ?assertEqual(4, length(Merged)),
    Keys = [maps:get(key, R) || R <- Merged],
    ?assert(lists:member(<<"bm25_doc1">>, Keys)),
    ?assert(lists:member(<<"vec_doc1">>, Keys)).

test_linear_merge() ->
    BM25Results = [{<<"doc1">>, 2.5}, {<<"doc2">>, 1.0}],  %% will be normalized
    VectorResults = [
        #{key => <<"doc1">>, score => 0.8},
        #{key => <<"doc2">>, score => 0.4}
    ],
    Merged = linear_merge(BM25Results, VectorResults, 2, 0.5, 0.5),
    ?assertEqual(2, length(Merged)),
    %% doc1: normalized BM25 = 1.0, vector = 0.8 -> 0.5*1.0 + 0.5*0.8 = 0.9
    %% doc2: normalized BM25 = 0.4, vector = 0.4 -> 0.5*0.4 + 0.5*0.4 = 0.4
    [Top | _] = Merged,
    ?assertEqual(<<"doc1">>, maps:get(key, Top)).

test_normalize_bm25_scores() ->
    Results = [{<<"doc1">>, 5.0}, {<<"doc2">>, 2.5}],
    Normalized = normalize_bm25_scores(Results),
    {_, Score1} = lists:keyfind(<<"doc1">>, 1, Normalized),
    {_, Score2} = lists:keyfind(<<"doc2">>, 1, Normalized),
    ?assertEqual(1.0, Score1),  %% max score normalized to 1.0
    ?assertEqual(0.5, Score2).  %% 2.5/5.0 = 0.5

%% Helper functions for BM25 scatter-gather tests

gather_bm25_results(Results) ->
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

rrf_merge(BM25Results, VectorResults, K, BM25Weight, VectorWeight) ->
    RRFk = 60,
    BM25Ranks = build_rank_map([Id || {Id, _} <- BM25Results]),
    VectorRanks = build_rank_map([maps:get(key, R) || R <- VectorResults]),
    BM25Ids = sets:from_list([Id || {Id, _} <- BM25Results]),
    VectorIds = sets:from_list([maps:get(key, R) || R <- VectorResults]),
    AllIds = sets:to_list(sets:union(BM25Ids, VectorIds)),
    Scores = lists:map(
        fun(Id) ->
            BM25Rank = maps:get(Id, BM25Ranks, 1000),
            VectorRank = maps:get(Id, VectorRanks, 1000),
            BM25RRF = BM25Weight / (RRFk + BM25Rank),
            VectorRRF = VectorWeight / (RRFk + VectorRank),
            {Id, BM25RRF + VectorRRF}
        end,
        AllIds),
    Sorted = lists:sort(fun({_, A}, {_, B}) -> A >= B end, Scores),
    lists:sublist([#{key => Id, score => Score} || {Id, Score} <- Sorted], K).

linear_merge(BM25Results, VectorResults, K, BM25Weight, VectorWeight) ->
    BM25Normalized = normalize_bm25_scores(BM25Results),
    BM25Scores = maps:from_list(BM25Normalized),
    VectorScores = maps:from_list([{maps:get(key, R), maps:get(score, R, 0.0)} || R <- VectorResults]),
    AllIds = sets:to_list(sets:union(
        sets:from_list(maps:keys(BM25Scores)),
        sets:from_list(maps:keys(VectorScores)))),
    Scores = lists:map(
        fun(Id) ->
            BM25Score = maps:get(Id, BM25Scores, 0.0),
            VectorScore = maps:get(Id, VectorScores, 0.0),
            Combined = BM25Weight * BM25Score + VectorWeight * VectorScore,
            {Id, Combined}
        end,
        AllIds),
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
