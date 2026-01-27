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
