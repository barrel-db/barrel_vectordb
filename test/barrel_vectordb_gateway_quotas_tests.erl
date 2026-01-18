-module(barrel_vectordb_gateway_quotas_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_PATH, "/tmp/barrel_gateway_quotas_test").

%%====================================================================
%% Test setup/teardown
%%====================================================================

setup() ->
    %% Clean up any existing test DB
    os:cmd("rm -rf " ++ ?TEST_PATH),
    {ok, Pid} = barrel_vectordb_system_db:start_link(#{path => ?TEST_PATH}),
    Pid.

cleanup(Pid) ->
    gen_server:stop(Pid),
    os:cmd("rm -rf " ++ ?TEST_PATH).

%%====================================================================
%% Test generators
%%====================================================================

gateway_quotas_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun test_check_can_add_vectors/1,
         fun test_check_can_create_collection/1,
         fun test_increment_decrement_vectors/1,
         fun test_increment_decrement_collections/1,
         fun test_storage_tracking/1,
         fun test_get_usage/1,
         fun test_quota_enforcement/1
     ]}.

%%====================================================================
%% Vector Quota Tests
%%====================================================================

test_check_can_add_vectors(_Pid) ->
    fun() ->
        TenantId = <<"vector_quota_tenant">>,

        %% Create a key with vector limit
        Opts = #{max_vectors => 1000},
        {ok, _} = barrel_vectordb_gateway_keys:create_key(TenantId, Opts),

        %% Should be able to add vectors when under limit
        ?assertEqual(ok, barrel_vectordb_gateway_quotas:check_can_add_vectors(TenantId, 500)),

        %% Add some vectors
        ok = barrel_vectordb_gateway_quotas:increment_vectors(TenantId, 800),

        %% Should still be able to add 200 more
        ?assertEqual(ok, barrel_vectordb_gateway_quotas:check_can_add_vectors(TenantId, 200)),

        %% But not 201 more
        ?assertEqual({error, quota_exceeded}, barrel_vectordb_gateway_quotas:check_can_add_vectors(TenantId, 201))
    end.

test_check_can_create_collection(_Pid) ->
    fun() ->
        TenantId = <<"collection_quota_tenant">>,

        %% Create a key with collection limit
        Opts = #{max_collections => 3},
        {ok, _} = barrel_vectordb_gateway_keys:create_key(TenantId, Opts),

        %% Should be able to create collections when under limit
        ?assertEqual(ok, barrel_vectordb_gateway_quotas:check_can_create_collection(TenantId)),

        %% Add 3 collections
        ok = barrel_vectordb_gateway_quotas:increment_collections(TenantId),
        ok = barrel_vectordb_gateway_quotas:increment_collections(TenantId),
        ok = barrel_vectordb_gateway_quotas:increment_collections(TenantId),

        %% Should not be able to create more
        ?assertEqual({error, quota_exceeded}, barrel_vectordb_gateway_quotas:check_can_create_collection(TenantId))
    end.

test_increment_decrement_vectors(_Pid) ->
    fun() ->
        TenantId = <<"inc_dec_vectors">>,

        %% Initialize quota
        barrel_vectordb_system_db:init_quota(TenantId),

        %% Increment vectors
        ok = barrel_vectordb_gateway_quotas:increment_vectors(TenantId, 100),
        ok = barrel_vectordb_gateway_quotas:increment_vectors(TenantId, 50),

        %% Check usage
        {ok, Usage} = barrel_vectordb_gateway_quotas:get_usage(TenantId),
        ?assertEqual(150, maps:get(vector_count, Usage)),

        %% Decrement vectors
        ok = barrel_vectordb_gateway_quotas:decrement_vectors(TenantId, 30),

        {ok, Usage2} = barrel_vectordb_gateway_quotas:get_usage(TenantId),
        ?assertEqual(120, maps:get(vector_count, Usage2))
    end.

test_increment_decrement_collections(_Pid) ->
    fun() ->
        TenantId = <<"inc_dec_collections">>,

        barrel_vectordb_system_db:init_quota(TenantId),

        %% Increment
        ok = barrel_vectordb_gateway_quotas:increment_collections(TenantId),
        ok = barrel_vectordb_gateway_quotas:increment_collections(TenantId),

        {ok, Usage} = barrel_vectordb_gateway_quotas:get_usage(TenantId),
        ?assertEqual(2, maps:get(collection_count, Usage)),

        %% Decrement
        ok = barrel_vectordb_gateway_quotas:decrement_collections(TenantId),

        {ok, Usage2} = barrel_vectordb_gateway_quotas:get_usage(TenantId),
        ?assertEqual(1, maps:get(collection_count, Usage2))
    end.

test_storage_tracking(_Pid) ->
    fun() ->
        TenantId = <<"storage_test">>,

        barrel_vectordb_system_db:init_quota(TenantId),

        %% Increment storage
        ok = barrel_vectordb_gateway_quotas:increment_storage(TenantId, 1000000), % 1MB
        ok = barrel_vectordb_gateway_quotas:increment_storage(TenantId, 500000),

        {ok, Usage} = barrel_vectordb_gateway_quotas:get_usage(TenantId),
        ?assertEqual(1500000, maps:get(storage_bytes, Usage)),

        %% Decrement storage
        ok = barrel_vectordb_gateway_quotas:decrement_storage(TenantId, 300000),

        {ok, Usage2} = barrel_vectordb_gateway_quotas:get_usage(TenantId),
        ?assertEqual(1200000, maps:get(storage_bytes, Usage2))
    end.

test_get_usage(_Pid) ->
    fun() ->
        TenantId = <<"usage_test">>,

        %% Get usage for non-existent tenant should return zeroes
        {ok, Usage1} = barrel_vectordb_gateway_quotas:get_usage(TenantId),
        ?assertEqual(TenantId, maps:get(tenant_id, Usage1)),
        ?assertEqual(0, maps:get(vector_count, Usage1)),
        ?assertEqual(0, maps:get(collection_count, Usage1)),
        ?assertEqual(0, maps:get(storage_bytes, Usage1)),

        %% Initialize and add some usage
        barrel_vectordb_system_db:init_quota(TenantId),
        ok = barrel_vectordb_gateway_quotas:increment_vectors(TenantId, 42),
        ok = barrel_vectordb_gateway_quotas:increment_collections(TenantId),
        ok = barrel_vectordb_gateway_quotas:increment_storage(TenantId, 12345),

        %% Check updated usage
        {ok, Usage2} = barrel_vectordb_gateway_quotas:get_usage(TenantId),
        ?assertEqual(42, maps:get(vector_count, Usage2)),
        ?assertEqual(1, maps:get(collection_count, Usage2)),
        ?assertEqual(12345, maps:get(storage_bytes, Usage2))
    end.

test_quota_enforcement(_Pid) ->
    fun() ->
        TenantId = <<"enforcement_test">>,

        %% Create key with tight limits
        Opts = #{max_vectors => 10, max_collections => 2},
        {ok, _} = barrel_vectordb_gateway_keys:create_key(TenantId, Opts),

        %% Should allow operations under limit
        ?assertEqual(ok, barrel_vectordb_gateway_quotas:check_can_add_vectors(TenantId, 5)),
        ?assertEqual(ok, barrel_vectordb_gateway_quotas:check_can_create_collection(TenantId)),

        %% Simulate using quota
        ok = barrel_vectordb_gateway_quotas:increment_vectors(TenantId, 10),
        ok = barrel_vectordb_gateway_quotas:increment_collections(TenantId),
        ok = barrel_vectordb_gateway_quotas:increment_collections(TenantId),

        %% Should now reject
        ?assertEqual({error, quota_exceeded}, barrel_vectordb_gateway_quotas:check_can_add_vectors(TenantId, 1)),
        ?assertEqual({error, quota_exceeded}, barrel_vectordb_gateway_quotas:check_can_create_collection(TenantId)),

        %% Free up some quota
        ok = barrel_vectordb_gateway_quotas:decrement_vectors(TenantId, 5),
        ok = barrel_vectordb_gateway_quotas:decrement_collections(TenantId),

        %% Should now allow again
        ?assertEqual(ok, barrel_vectordb_gateway_quotas:check_can_add_vectors(TenantId, 5)),
        ?assertEqual(ok, barrel_vectordb_gateway_quotas:check_can_create_collection(TenantId))
    end.
