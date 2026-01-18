-module(barrel_vectordb_system_db_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_PATH, "/tmp/barrel_system_db_test").

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

system_db_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun test_put_get_key/1,
         fun test_delete_key/1,
         fun test_list_keys_by_tenant/1,
         fun test_quota_operations/1,
         fun test_get_tenant_ids/1,
         fun test_key_not_found/1
     ]}.

%%====================================================================
%% API Key Tests
%%====================================================================

test_put_get_key(_Pid) ->
    fun() ->
        %% Create a test key record
        KeyRecord = {api_key,
            <<"bvdb_test_key_123">>,
            <<"tenant1">>,
            erlang:system_time(second),
            100,  % rpm_limit
            1024, % max_storage_mb
            10000, % max_vectors
            10    % max_collections
        },

        %% Put the key
        ?assertEqual(ok, barrel_vectordb_system_db:put_key(KeyRecord)),

        %% Get the key back
        {ok, Retrieved} = barrel_vectordb_system_db:get_key(<<"bvdb_test_key_123">>),

        %% Verify fields
        ?assertEqual(<<"bvdb_test_key_123">>, element(2, Retrieved)),
        ?assertEqual(<<"tenant1">>, element(3, Retrieved)),
        ?assertEqual(100, element(5, Retrieved)),
        ?assertEqual(1024, element(6, Retrieved))
    end.

test_delete_key(_Pid) ->
    fun() ->
        %% Create and store a key
        KeyRecord = {api_key,
            <<"bvdb_delete_test">>,
            <<"tenant_del">>,
            erlang:system_time(second),
            100, 1024, 10000, 10
        },
        ?assertEqual(ok, barrel_vectordb_system_db:put_key(KeyRecord)),

        %% Verify it exists
        ?assertMatch({ok, _}, barrel_vectordb_system_db:get_key(<<"bvdb_delete_test">>)),

        %% Delete the key
        ?assertEqual(ok, barrel_vectordb_system_db:delete_key(<<"bvdb_delete_test">>)),

        %% Verify it's gone
        ?assertEqual({error, not_found}, barrel_vectordb_system_db:get_key(<<"bvdb_delete_test">>))
    end.

test_list_keys_by_tenant(_Pid) ->
    fun() ->
        TenantId = <<"multi_key_tenant">>,

        %% Create multiple keys for same tenant
        Key1 = {api_key, <<"bvdb_k1">>, TenantId, erlang:system_time(second), 100, 1024, 10000, 10},
        Key2 = {api_key, <<"bvdb_k2">>, TenantId, erlang:system_time(second), 200, 2048, 20000, 20},
        Key3 = {api_key, <<"bvdb_k3">>, <<"other_tenant">>, erlang:system_time(second), 100, 1024, 10000, 10},

        ok = barrel_vectordb_system_db:put_key(Key1),
        ok = barrel_vectordb_system_db:put_key(Key2),
        ok = barrel_vectordb_system_db:put_key(Key3),

        %% List keys for our tenant
        {ok, Keys} = barrel_vectordb_system_db:list_keys_by_tenant(TenantId),

        %% Should have exactly 2 keys
        ?assertEqual(2, length(Keys)),

        %% Verify both keys belong to our tenant
        KeyIds = [element(2, K) || K <- Keys],
        ?assert(lists:member(<<"bvdb_k1">>, KeyIds)),
        ?assert(lists:member(<<"bvdb_k2">>, KeyIds)),
        ?assertNot(lists:member(<<"bvdb_k3">>, KeyIds))
    end.

test_key_not_found(_Pid) ->
    fun() ->
        ?assertEqual({error, not_found}, barrel_vectordb_system_db:get_key(<<"nonexistent_key">>)),
        ?assertEqual({error, not_found}, barrel_vectordb_system_db:delete_key(<<"nonexistent_key">>))
    end.

%%====================================================================
%% Quota Tests
%%====================================================================

test_quota_operations(_Pid) ->
    fun() ->
        TenantId = <<"quota_test_tenant">>,

        %% Initialize quota
        ?assertEqual(ok, barrel_vectordb_system_db:init_quota(TenantId)),

        %% Get initial quota
        {ok, Quota1} = barrel_vectordb_system_db:get_quota(TenantId),
        ?assertEqual(TenantId, element(2, Quota1)),
        ?assertEqual(0, element(3, Quota1)), % storage_bytes
        ?assertEqual(0, element(4, Quota1)), % vector_count
        ?assertEqual(0, element(5, Quota1)), % collection_count

        %% Increment vectors
        ?assertEqual(ok, barrel_vectordb_system_db:update_quota(TenantId, {incr_vectors, 100})),
        {ok, Quota2} = barrel_vectordb_system_db:get_quota(TenantId),
        ?assertEqual(100, element(4, Quota2)),

        %% Increment collections
        ?assertEqual(ok, barrel_vectordb_system_db:update_quota(TenantId, {incr_collections, 5})),
        {ok, Quota3} = barrel_vectordb_system_db:get_quota(TenantId),
        ?assertEqual(5, element(5, Quota3)),

        %% Increment storage
        ?assertEqual(ok, barrel_vectordb_system_db:update_quota(TenantId, {incr_storage, 1000})),
        {ok, Quota4} = barrel_vectordb_system_db:get_quota(TenantId),
        ?assertEqual(1000, element(3, Quota4)),

        %% Decrement vectors
        ?assertEqual(ok, barrel_vectordb_system_db:update_quota(TenantId, {decr_vectors, 30})),
        {ok, Quota5} = barrel_vectordb_system_db:get_quota(TenantId),
        ?assertEqual(70, element(4, Quota5)),

        %% Decrement below zero should result in 0
        ?assertEqual(ok, barrel_vectordb_system_db:update_quota(TenantId, {decr_vectors, 1000})),
        {ok, Quota6} = barrel_vectordb_system_db:get_quota(TenantId),
        ?assertEqual(0, element(4, Quota6))
    end.

test_get_tenant_ids(_Pid) ->
    fun() ->
        %% Initialize quotas for multiple tenants
        ok = barrel_vectordb_system_db:init_quota(<<"tenant_a">>),
        ok = barrel_vectordb_system_db:init_quota(<<"tenant_b">>),
        ok = barrel_vectordb_system_db:init_quota(<<"tenant_c">>),

        %% Get all tenant IDs
        {ok, TenantIds} = barrel_vectordb_system_db:get_tenant_ids(),

        %% Should have at least 3 tenants
        ?assert(length(TenantIds) >= 3),
        ?assert(lists:member(<<"tenant_a">>, TenantIds)),
        ?assert(lists:member(<<"tenant_b">>, TenantIds)),
        ?assert(lists:member(<<"tenant_c">>, TenantIds))
    end.
