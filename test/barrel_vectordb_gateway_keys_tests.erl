-module(barrel_vectordb_gateway_keys_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_PATH, "/tmp/barrel_gateway_keys_test").

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

gateway_keys_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun test_create_key/1,
         fun test_validate_key/1,
         fun test_revoke_key/1,
         fun test_list_keys/1,
         fun test_get_tenant_limits/1,
         fun test_key_accessors/1
     ]}.

%%====================================================================
%% Key Creation Tests
%%====================================================================

test_create_key(_Pid) ->
    fun() ->
        TenantId = <<"test_tenant">>,

        %% Create a key with default options
        {ok, ApiKey} = barrel_vectordb_gateway_keys:create_key(TenantId, #{}),

        %% Key should start with prefix
        ?assert(binary:match(ApiKey, <<"bvdb_">>) =:= {0, 5}),

        %% Key should be longer than prefix
        ?assert(byte_size(ApiKey) > 5),

        %% Create a key with custom options
        Opts = #{
            rpm_limit => 500,
            max_vectors => 50000,
            max_collections => 20,
            max_storage_mb => 2048
        },
        {ok, ApiKey2} = barrel_vectordb_gateway_keys:create_key(TenantId, Opts),

        %% Verify the key is valid
        {ok, KeyRecord} = barrel_vectordb_gateway_keys:validate_key(ApiKey2),
        ?assertEqual(500, barrel_vectordb_gateway_keys:key_rpm_limit(KeyRecord)),
        ?assertEqual(50000, barrel_vectordb_gateway_keys:key_max_vectors(KeyRecord))
    end.

test_validate_key(_Pid) ->
    fun() ->
        TenantId = <<"validate_tenant">>,

        %% Create a key
        {ok, ApiKey} = barrel_vectordb_gateway_keys:create_key(TenantId, #{}),

        %% Validate it
        {ok, KeyRecord} = barrel_vectordb_gateway_keys:validate_key(ApiKey),
        ?assertEqual(TenantId, barrel_vectordb_gateway_keys:key_tenant_id(KeyRecord)),

        %% Try to validate a non-existent key
        ?assertEqual({error, invalid}, barrel_vectordb_gateway_keys:validate_key(<<"nonexistent_key">>))
    end.

test_revoke_key(_Pid) ->
    fun() ->
        TenantId = <<"revoke_tenant">>,

        %% Create a key
        {ok, ApiKey} = barrel_vectordb_gateway_keys:create_key(TenantId, #{}),

        %% Verify it's valid
        ?assertMatch({ok, _}, barrel_vectordb_gateway_keys:validate_key(ApiKey)),

        %% Revoke it
        ?assertEqual(ok, barrel_vectordb_gateway_keys:revoke_key(ApiKey)),

        %% Verify it's no longer valid
        ?assertEqual({error, invalid}, barrel_vectordb_gateway_keys:validate_key(ApiKey))
    end.

test_list_keys(_Pid) ->
    fun() ->
        TenantId = <<"list_tenant">>,

        %% Create multiple keys
        {ok, _Key1} = barrel_vectordb_gateway_keys:create_key(TenantId, #{}),
        {ok, _Key2} = barrel_vectordb_gateway_keys:create_key(TenantId, #{rpm_limit => 200}),
        {ok, _Key3} = barrel_vectordb_gateway_keys:create_key(<<"other_tenant">>, #{}),

        %% List keys for our tenant
        {ok, Keys} = barrel_vectordb_gateway_keys:list_keys(TenantId),

        %% Should have exactly 2 keys
        ?assertEqual(2, length(Keys)),

        %% Each key should have sanitized fields
        [First | _] = Keys,
        ?assert(maps:is_key(key_prefix, First)),
        ?assert(maps:is_key(rpm_limit, First)),
        ?assert(maps:is_key(max_vectors, First)),

        %% Key prefix should not be the full key
        ?assert(byte_size(maps:get(key_prefix, First)) =< 12)
    end.

test_get_tenant_limits(_Pid) ->
    fun() ->
        TenantId = <<"limits_tenant">>,

        %% Create a key with specific limits
        Opts = #{
            rpm_limit => 300,
            max_vectors => 25000,
            max_collections => 15,
            max_storage_mb => 512
        },
        {ok, _} = barrel_vectordb_gateway_keys:create_key(TenantId, Opts),

        %% Get tenant limits
        {ok, Limits} = barrel_vectordb_gateway_keys:get_tenant_limits(TenantId),

        ?assertEqual(300, maps:get(rpm_limit, Limits)),
        ?assertEqual(25000, maps:get(max_vectors, Limits)),
        ?assertEqual(15, maps:get(max_collections, Limits)),
        ?assertEqual(512, maps:get(max_storage_mb, Limits)),

        %% Non-existent tenant should return not_found
        ?assertEqual({error, not_found}, barrel_vectordb_gateway_keys:get_tenant_limits(<<"no_such_tenant">>))
    end.

test_key_accessors(_Pid) ->
    fun() ->
        TenantId = <<"accessor_tenant">>,
        Opts = #{rpm_limit => 999, max_vectors => 88888},

        {ok, ApiKey} = barrel_vectordb_gateway_keys:create_key(TenantId, Opts),
        {ok, KeyRecord} = barrel_vectordb_gateway_keys:validate_key(ApiKey),

        %% Test all accessors
        ?assertEqual(ApiKey, barrel_vectordb_gateway_keys:key_id(KeyRecord)),
        ?assertEqual(TenantId, barrel_vectordb_gateway_keys:key_tenant_id(KeyRecord)),
        ?assertEqual(999, barrel_vectordb_gateway_keys:key_rpm_limit(KeyRecord)),
        ?assertEqual(88888, barrel_vectordb_gateway_keys:key_max_vectors(KeyRecord)),

        %% Created at should be a reasonable timestamp
        CreatedAt = element(4, KeyRecord),
        Now = erlang:system_time(second),
        ?assert(CreatedAt > Now - 10),
        ?assert(CreatedAt =< Now)
    end.
