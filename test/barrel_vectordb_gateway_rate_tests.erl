-module(barrel_vectordb_gateway_rate_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test setup/teardown
%%====================================================================

setup() ->
    %% Start the rate limiter
    {ok, Pid} = barrel_vectordb_gateway_rate:start_link(),
    Pid.

cleanup(Pid) ->
    gen_server:stop(Pid).

%%====================================================================
%% Test generators
%%====================================================================

gateway_rate_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
         fun test_check_rate_allows_requests/1,
         fun test_check_rate_with_custom_rpm/1,
         fun test_get_bucket_info/1,
         fun test_reset_bucket/1,
         fun test_rate_limiting/1,
         fun test_token_refill/1
     ]}.

%%====================================================================
%% Rate Limiting Tests
%%====================================================================

test_check_rate_allows_requests(_Pid) ->
    fun() ->
        TenantId = <<"rate_tenant_1">>,

        %% First request should always succeed
        ?assertEqual(ok, barrel_vectordb_gateway_rate:check_rate(TenantId, 100)),

        %% Subsequent requests should also succeed (under limit)
        ?assertEqual(ok, barrel_vectordb_gateway_rate:check_rate(TenantId, 100)),
        ?assertEqual(ok, barrel_vectordb_gateway_rate:check_rate(TenantId, 100))
    end.

test_check_rate_with_custom_rpm(_Pid) ->
    fun() ->
        TenantId = <<"rate_tenant_2">>,

        %% Use a high RPM limit
        ?assertEqual(ok, barrel_vectordb_gateway_rate:check_rate(TenantId, 1000)),

        %% Should have many tokens available
        {ok, Info} = barrel_vectordb_gateway_rate:get_bucket_info(TenantId),
        ?assert(maps:get(tokens, Info) >= 998),
        ?assertEqual(1000, maps:get(max_tokens, Info))
    end.

test_get_bucket_info(_Pid) ->
    fun() ->
        TenantId = <<"bucket_info_tenant">>,

        %% No bucket initially
        ?assertEqual({error, not_found}, barrel_vectordb_gateway_rate:get_bucket_info(TenantId)),

        %% Create bucket by making a request
        ?assertEqual(ok, barrel_vectordb_gateway_rate:check_rate(TenantId, 50)),

        %% Now should have bucket info
        {ok, Info} = barrel_vectordb_gateway_rate:get_bucket_info(TenantId),

        ?assertEqual(TenantId, maps:get(tenant_id, Info)),
        ?assert(maps:get(tokens, Info) >= 48), % 50 - 1 for the request
        ?assertEqual(50, maps:get(max_tokens, Info)),
        ?assert(maps:get(rate_per_second, Info) > 0),
        ?assert(maps:get(last_refill, Info) > 0)
    end.

test_reset_bucket(_Pid) ->
    fun() ->
        TenantId = <<"reset_tenant">>,

        %% Create a bucket
        ?assertEqual(ok, barrel_vectordb_gateway_rate:check_rate(TenantId, 100)),

        %% Verify it exists
        ?assertMatch({ok, _}, barrel_vectordb_gateway_rate:get_bucket_info(TenantId)),

        %% Reset it
        ?assertEqual(ok, barrel_vectordb_gateway_rate:reset_bucket(TenantId)),

        %% Verify it's gone
        ?assertEqual({error, not_found}, barrel_vectordb_gateway_rate:get_bucket_info(TenantId))
    end.

test_rate_limiting(_Pid) ->
    fun() ->
        TenantId = <<"rate_limited_tenant">>,
        RpmLimit = 5,  % Very low limit for testing

        %% Exhaust all tokens
        lists:foreach(fun(_) ->
            barrel_vectordb_gateway_rate:check_rate(TenantId, RpmLimit)
        end, lists:seq(1, RpmLimit)),

        %% Next request should be rate limited
        ?assertEqual({error, rate_limited}, barrel_vectordb_gateway_rate:check_rate(TenantId, RpmLimit)),
        ?assertEqual({error, rate_limited}, barrel_vectordb_gateway_rate:check_rate(TenantId, RpmLimit))
    end.

test_token_refill(_Pid) ->
    fun() ->
        TenantId = <<"refill_tenant">>,
        RpmLimit = 60,  % 1 token per second

        %% Use all tokens
        lists:foreach(fun(_) ->
            barrel_vectordb_gateway_rate:check_rate(TenantId, RpmLimit)
        end, lists:seq(1, RpmLimit)),

        %% Should be rate limited
        ?assertEqual({error, rate_limited}, barrel_vectordb_gateway_rate:check_rate(TenantId, RpmLimit)),

        %% Wait a bit for refill (1 token per second at 60 RPM)
        timer:sleep(1100),

        %% Should have at least 1 token now
        ?assertEqual(ok, barrel_vectordb_gateway_rate:check_rate(TenantId, RpmLimit))
    end.

%%====================================================================
%% Isolation Tests
%%====================================================================

different_tenants_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_Pid) ->
        fun() ->
            Tenant1 = <<"isolated_tenant_1">>,
            Tenant2 = <<"isolated_tenant_2">>,

            %% Use all tokens for tenant 1 (very low limit)
            lists:foreach(fun(_) ->
                barrel_vectordb_gateway_rate:check_rate(Tenant1, 3)
            end, lists:seq(1, 3)),

            %% Tenant 1 should be rate limited
            ?assertEqual({error, rate_limited}, barrel_vectordb_gateway_rate:check_rate(Tenant1, 3)),

            %% Tenant 2 should still be fine
            ?assertEqual(ok, barrel_vectordb_gateway_rate:check_rate(Tenant2, 100)),
            ?assertEqual(ok, barrel_vectordb_gateway_rate:check_rate(Tenant2, 100))
        end
     end}.
