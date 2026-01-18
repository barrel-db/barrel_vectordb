%%%-------------------------------------------------------------------
%%% @doc Quota management for the gateway
%%%
%%% Handles quota checking and enforcement for tenants.
%%% Tracks vectors, collections, and storage usage.
%%% In standalone mode, updates system RocksDB directly.
%%% In clustered mode, routes updates through Raft.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_gateway_quotas).

%% API
-export([check_can_add_vectors/2, check_can_create_collection/1]).
-export([increment_vectors/2, decrement_vectors/2]).
-export([increment_collections/1, decrement_collections/1]).
-export([increment_storage/2, decrement_storage/2]).
-export([get_usage/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Check if tenant can add more vectors.
-spec check_can_add_vectors(binary(), pos_integer()) -> ok | {error, quota_exceeded}.
check_can_add_vectors(TenantId, Count) when is_binary(TenantId), is_integer(Count), Count > 0 ->
    case get_tenant_limit(TenantId, max_vectors) of
        unlimited ->
            ok;
        MaxVectors ->
            CurrentCount = get_current_vector_count(TenantId),
            case CurrentCount + Count =< MaxVectors of
                true -> ok;
                false -> {error, quota_exceeded}
            end
    end.

%% @doc Check if tenant can create another collection.
-spec check_can_create_collection(binary()) -> ok | {error, quota_exceeded}.
check_can_create_collection(TenantId) when is_binary(TenantId) ->
    case get_tenant_limit(TenantId, max_collections) of
        unlimited ->
            ok;
        MaxCollections ->
            CurrentCount = get_current_collection_count(TenantId),
            case CurrentCount + 1 =< MaxCollections of
                true -> ok;
                false -> {error, quota_exceeded}
            end
    end.

%% @doc Increment vector count after successful add.
-spec increment_vectors(binary(), pos_integer()) -> ok | {error, term()}.
increment_vectors(TenantId, Count) when is_binary(TenantId), is_integer(Count), Count > 0 ->
    update_quota(TenantId, {incr_vectors, Count}).

%% @doc Decrement vector count after delete.
-spec decrement_vectors(binary(), pos_integer()) -> ok | {error, term()}.
decrement_vectors(TenantId, Count) when is_binary(TenantId), is_integer(Count), Count > 0 ->
    update_quota(TenantId, {decr_vectors, Count}).

%% @doc Increment collection count after successful create.
-spec increment_collections(binary()) -> ok | {error, term()}.
increment_collections(TenantId) when is_binary(TenantId) ->
    update_quota(TenantId, {incr_collections, 1}).

%% @doc Decrement collection count after delete.
-spec decrement_collections(binary()) -> ok | {error, term()}.
decrement_collections(TenantId) when is_binary(TenantId) ->
    update_quota(TenantId, {decr_collections, 1}).

%% @doc Increment storage bytes.
-spec increment_storage(binary(), non_neg_integer()) -> ok | {error, term()}.
increment_storage(TenantId, Bytes) when is_binary(TenantId), is_integer(Bytes), Bytes >= 0 ->
    update_quota(TenantId, {incr_storage, Bytes}).

%% @doc Decrement storage bytes.
-spec decrement_storage(binary(), non_neg_integer()) -> ok | {error, term()}.
decrement_storage(TenantId, Bytes) when is_binary(TenantId), is_integer(Bytes), Bytes >= 0 ->
    update_quota(TenantId, {decr_storage, Bytes}).

%% @doc Get current usage for a tenant.
-spec get_usage(binary()) -> {ok, map()} | {error, term()}.
get_usage(TenantId) when is_binary(TenantId) ->
    case barrel_vectordb_system_db:get_quota(TenantId) of
        {ok, Quota} ->
            %% Convert record to map
            {ok, #{
                tenant_id => element(2, Quota),
                storage_bytes => element(3, Quota),
                vector_count => element(4, Quota),
                collection_count => element(5, Quota)
            }};
        {error, not_found} ->
            {ok, #{
                tenant_id => TenantId,
                storage_bytes => 0,
                vector_count => 0,
                collection_count => 0
            }};
        {error, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Get tenant limit for a specific field.
get_tenant_limit(TenantId, Field) ->
    case barrel_vectordb_gateway_keys:get_tenant_limits(TenantId) of
        {ok, Limits} ->
            maps:get(Field, Limits, unlimited);
        {error, _} ->
            %% Fall back to defaults from config
            Defaults = get_default_limits(),
            maps:get(Field, Defaults, unlimited)
    end.

%% @private Get default limits from configuration.
get_default_limits() ->
    case application:get_env(barrel_vectordb, gateway) of
        {ok, GatewayConfig} ->
            maps:get(default_quotas, GatewayConfig, default_quotas());
        undefined ->
            default_quotas()
    end.

%% @private Default quotas if not configured.
default_quotas() ->
    #{
        max_storage_mb => 1024,
        max_vectors => 100000,
        max_collections => 10
    }.

%% @private Get current vector count for tenant.
get_current_vector_count(TenantId) ->
    case barrel_vectordb_system_db:get_quota(TenantId) of
        {ok, Quota} -> element(4, Quota);
        {error, not_found} -> 0
    end.

%% @private Get current collection count for tenant.
get_current_collection_count(TenantId) ->
    case barrel_vectordb_system_db:get_quota(TenantId) of
        {ok, Quota} -> element(5, Quota);
        {error, not_found} -> 0
    end.

%% @private Update quota through appropriate backend.
update_quota(TenantId, Op) ->
    case get_backend() of
        standalone ->
            barrel_vectordb_system_db:update_quota(TenantId, Op);
        clustered ->
            barrel_vectordb_cluster_client:command({gateway_update_quota, TenantId, Op})
    end.

%% @private Get backend mode.
get_backend() ->
    case application:get_env(barrel_vectordb, cluster_enabled, false) of
        true -> clustered;
        false -> standalone
    end.
