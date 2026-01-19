%%%-------------------------------------------------------------------
%%% @doc API key management for the gateway
%%%
%%% Handles API key creation, validation, and revocation.
%%% In standalone mode, writes directly to system RocksDB.
%%% In clustered mode, routes writes through Raft for consensus.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_gateway_keys).

%% API
-export([create_key/2, validate_key/1, revoke_key/1, list_keys/1]).
-export([get_tenant_limits/1]).

%% Key record accessors
-export([key_id/1, key_value/1, key_tenant_id/1, key_rpm_limit/1,
         key_max_storage_mb/1, key_max_vectors/1, key_max_collections/1]).

%% Types
-type api_key_record() :: barrel_vectordb_system_db:api_key().

-type create_opts() :: #{
    rpm_limit => pos_integer(),
    max_storage_mb => pos_integer() | unlimited,
    max_vectors => pos_integer() | unlimited,
    max_collections => pos_integer() | unlimited
}.

-export_type([api_key_record/0, create_opts/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new API key for a tenant.
-spec create_key(binary(), create_opts()) -> {ok, binary()} | {error, term()}.
create_key(TenantId, Opts) when is_binary(TenantId), is_map(Opts) ->
    %% Generate new API key
    ApiKey = generate_api_key(),

    %% Build key record with defaults from gateway config
    Defaults = get_default_limits(),
    Record = build_key_record(ApiKey, TenantId, Opts, Defaults),

    %% Route to appropriate backend
    case get_backend() of
        standalone ->
            %% Direct write to system DB
            case barrel_vectordb_system_db:put_key(Record) of
                ok ->
                    %% Initialize quota if not exists
                    ok = barrel_vectordb_system_db:init_quota(TenantId),
                    {ok, ApiKey};
                {error, Reason} ->
                    {error, Reason}
            end;
        clustered ->
            %% Route through Raft for consensus
            %% cluster_client:command returns the Result directly (unwrapped)
            case barrel_vectordb_cluster_client:command({gateway_create_key, Record}) of
                ok ->
                    %% Initialize quota through Raft
                    _ = barrel_vectordb_cluster_client:command({gateway_init_quota, TenantId}),
                    {ok, ApiKey};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @doc Validate an API key and return the associated tenant info.
-spec validate_key(binary()) -> {ok, api_key_record()} | {error, invalid | not_found}.
validate_key(ApiKey) when is_binary(ApiKey) ->
    %% Always read from local system DB (fast path)
    case barrel_vectordb_system_db:get_key(ApiKey) of
        {ok, KeyRecord} ->
            {ok, KeyRecord};
        {error, not_found} ->
            {error, invalid};
        {error, _} ->
            {error, invalid}
    end.

%% @doc Revoke an API key.
-spec revoke_key(binary()) -> ok | {error, term()}.
revoke_key(ApiKey) when is_binary(ApiKey) ->
    case get_backend() of
        standalone ->
            barrel_vectordb_system_db:delete_key(ApiKey);
        clustered ->
            barrel_vectordb_cluster_client:command({gateway_delete_key, ApiKey})
    end.

%% @doc List all API keys for a tenant (without exposing full key).
-spec list_keys(binary()) -> {ok, [map()]} | {error, term()}.
list_keys(TenantId) when is_binary(TenantId) ->
    case barrel_vectordb_system_db:list_keys_by_tenant(TenantId) of
        {ok, Keys} ->
            %% Return sanitized key info (hide full key)
            Sanitized = lists:map(fun(KeyRecord) ->
                ApiKey = key_id(KeyRecord),
                #{
                    key_prefix => binary:part(ApiKey, 0, min(12, byte_size(ApiKey))),
                    created_at => key_created_at(KeyRecord),
                    rpm_limit => key_rpm_limit(KeyRecord),
                    max_vectors => key_max_vectors(KeyRecord),
                    max_collections => key_max_collections(KeyRecord),
                    max_storage_mb => key_max_storage_mb(KeyRecord)
                }
            end, Keys),
            {ok, Sanitized};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get tenant limits from any of their API keys.
-spec get_tenant_limits(binary()) -> {ok, map()} | {error, not_found}.
get_tenant_limits(TenantId) when is_binary(TenantId) ->
    case barrel_vectordb_system_db:list_keys_by_tenant(TenantId) of
        {ok, [KeyRecord | _]} ->
            %% Return limits from first key (all keys for a tenant should have same limits)
            {ok, #{
                rpm_limit => key_rpm_limit(KeyRecord),
                max_vectors => key_max_vectors(KeyRecord),
                max_collections => key_max_collections(KeyRecord),
                max_storage_mb => key_max_storage_mb(KeyRecord)
            }};
        {ok, []} ->
            {error, not_found};
        {error, Reason} ->
            {error, Reason}
    end.

%%====================================================================
%% Key record accessors
%%====================================================================

%% Access the api_key record fields using element/2 to avoid exposing
%% the record definition outside the system_db module.

%% Record layout: {api_key, key, tenant_id, created_at, rpm_limit,
%%                 max_storage_mb, max_vectors, max_collections}

key_id(Record) -> element(2, Record).
key_value(Record) -> element(2, Record).  % Alias for key_id, used for audit logging
key_tenant_id(Record) -> element(3, Record).
key_created_at(Record) -> element(4, Record).
key_rpm_limit(Record) -> element(5, Record).
key_max_storage_mb(Record) -> element(6, Record).
key_max_vectors(Record) -> element(7, Record).
key_max_collections(Record) -> element(8, Record).

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Generate a new API key.
generate_api_key() ->
    Bytes = crypto:strong_rand_bytes(24),
    <<"bvdb_", (base64:encode(Bytes, #{padding => false}))/binary>>.

%% @private Get backend mode.
get_backend() ->
    case application:get_env(barrel_vectordb, cluster_enabled, false) of
        true -> clustered;
        false -> standalone
    end.

%% @private Get default limits from gateway configuration.
get_default_limits() ->
    case application:get_env(barrel_vectordb, gateway) of
        {ok, GatewayConfig} ->
            %% Merge default_quotas with default_rate_limit
            Quotas = maps:get(default_quotas, GatewayConfig, #{}),
            RpmLimit = maps:get(default_rate_limit, GatewayConfig, 100),
            maps:merge(default_quotas(), Quotas#{rpm_limit => RpmLimit});
        undefined ->
            default_quotas()
    end.

%% @private Default quotas if not configured.
default_quotas() ->
    #{
        rpm_limit => 100,
        max_storage_mb => 1024,
        max_vectors => 100000,
        max_collections => 10
    }.

%% @private Build the api_key record.
%% We use a tuple directly since we can't include the record definition.
build_key_record(ApiKey, TenantId, Opts, Defaults) ->
    {api_key,
     ApiKey,
     TenantId,
     erlang:system_time(second),
     maps:get(rpm_limit, Opts, maps:get(rpm_limit, Defaults)),
     maps:get(max_storage_mb, Opts, maps:get(max_storage_mb, Defaults)),
     maps:get(max_vectors, Opts, maps:get(max_vectors, Defaults)),
     maps:get(max_collections, Opts, maps:get(max_collections, Defaults))
    }.
