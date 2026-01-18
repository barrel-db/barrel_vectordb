%%%-------------------------------------------------------------------
%%% @doc System RocksDB manager for gateway data
%%%
%%% Manages a separate RocksDB instance for storing gateway data:
%%% - API keys (cf_keys)
%%% - API keys by tenant index (cf_keys_by_tenant)
%%% - Tenant quota tracking (cf_quotas)
%%%
%%% This database is local to each node. In clustered mode, writes are
%%% propagated via Raft and applied locally to each node's system DB.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_system_db).
-behaviour(gen_server).

%% API
-export([start_link/1]).
-export([put_key/1, get_key/1, delete_key/1, list_keys_by_tenant/1]).
-export([get_quota/1, update_quota/2, init_quota/1]).
-export([get_tenant_ids/0]).
%% Collection tracking for standalone mode
-export([add_collection/2, remove_collection/2, list_collections_by_tenant/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Column family names
-define(CF_DEFAULT, "default").
-define(CF_KEYS, "cf_keys").
-define(CF_KEYS_BY_TENANT, "cf_keys_by_tenant").
-define(CF_QUOTAS, "cf_quotas").

-record(state, {
    db :: rocksdb:db_handle(),
    cf_default :: rocksdb:cf_handle(),
    cf_keys :: rocksdb:cf_handle(),
    cf_keys_by_tenant :: rocksdb:cf_handle(),
    cf_quotas :: rocksdb:cf_handle()
}).

%% API key record
-record(api_key, {
    key :: binary(),              % bvdb_<base64-24-bytes>
    tenant_id :: binary(),
    created_at :: integer(),
    rpm_limit :: pos_integer(),   % requests per minute
    max_storage_mb :: pos_integer() | unlimited,
    max_vectors :: pos_integer() | unlimited,
    max_collections :: pos_integer() | unlimited
}).

%% Tenant quota record
-record(tenant_quota, {
    tenant_id :: binary(),
    storage_bytes :: non_neg_integer(),
    vector_count :: non_neg_integer(),
    collection_count :: non_neg_integer()
}).

-export_type([api_key/0, tenant_quota/0]).
-type api_key() :: #api_key{}.
-type tenant_quota() :: #tenant_quota{}.

%%====================================================================
%% API
%%====================================================================

%% @doc Start the system database manager.
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

%% @doc Store an API key.
-spec put_key(api_key()) -> ok | {error, term()}.
put_key(#api_key{} = Key) ->
    gen_server:call(?MODULE, {put_key, Key}).

%% @doc Retrieve an API key by key string.
-spec get_key(binary()) -> {ok, api_key()} | {error, not_found | term()}.
get_key(ApiKey) when is_binary(ApiKey) ->
    gen_server:call(?MODULE, {get_key, ApiKey}).

%% @doc Delete an API key.
-spec delete_key(binary()) -> ok | {error, term()}.
delete_key(ApiKey) when is_binary(ApiKey) ->
    gen_server:call(?MODULE, {delete_key, ApiKey}).

%% @doc List all API keys for a tenant.
-spec list_keys_by_tenant(binary()) -> {ok, [api_key()]} | {error, term()}.
list_keys_by_tenant(TenantId) when is_binary(TenantId) ->
    gen_server:call(?MODULE, {list_keys_by_tenant, TenantId}).

%% @doc Get tenant quota usage.
-spec get_quota(binary()) -> {ok, tenant_quota()} | {error, not_found | term()}.
get_quota(TenantId) when is_binary(TenantId) ->
    gen_server:call(?MODULE, {get_quota, TenantId}).

%% @doc Initialize quota for a tenant.
-spec init_quota(binary()) -> ok | {error, term()}.
init_quota(TenantId) when is_binary(TenantId) ->
    gen_server:call(?MODULE, {init_quota, TenantId}).

%% @doc Update tenant quota (increment/decrement counters).
%% Op can be:
%% - {incr_vectors, Count}
%% - {decr_vectors, Count}
%% - {incr_collections, Count}
%% - {decr_collections, Count}
%% - {incr_storage, Bytes}
%% - {decr_storage, Bytes}
-spec update_quota(binary(), term()) -> ok | {error, term()}.
update_quota(TenantId, Op) when is_binary(TenantId) ->
    gen_server:call(?MODULE, {update_quota, TenantId, Op}).

%% @doc Get all tenant IDs that have quotas.
-spec get_tenant_ids() -> {ok, [binary()]} | {error, term()}.
get_tenant_ids() ->
    gen_server:call(?MODULE, get_tenant_ids).

%% @doc Add a collection name for a tenant (standalone mode tracking).
-spec add_collection(binary(), binary()) -> ok | {error, term()}.
add_collection(TenantId, CollectionName) when is_binary(TenantId), is_binary(CollectionName) ->
    gen_server:call(?MODULE, {add_collection, TenantId, CollectionName}).

%% @doc Remove a collection name for a tenant (standalone mode tracking).
-spec remove_collection(binary(), binary()) -> ok | {error, term()}.
remove_collection(TenantId, CollectionName) when is_binary(TenantId), is_binary(CollectionName) ->
    gen_server:call(?MODULE, {remove_collection, TenantId, CollectionName}).

%% @doc List all collection names for a tenant (standalone mode tracking).
-spec list_collections_by_tenant(binary()) -> {ok, [binary()]} | {error, term()}.
list_collections_by_tenant(TenantId) when is_binary(TenantId) ->
    gen_server:call(?MODULE, {list_collections_by_tenant, TenantId}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    Path = maps:get(path, Opts, default_path()),
    ok = filelib:ensure_dir(Path ++ "/"),

    %% Define column families
    CfDefs = [
        {?CF_DEFAULT, []},
        {?CF_KEYS, []},
        {?CF_KEYS_BY_TENANT, []},
        {?CF_QUOTAS, []}
    ],

    %% Open or create database with column families
    DbOpts = [{create_if_missing, true}, {create_missing_column_families, true}],
    case rocksdb:open_with_cf(Path, DbOpts, CfDefs) of
        {ok, Db, CfHandles} ->
            %% Map column family handles
            [CfDefault, CfKeys, CfKeysByTenant, CfQuotas] = CfHandles,
            State = #state{
                db = Db,
                cf_default = CfDefault,
                cf_keys = CfKeys,
                cf_keys_by_tenant = CfKeysByTenant,
                cf_quotas = CfQuotas
            },
            {ok, State};
        {error, Reason} ->
            {stop, {db_open_failed, Reason}}
    end.

handle_call({put_key, #api_key{key = Key, tenant_id = TenantId} = ApiKey}, _From, State) ->
    #state{db = Db, cf_keys = CfKeys, cf_keys_by_tenant = CfKeysByTenant} = State,

    %% Serialize the key record
    Value = term_to_binary(ApiKey),

    %% Create batch for atomic write
    {ok, Batch} = rocksdb:batch(),
    ok = rocksdb:batch_put(Batch, CfKeys, Key, Value),

    %% Secondary index: {TenantId, Key} -> <<>>
    IndexKey = <<TenantId/binary, ":", Key/binary>>,
    ok = rocksdb:batch_put(Batch, CfKeysByTenant, IndexKey, <<>>),

    case rocksdb:write_batch(Db, Batch, [{sync, true}]) of
        ok ->
            rocksdb:release_batch(Batch),
            {reply, ok, State};
        {error, Reason} ->
            rocksdb:release_batch(Batch),
            {reply, {error, Reason}, State}
    end;

handle_call({get_key, ApiKey}, _From, State) ->
    #state{db = Db, cf_keys = CfKeys} = State,

    case rocksdb:get(Db, CfKeys, ApiKey, []) of
        {ok, Value} ->
            KeyRecord = binary_to_term(Value),
            {reply, {ok, KeyRecord}, State};
        not_found ->
            {reply, {error, not_found}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({delete_key, ApiKey}, _From, State) ->
    #state{db = Db, cf_keys = CfKeys, cf_keys_by_tenant = CfKeysByTenant} = State,

    %% First get the key to find the tenant ID for index cleanup
    case rocksdb:get(Db, CfKeys, ApiKey, []) of
        {ok, Value} ->
            #api_key{tenant_id = TenantId} = binary_to_term(Value),

            %% Create batch for atomic delete
            {ok, Batch} = rocksdb:batch(),
            ok = rocksdb:batch_delete(Batch, CfKeys, ApiKey),

            %% Remove secondary index
            IndexKey = <<TenantId/binary, ":", ApiKey/binary>>,
            ok = rocksdb:batch_delete(Batch, CfKeysByTenant, IndexKey),

            case rocksdb:write_batch(Db, Batch, [{sync, true}]) of
                ok ->
                    rocksdb:release_batch(Batch),
                    {reply, ok, State};
                {error, Reason} ->
                    rocksdb:release_batch(Batch),
                    {reply, {error, Reason}, State}
            end;
        not_found ->
            {reply, {error, not_found}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({list_keys_by_tenant, TenantId}, _From, State) ->
    #state{db = Db, cf_keys = CfKeys, cf_keys_by_tenant = CfKeysByTenant} = State,

    %% Scan the secondary index for this tenant
    Prefix = <<TenantId/binary, ":">>,
    PrefixLen = byte_size(Prefix),

    {ok, Iter} = rocksdb:iterator(Db, CfKeysByTenant, []),
    try
        Keys = collect_keys_for_tenant(Iter, rocksdb:iterator_move(Iter, {seek, Prefix}), Prefix, PrefixLen, Db, CfKeys, []),
        {reply, {ok, Keys}, State}
    after
        rocksdb:iterator_close(Iter)
    end;

handle_call({get_quota, TenantId}, _From, State) ->
    #state{db = Db, cf_quotas = CfQuotas} = State,

    case rocksdb:get(Db, CfQuotas, TenantId, []) of
        {ok, Value} ->
            Quota = binary_to_term(Value),
            {reply, {ok, Quota}, State};
        not_found ->
            {reply, {error, not_found}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({init_quota, TenantId}, _From, State) ->
    #state{db = Db, cf_quotas = CfQuotas} = State,

    %% Only initialize if not exists
    case rocksdb:get(Db, CfQuotas, TenantId, []) of
        not_found ->
            Quota = #tenant_quota{
                tenant_id = TenantId,
                storage_bytes = 0,
                vector_count = 0,
                collection_count = 0
            },
            Value = term_to_binary(Quota),
            case rocksdb:put(Db, CfQuotas, TenantId, Value, [{sync, true}]) of
                ok -> {reply, ok, State};
                {error, Reason} -> {reply, {error, Reason}, State}
            end;
        {ok, _} ->
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({update_quota, TenantId, Op}, _From, State) ->
    #state{db = Db, cf_quotas = CfQuotas} = State,

    %% Get current quota or initialize
    CurrentQuota = case rocksdb:get(Db, CfQuotas, TenantId, []) of
        {ok, ExistingValue} ->
            binary_to_term(ExistingValue);
        not_found ->
            #tenant_quota{
                tenant_id = TenantId,
                storage_bytes = 0,
                vector_count = 0,
                collection_count = 0
            }
    end,

    %% Apply operation
    NewQuota = apply_quota_op(CurrentQuota, Op),
    NewValue = term_to_binary(NewQuota),

    case rocksdb:put(Db, CfQuotas, TenantId, NewValue, [{sync, true}]) of
        ok -> {reply, ok, State};
        {error, Reason} -> {reply, {error, Reason}, State}
    end;

handle_call(get_tenant_ids, _From, State) ->
    #state{db = Db, cf_quotas = CfQuotas} = State,

    {ok, Iter} = rocksdb:iterator(Db, CfQuotas, []),
    try
        TenantIds = collect_tenant_ids(Iter, rocksdb:iterator_move(Iter, first), []),
        {reply, {ok, TenantIds}, State}
    after
        rocksdb:iterator_close(Iter)
    end;

%% Collection tracking for standalone mode
handle_call({add_collection, TenantId, CollectionName}, _From, State) ->
    #state{db = Db, cf_default = CfDefault} = State,
    %% Key format: coll:{tenant_id}:{collection_name}
    Key = <<"coll:", TenantId/binary, ":", CollectionName/binary>>,
    case rocksdb:put(Db, CfDefault, Key, <<>>, [{sync, true}]) of
        ok -> {reply, ok, State};
        {error, Reason} -> {reply, {error, Reason}, State}
    end;

handle_call({remove_collection, TenantId, CollectionName}, _From, State) ->
    #state{db = Db, cf_default = CfDefault} = State,
    Key = <<"coll:", TenantId/binary, ":", CollectionName/binary>>,
    case rocksdb:delete(Db, CfDefault, Key, [{sync, true}]) of
        ok -> {reply, ok, State};
        {error, Reason} -> {reply, {error, Reason}, State}
    end;

handle_call({list_collections_by_tenant, TenantId}, _From, State) ->
    #state{db = Db, cf_default = CfDefault} = State,
    %% Scan keys with prefix coll:{tenant_id}:
    Prefix = <<"coll:", TenantId/binary, ":">>,
    PrefixLen = byte_size(Prefix),

    {ok, Iter} = rocksdb:iterator(Db, CfDefault, []),
    try
        Collections = collect_collections(Iter, rocksdb:iterator_move(Iter, {seek, Prefix}), Prefix, PrefixLen, []),
        {reply, {ok, Collections}, State}
    after
        rocksdb:iterator_close(Iter)
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{db = Db, cf_keys = CfKeys, cf_keys_by_tenant = CfKeysByTenant, cf_quotas = CfQuotas, cf_default = CfDefault}) ->
    %% Close column family handles first
    lists:foreach(fun(Cf) ->
        catch rocksdb:destroy_column_family(Cf)
    end, [CfKeys, CfKeysByTenant, CfQuotas, CfDefault]),
    %% Close database
    _ = rocksdb:close(Db),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

default_path() ->
    case application:get_env(barrel_vectordb, gateway) of
        {ok, #{system_db_path := Path}} -> Path;
        _ ->
            case application:get_env(barrel_vectordb, path) of
                {ok, BasePath} -> BasePath ++ "/system";
                undefined -> "priv/barrel_vectordb_system"
            end
    end.

collect_keys_for_tenant(Iter, {ok, IndexKey, _Value}, Prefix, PrefixLen, Db, CfKeys, Acc) ->
    case IndexKey of
        <<Prefix:PrefixLen/binary, ApiKey/binary>> ->
            %% Get the full key record
            NewAcc = case rocksdb:get(Db, CfKeys, ApiKey, []) of
                {ok, KeyValue} ->
                    [binary_to_term(KeyValue) | Acc];
                _ ->
                    Acc
            end,
            collect_keys_for_tenant(Iter, rocksdb:iterator_move(Iter, next), Prefix, PrefixLen, Db, CfKeys, NewAcc);
        _ ->
            %% Past prefix range
            lists:reverse(Acc)
    end;
collect_keys_for_tenant(_Iter, {error, _}, _Prefix, _PrefixLen, _Db, _CfKeys, Acc) ->
    lists:reverse(Acc).

collect_tenant_ids(Iter, {ok, TenantId, _Value}, Acc) ->
    collect_tenant_ids(Iter, rocksdb:iterator_move(Iter, next), [TenantId | Acc]);
collect_tenant_ids(_Iter, {error, _}, Acc) ->
    lists:reverse(Acc).

collect_collections(Iter, {ok, Key, _Value}, Prefix, PrefixLen, Acc) ->
    case Key of
        <<Prefix:PrefixLen/binary, CollectionName/binary>> ->
            collect_collections(Iter, rocksdb:iterator_move(Iter, next), Prefix, PrefixLen, [CollectionName | Acc]);
        _ ->
            %% Past prefix range
            lists:reverse(Acc)
    end;
collect_collections(_Iter, {error, _}, _Prefix, _PrefixLen, Acc) ->
    lists:reverse(Acc).

apply_quota_op(Quota, {incr_vectors, Count}) ->
    Quota#tenant_quota{vector_count = Quota#tenant_quota.vector_count + Count};
apply_quota_op(Quota, {decr_vectors, Count}) ->
    NewCount = max(0, Quota#tenant_quota.vector_count - Count),
    Quota#tenant_quota{vector_count = NewCount};
apply_quota_op(Quota, {incr_collections, Count}) ->
    Quota#tenant_quota{collection_count = Quota#tenant_quota.collection_count + Count};
apply_quota_op(Quota, {decr_collections, Count}) ->
    NewCount = max(0, Quota#tenant_quota.collection_count - Count),
    Quota#tenant_quota{collection_count = NewCount};
apply_quota_op(Quota, {incr_storage, Bytes}) ->
    Quota#tenant_quota{storage_bytes = Quota#tenant_quota.storage_bytes + Bytes};
apply_quota_op(Quota, {decr_storage, Bytes}) ->
    NewBytes = max(0, Quota#tenant_quota.storage_bytes - Bytes),
    Quota#tenant_quota{storage_bytes = NewBytes}.
