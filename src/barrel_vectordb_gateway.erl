%%%-------------------------------------------------------------------
%%% @doc Multi-tenant HTTP gateway for barrel_vectordb
%%%
%%% Provides a REST API with:
%%% - Tenant isolation via API key authentication
%%% - Transparent collection name prefixing (hash + tenant + name)
%%% - Rate limiting per tenant
%%% - Quota enforcement
%%% - Enterprise hooks for SSO, RBAC, and audit logging
%%%
%%% Collection naming: {4-char-hash}_{tenant}_{collection}
%%% Example: a3f2_acme_documents
%%%   - a3f2: 4-char hex hash of tenant ID (RocksDB prefix locality)
%%%   - acme: tenant ID
%%%   - documents: collection name
%%%
%%% Enterprise Integration:
%%% When barrel_vectordb_gateway_enterprise module is loaded (from
%%% barrel_enterprise package), the gateway automatically calls:
%%% - authenticate/2: SSO/OIDC authentication (falls back to API key)
%%% - authorize/3: RBAC permission checks
%%% - audit/3: Audit logging of all requests
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_gateway).

-export([routes/0]).
-export([init/2]).

%% Utility exports for testing
-export([tenant_hash/1, prefix_collection/2, strip_prefix/2]).

%% Configuration
-define(API_KEY_HEADER, <<"x-api-key">>).
-define(ENTERPRISE_MOD, barrel_vectordb_gateway_enterprise).

%% Suppress dialyzer warnings for optional enterprise module functions
-dialyzer({nowarn_function, [enterprise_authenticate/2,
                             enterprise_authorize/3,
                             enterprise_audit/3]}).

%%====================================================================
%% API
%%====================================================================

%% @doc Return cowboy routes for the gateway.
-spec routes() -> list().
routes() ->
    [
        %% Collections
        {"/v1/collections", ?MODULE, #{action => list_collections}},
        {"/v1/collections/:collection", ?MODULE, #{action => collection}},

        %% Documents
        {"/v1/collections/:collection/documents", ?MODULE, #{action => documents}},
        {"/v1/collections/:collection/documents/:doc_id", ?MODULE, #{action => document}},

        %% Search
        {"/v1/collections/:collection/search", ?MODULE, #{action => search}},

        %% Admin endpoints (master key required)
        {"/admin/tenants", ?MODULE, #{action => admin_tenants}},
        {"/admin/tenants/:tenant_id/keys", ?MODULE, #{action => admin_keys}},
        {"/admin/tenants/:tenant_id/usage", ?MODULE, #{action => admin_usage}}
    ].

%%====================================================================
%% Cowboy Handler
%%====================================================================

init(Req0, #{action := Action} = State) ->
    %% Capture request metadata for audit logging
    StartTime = erlang:system_time(microsecond),
    Method = cowboy_req:method(Req0),
    AuditCtx = build_audit_context(Req0, Action, StartTime),

    %% Process the request
    {Response, FinalCtx} = case is_admin_action(Action) of
        true ->
            process_admin_request(Req0, Action, Method, AuditCtx);
        false ->
            process_tenant_request(Req0, Action, Method, AuditCtx)
    end,

    %% Enterprise audit hook
    DurationUs = erlang:system_time(microsecond) - StartTime,
    _ = enterprise_audit(FinalCtx, Response, DurationUs),

    {ok, Response, State}.

%% @private Process admin requests (master key required)
process_admin_request(Req, Action, Method, AuditCtx) ->
    case authenticate_admin(Req) of
        ok ->
            Ctx = AuditCtx#{tenant_id => admin, auth_type => master_key},
            Response = handle_admin(Method, Action, Req),
            {Response, Ctx};
        {error, Reason} ->
            Response = error_response(401, Reason, Req),
            {Response, AuditCtx#{error_code => Reason}}
    end.

%% @private Process tenant requests with enterprise hooks
process_tenant_request(Req, Action, Method, AuditCtx) ->
    %% Step 1: Authentication
    %% Try enterprise auth first (SSO/OIDC), fall back to API key
    case enterprise_authenticate(Req, Action) of
        {handled, {ok, TenantId, Identity}} ->
            %% Enterprise SSO authentication succeeded
            Ctx = AuditCtx#{
                tenant_id => TenantId,
                identity => Identity,
                auth_type => enterprise,
                api_key_prefix => undefined
            },
            process_authorized_request(Req, Action, Method, TenantId, Identity, Ctx);
        {handled, {error, Reason}} ->
            %% Enterprise auth failed
            Response = error_response(401, format_error(Reason), Req),
            {Response, AuditCtx#{error_code => Reason}};
        passthrough ->
            %% No enterprise auth, use API key
            case authenticate(Req) of
                {ok, TenantId, KeyRecord} ->
                    ApiKeyPrefix = get_key_prefix(KeyRecord),
                    Ctx = AuditCtx#{
                        tenant_id => TenantId,
                        identity => undefined,
                        auth_type => api_key,
                        api_key_prefix => ApiKeyPrefix
                    },
                    process_authorized_request(Req, Action, Method, TenantId, undefined, Ctx);
                {error, Reason} ->
                    Response = error_response(401, Reason, Req),
                    {Response, AuditCtx#{error_code => Reason}}
            end
    end.

%% @private Process request after authentication, with optional RBAC
process_authorized_request(Req, Action, Method, TenantId, Identity, AuditCtx) ->
    %% Step 2: Authorization (RBAC)
    case enterprise_authorize(Identity, Action, AuditCtx) of
        {handled, {deny, Reason}} ->
            Response = error_response(403, Reason, Req),
            {Response, AuditCtx#{error_code => <<"access_denied">>}};
        {handled, {allow, _}} ->
            %% RBAC allowed
            process_rate_limited_request(Req, Action, Method, TenantId, AuditCtx);
        passthrough ->
            %% No RBAC configured, proceed
            process_rate_limited_request(Req, Action, Method, TenantId, AuditCtx)
    end.

%% @private Apply rate limiting and execute request
process_rate_limited_request(Req, Action, Method, TenantId, AuditCtx) ->
    case barrel_vectordb_gateway_rate:check_rate(TenantId) of
        ok ->
            %% Build a minimal KeyRecord-like structure for handle_action
            KeyRecord = undefined, % handle_action only uses KeyRecord for limits, which we don't need here
            Response = handle_action(Method, Action, TenantId, KeyRecord, Req),
            {Response, AuditCtx};
        {error, rate_limited} ->
            Response = error_response(429, <<"rate_limit_exceeded">>, Req),
            {Response, AuditCtx#{error_code => <<"rate_limited">>}}
    end.

%%====================================================================
%% Authentication
%%====================================================================

authenticate(Req) ->
    case cowboy_req:header(?API_KEY_HEADER, Req) of
        undefined ->
            {error, <<"missing_api_key">>};
        ApiKey ->
            case barrel_vectordb_gateway_keys:validate_key(ApiKey) of
                {ok, KeyRecord} ->
                    TenantId = barrel_vectordb_gateway_keys:key_tenant_id(KeyRecord),
                    {ok, TenantId, KeyRecord};
                {error, _} ->
                    {error, <<"invalid_api_key">>}
            end
    end.

authenticate_admin(Req) ->
    case cowboy_req:header(?API_KEY_HEADER, Req) of
        undefined ->
            {error, <<"missing_api_key">>};
        ApiKey ->
            case get_master_key() of
                ApiKey -> ok;
                _ -> {error, <<"invalid_master_key">>}
            end
    end.

is_admin_action(admin_tenants) -> true;
is_admin_action(admin_keys) -> true;
is_admin_action(admin_usage) -> true;
is_admin_action(_) -> false.

%%====================================================================
%% Collection Handlers
%%====================================================================

handle_action(<<"GET">>, list_collections, TenantId, _KeyRecord, Req) ->
    Collections = list_tenant_collections(TenantId),
    json_response(200, #{collections => Collections}, Req);

handle_action(<<"POST">>, list_collections, TenantId, _KeyRecord, Req) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    #{<<"name">> := Name} = Params = json:decode(Body),

    %% Check quota
    case barrel_vectordb_gateway_quotas:check_can_create_collection(TenantId) of
        ok ->
            FullName = prefix_collection(TenantId, Name),
            Config = build_collection_config(Params),

            case do_create_collection(FullName, Config) of
                ok ->
                    %% Track collection in system DB (for standalone mode)
                    _ = barrel_vectordb_system_db:add_collection(TenantId, FullName),
                    %% Update quota
                    ok = barrel_vectordb_gateway_quotas:increment_collections(TenantId),
                    json_response(201, #{name => Name, status => <<"created">>}, Req1);
                {error, already_exists} ->
                    error_response(409, <<"collection_already_exists">>, Req1);
                {error, Reason} ->
                    error_response(500, format_error(Reason), Req1)
            end;
        {error, quota_exceeded} ->
            error_response(403, <<"collection_quota_exceeded">>, Req1)
    end;

handle_action(<<"DELETE">>, collection, TenantId, _KeyRecord, Req) ->
    Collection = cowboy_req:binding(collection, Req),
    FullName = prefix_collection(TenantId, Collection),

    case do_delete_collection(FullName) of
        ok ->
            %% Remove collection from tracking (for standalone mode)
            _ = barrel_vectordb_system_db:remove_collection(TenantId, FullName),
            ok = barrel_vectordb_gateway_quotas:decrement_collections(TenantId),
            json_response(200, #{status => <<"deleted">>}, Req);
        {error, not_found} ->
            error_response(404, <<"collection_not_found">>, Req);
        {error, Reason} ->
            error_response(500, format_error(Reason), Req)
    end;

handle_action(<<"GET">>, collection, TenantId, _KeyRecord, Req) ->
    Collection = cowboy_req:binding(collection, Req),
    FullName = prefix_collection(TenantId, Collection),

    case do_get_collection(FullName) of
        {ok, Info} ->
            json_response(200, Info#{name => Collection}, Req);
        {error, not_found} ->
            error_response(404, <<"collection_not_found">>, Req)
    end;

%%====================================================================
%% Document Handlers
%%====================================================================

handle_action(<<"POST">>, documents, TenantId, _KeyRecord, Req) ->
    Collection = cowboy_req:binding(collection, Req),
    FullName = prefix_collection(TenantId, Collection),

    {ok, Body, Req1} = cowboy_req:read_body(Req),
    Params = json:decode(Body),

    case Params of
        #{<<"documents">> := Docs} when is_list(Docs) ->
            %% Batch insert - check quota first
            DocCount = length(Docs),
            case barrel_vectordb_gateway_quotas:check_can_add_vectors(TenantId, DocCount) of
                ok ->
                    Results = lists:map(fun(Doc) ->
                        insert_document(FullName, Doc)
                    end, Docs),
                    SuccessCount = length([R || R <- Results, element(1, R) =:= ok]),
                    ok = barrel_vectordb_gateway_quotas:increment_vectors(TenantId, SuccessCount),
                    json_response(200, #{results => format_insert_results(Results)}, Req1);
                {error, quota_exceeded} ->
                    error_response(403, <<"vector_quota_exceeded">>, Req1)
            end;
        Doc when is_map(Doc) ->
            %% Single insert
            case barrel_vectordb_gateway_quotas:check_can_add_vectors(TenantId, 1) of
                ok ->
                    case insert_document(FullName, Doc) of
                        {ok, Id} ->
                            ok = barrel_vectordb_gateway_quotas:increment_vectors(TenantId, 1),
                            json_response(201, #{id => Id, status => <<"created">>}, Req1);
                        {error, Reason} ->
                            error_response(400, format_error(Reason), Req1)
                    end;
                {error, quota_exceeded} ->
                    error_response(403, <<"vector_quota_exceeded">>, Req1)
            end
    end;

handle_action(<<"GET">>, document, TenantId, _KeyRecord, Req) ->
    Collection = cowboy_req:binding(collection, Req),
    DocId = cowboy_req:binding(doc_id, Req),
    FullName = prefix_collection(TenantId, Collection),

    case do_get(FullName, DocId) of
        {ok, Doc} ->
            json_response(200, Doc, Req);
        {error, not_found} ->
            error_response(404, <<"document_not_found">>, Req)
    end;

handle_action(<<"DELETE">>, document, TenantId, _KeyRecord, Req) ->
    Collection = cowboy_req:binding(collection, Req),
    DocId = cowboy_req:binding(doc_id, Req),
    FullName = prefix_collection(TenantId, Collection),

    case do_delete(FullName, DocId) of
        ok ->
            ok = barrel_vectordb_gateway_quotas:decrement_vectors(TenantId, 1),
            json_response(200, #{status => <<"deleted">>}, Req);
        {error, not_found} ->
            error_response(404, <<"document_not_found">>, Req)
    end;

%%====================================================================
%% Search Handler
%%====================================================================

handle_action(<<"POST">>, search, TenantId, _KeyRecord, Req) ->
    Collection = cowboy_req:binding(collection, Req),
    FullName = prefix_collection(TenantId, Collection),

    {ok, Body, Req1} = cowboy_req:read_body(Req),
    Params = json:decode(Body),

    Query = maps:get(<<"query">>, Params, undefined),
    Vector = maps:get(<<"vector">>, Params, undefined),
    K = maps:get(<<"k">>, Params, 10),
    Filter = maps:get(<<"filter">>, Params, undefined),

    Opts = #{k => K},
    Opts1 = maybe_add_filter(Opts, Filter),

    Result = case {Query, Vector} of
        {undefined, V} when is_list(V) ->
            do_search_vector(FullName, V, Opts1);
        {Q, undefined} when is_binary(Q) ->
            do_search(FullName, Q, Opts1);
        _ ->
            {error, <<"must provide query or vector">>}
    end,

    case Result of
        {ok, Hits} ->
            json_response(200, #{hits => format_hits(Hits)}, Req1);
        {error, Reason} ->
            error_response(400, format_error(Reason), Req1)
    end;

handle_action(_, _, _, _, Req) ->
    error_response(405, <<"method_not_allowed">>, Req).

%%====================================================================
%% Admin Handlers
%%====================================================================

handle_admin(<<"POST">>, admin_tenants, Req) ->
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    #{<<"tenant_id">> := TenantId} = Params = json:decode(Body),

    %% Create initial API key for tenant
    Opts = maps:with([<<"rpm_limit">>, <<"max_storage_mb">>, <<"max_vectors">>, <<"max_collections">>], Params),
    OptsAtom = maps:fold(fun(K, V, Acc) ->
        Acc#{binary_to_atom(K) => V}
    end, #{}, Opts),

    case barrel_vectordb_gateway_keys:create_key(TenantId, OptsAtom) of
        {ok, ApiKey} ->
            json_response(201, #{
                tenant_id => TenantId,
                api_key => ApiKey,
                status => <<"created">>
            }, Req1);
        {error, Reason} ->
            error_response(500, format_error(Reason), Req1)
    end;

handle_admin(<<"POST">>, admin_keys, Req) ->
    TenantId = cowboy_req:binding(tenant_id, Req),
    {ok, Body, Req1} = cowboy_req:read_body(Req),
    Params = json:decode(Body),

    Opts = maps:with([<<"rpm_limit">>, <<"max_storage_mb">>, <<"max_vectors">>, <<"max_collections">>], Params),
    OptsAtom = maps:fold(fun(K, V, Acc) ->
        Acc#{binary_to_atom(K) => V}
    end, #{}, Opts),

    case barrel_vectordb_gateway_keys:create_key(TenantId, OptsAtom) of
        {ok, ApiKey} ->
            json_response(201, #{api_key => ApiKey, tenant_id => TenantId}, Req1);
        {error, Reason} ->
            error_response(500, format_error(Reason), Req1)
    end;

handle_admin(<<"GET">>, admin_keys, Req) ->
    TenantId = cowboy_req:binding(tenant_id, Req),

    case barrel_vectordb_gateway_keys:list_keys(TenantId) of
        {ok, Keys} ->
            json_response(200, #{keys => Keys}, Req);
        {error, Reason} ->
            error_response(500, format_error(Reason), Req)
    end;

handle_admin(<<"DELETE">>, admin_keys, Req) ->
    %% Revoke a specific key - key should be in query string or body
    case cowboy_req:match_qs([{key, [], undefined}], Req) of
        #{key := undefined} ->
            error_response(400, <<"missing_key_parameter">>, Req);
        #{key := ApiKey} ->
            case barrel_vectordb_gateway_keys:revoke_key(ApiKey) of
                ok ->
                    json_response(200, #{status => <<"revoked">>}, Req);
                {error, not_found} ->
                    error_response(404, <<"key_not_found">>, Req);
                {error, Reason} ->
                    error_response(500, format_error(Reason), Req)
            end
    end;

handle_admin(<<"GET">>, admin_usage, Req) ->
    TenantId = cowboy_req:binding(tenant_id, Req),

    case barrel_vectordb_gateway_quotas:get_usage(TenantId) of
        {ok, Usage} ->
            %% Also get limits
            Limits = case barrel_vectordb_gateway_keys:get_tenant_limits(TenantId) of
                {ok, L} -> L;
                {error, _} -> #{}
            end,
            json_response(200, #{usage => Usage, limits => Limits}, Req);
        {error, Reason} ->
            error_response(500, format_error(Reason), Req)
    end;

handle_admin(_, _, Req) ->
    error_response(405, <<"method_not_allowed">>, Req).

%%====================================================================
%% Collection Name Prefixing
%%====================================================================

%% @doc Generate 4-char hex hash of tenant ID for RocksDB prefix locality.
-spec tenant_hash(binary()) -> binary().
tenant_hash(TenantId) when is_binary(TenantId) ->
    <<H:16, _/binary>> = crypto:hash(md5, TenantId),
    iolist_to_binary(io_lib:format("~4.16.0b", [H])).

%% @doc Prefix collection name with hash and tenant ID.
%% Format: {hash}_{tenant}_{collection}
-spec prefix_collection(binary(), binary()) -> binary().
prefix_collection(TenantId, Collection) when is_binary(TenantId), is_binary(Collection) ->
    Hash = tenant_hash(TenantId),
    <<Hash/binary, "_", TenantId/binary, "_", Collection/binary>>.

%% @doc Strip prefix from collection name, returning just the collection part.
-spec strip_prefix(binary(), binary()) -> binary().
strip_prefix(FullName, TenantId) when is_binary(FullName), is_binary(TenantId) ->
    Hash = tenant_hash(TenantId),
    Prefix = <<Hash/binary, "_", TenantId/binary, "_">>,
    PrefixLen = byte_size(Prefix),
    case FullName of
        <<Prefix:PrefixLen/binary, Rest/binary>> -> Rest;
        _ -> FullName
    end.

%% @private List collections for a tenant.
list_tenant_collections(TenantId) ->
    Hash = tenant_hash(TenantId),
    Prefix = <<Hash/binary, "_", TenantId/binary, "_">>,
    PrefixLen = byte_size(Prefix),

    All = do_list_collections(),
    [#{name => strip_prefix(C, TenantId)}
     || C <- All,
        byte_size(C) > PrefixLen,
        binary:part(C, 0, PrefixLen) =:= Prefix].

%%====================================================================
%% Backend Abstraction
%%====================================================================

%% @private Detect backend mode.
get_backend() ->
    case application:get_env(barrel_vectordb, cluster_enabled, false) of
        true -> clustered;
        false -> standalone
    end.

%% @private Create collection - routes based on mode.
%% Returns ok | {error, Reason}
do_create_collection(Name, Config) ->
    case get_backend() of
        standalone ->
            %% Use the store supervisor to ensure processes are supervised
            case barrel_vectordb_gateway_stores:start_store(Config#{name => binary_to_atom(Name)}) of
                {ok, _Pid} -> ok;
                {error, {already_started, _Pid}} -> {error, already_exists};
                {error, Reason} -> {error, Reason}
            end;
        clustered ->
            case barrel_vectordb:create_collection(Name, Config) of
                {ok, _Meta} -> ok;
                {error, Reason} -> {error, Reason}
            end
    end.

%% @private Delete collection.
do_delete_collection(Name) ->
    case get_backend() of
        standalone ->
            barrel_vectordb_gateway_stores:stop_store(binary_to_atom(Name));
        clustered ->
            barrel_vectordb:delete_collection(Name)
    end.

%% @private Get collection info.
do_get_collection(Name) ->
    case get_backend() of
        standalone ->
            case catch barrel_vectordb:stats(binary_to_atom(Name)) of
                {'EXIT', _} -> {error, not_found};
                Stats -> {ok, Stats}
            end;
        clustered ->
            barrel_vectordb:get_collection(Name)
    end.

%% @private List all collections.
%% Returns a list of collection names (binaries).
do_list_collections() ->
    case get_backend() of
        standalone ->
            %% In standalone mode, list all collections from all tenants
            %% We need the full names (with prefix) for filtering later
            case barrel_vectordb_system_db:get_tenant_ids() of
                {ok, TenantIds} ->
                    lists:foldl(fun(TenantId, Acc) ->
                        case barrel_vectordb_system_db:list_collections_by_tenant(TenantId) of
                            {ok, Colls} -> Colls ++ Acc;
                            {error, _} -> Acc
                        end
                    end, [], TenantIds);
                {error, _} ->
                    []
            end;
        clustered ->
            case barrel_vectordb:list_collections() of
                {ok, CollectionsMap} when is_map(CollectionsMap) ->
                    maps:keys(CollectionsMap);
                {error, _} ->
                    []
            end
    end.

%% @private Add document.
do_add(Collection, Id, Text, Metadata) ->
    case get_backend() of
        standalone ->
            barrel_vectordb:add(binary_to_atom(Collection), Id, Text, Metadata);
        clustered ->
            barrel_vectordb:cluster_add(Collection, Id, Text, Metadata)
    end.

%% @private Add document with vector.
do_add_vector(Collection, Id, Text, Metadata, Vector) ->
    case get_backend() of
        standalone ->
            barrel_vectordb:add_vector(binary_to_atom(Collection), Id, Text, Metadata, Vector);
        clustered ->
            barrel_vectordb:cluster_add_vector(Collection, Id, Text, Metadata, Vector)
    end.

%% @private Get document.
do_get(Collection, Id) ->
    case get_backend() of
        standalone ->
            barrel_vectordb:get(binary_to_atom(Collection), Id);
        clustered ->
            barrel_vectordb:cluster_get(Collection, Id)
    end.

%% @private Delete document.
do_delete(Collection, Id) ->
    case get_backend() of
        standalone ->
            barrel_vectordb:delete(binary_to_atom(Collection), Id);
        clustered ->
            barrel_vectordb:cluster_delete(Collection, Id)
    end.

%% @private Search with text query.
do_search(Collection, Query, Opts) ->
    case get_backend() of
        standalone ->
            barrel_vectordb:search(binary_to_atom(Collection), Query, Opts);
        clustered ->
            barrel_vectordb:cluster_search(Collection, Query, Opts)
    end.

%% @private Search with vector.
do_search_vector(Collection, Vector, Opts) ->
    case get_backend() of
        standalone ->
            barrel_vectordb:search_vector(binary_to_atom(Collection), Vector, Opts);
        clustered ->
            barrel_vectordb:cluster_search_vector(Collection, Vector, Opts)
    end.

%%====================================================================
%% Helpers
%%====================================================================

%% @private Get master API key from configuration.
get_master_key() ->
    case application:get_env(barrel_vectordb, gateway) of
        {ok, #{master_api_key := Key}} -> Key;
        _ ->
            %% Fallback to old config location
            case application:get_env(barrel_vectordb, master_api_key) of
                {ok, Key} -> Key;
                _ -> undefined
            end
    end.

%% @private Build collection config from request params.
build_collection_config(Params) ->
    Config = #{},
    Config1 = case maps:get(<<"dimension">>, Params, undefined) of
        undefined -> Config;
        Dim -> Config#{dimensions => Dim}
    end,
    case maps:get(<<"embedder">>, Params, undefined) of
        undefined -> Config1;
        EmbedConfig -> Config1#{embedder => parse_embedder_config(EmbedConfig)}
    end.

%% @private Parse embedder config from JSON.
parse_embedder_config(#{<<"type">> := <<"local">>} = Config) ->
    {local, maps:without([<<"type">>], Config)};
parse_embedder_config(#{<<"type">> := <<"ollama">>} = Config) ->
    {ollama, maps:without([<<"type">>], Config)};
parse_embedder_config(Config) when is_map(Config) ->
    Config.

%% @private Insert a document.
insert_document(Collection, #{<<"id">> := Id} = Doc) ->
    Text = maps:get(<<"text">>, Doc, <<>>),
    Vector = maps:get(<<"vector">>, Doc, undefined),
    Metadata = maps:get(<<"metadata">>, Doc, #{}),

    Result = case Vector of
        undefined ->
            do_add(Collection, Id, Text, Metadata);
        V when is_list(V) ->
            do_add_vector(Collection, Id, Text, Metadata, V)
    end,

    case Result of
        ok -> {ok, Id};
        {error, _} = Err -> Err
    end;
insert_document(Collection, Doc) ->
    %% Auto-generate ID if not provided
    Id = generate_doc_id(),
    insert_document(Collection, Doc#{<<"id">> => Id}).

%% @private Generate a document ID.
generate_doc_id() ->
    Bytes = crypto:strong_rand_bytes(12),
    base64:encode(Bytes, #{padding => false}).

%% @private Add filter to search options.
maybe_add_filter(Opts, undefined) -> Opts;
maybe_add_filter(Opts, FilterMap) when is_map(FilterMap) ->
    %% Convert JSON filter to Erlang filter function
    FilterFun = fun(Meta) ->
        maps:fold(fun(K, V, Acc) ->
            Acc andalso (maps:get(K, Meta, undefined) =:= V)
        end, true, FilterMap)
    end,
    Opts#{filter => FilterFun}.

%% @private Format search hits for JSON response.
%% Search results are maps: #{key := binary(), text := binary(), metadata := map(), score := float(), vector => [float()]}
format_hits(Hits) ->
    [#{id => maps:get(key, H),
       score => maps:get(score, H),
       text => maps:get(text, H),
       metadata => maps:get(metadata, H)}
     || H <- Hits].

%% @private Format insert results for batch response.
format_insert_results(Results) ->
    [case R of
        {ok, Id} -> #{id => Id, status => <<"created">>};
        {error, Reason} -> #{error => format_error(Reason)}
    end || R <- Results].

%% @private Format error for JSON response.
format_error(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason);
format_error(Reason) when is_binary(Reason) ->
    Reason;
format_error(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).

%% @private JSON response helper.
json_response(Status, Body, Req) ->
    cowboy_req:reply(Status, #{
        <<"content-type">> => <<"application/json">>
    }, json:encode(Body), Req).

%% @private Error response helper.
error_response(Status, Message, Req) ->
    json_response(Status, #{error => Message}, Req).

%%====================================================================
%% Enterprise Integration
%%====================================================================

%% @private Build audit context from request.
build_audit_context(Req, Action, StartTime) ->
    #{
        start_time => StartTime,
        request_id => get_or_generate_request_id(Req),
        client_ip => get_client_ip(Req),
        user_agent => cowboy_req:header(<<"user-agent">>, Req),
        method => cowboy_req:method(Req),
        path => cowboy_req:path(Req),
        query_params => cowboy_req:parse_qs(Req),
        action => Action,
        resource_id => get_resource_id(Req, Action)
    }.

%% @private Get or generate a request ID.
get_or_generate_request_id(Req) ->
    case cowboy_req:header(<<"x-request-id">>, Req) of
        undefined -> generate_request_id();
        Id -> Id
    end.

%% @private Generate a unique request ID.
generate_request_id() ->
    Bytes = crypto:strong_rand_bytes(12),
    base64:encode(Bytes, #{padding => false}).

%% @private Extract client IP, handling proxied requests.
get_client_ip(Req) ->
    case cowboy_req:header(<<"x-forwarded-for">>, Req) of
        undefined ->
            {IP, _Port} = cowboy_req:peer(Req),
            iolist_to_binary(inet:ntoa(IP));
        ForwardedFor ->
            %% Take first IP in chain (original client)
            [FirstIP | _] = binary:split(ForwardedFor, <<",">>),
            string:trim(FirstIP)
    end.

%% @private Get resource ID from request bindings.
get_resource_id(Req, Action) ->
    case Action of
        collection -> cowboy_req:binding(collection, Req);
        documents -> cowboy_req:binding(collection, Req);
        document -> cowboy_req:binding(doc_id, Req);
        search -> cowboy_req:binding(collection, Req);
        admin_keys -> cowboy_req:binding(tenant_id, Req);
        admin_usage -> cowboy_req:binding(tenant_id, Req);
        _ -> undefined
    end.

%% @private Get API key prefix for audit logging (first 12 chars).
%% Note: undefined clause kept for defensive programming
-dialyzer({nowarn_function, get_key_prefix/1}).
get_key_prefix(undefined) ->
    undefined;
get_key_prefix(KeyRecord) ->
    Key = barrel_vectordb_gateway_keys:key_value(KeyRecord),
    case Key of
        undefined -> undefined;
        _ -> binary:part(Key, 0, min(12, byte_size(Key)))
    end.

%% @private Call enterprise authentication if module is loaded.
%% Returns {handled, Result} or 'passthrough'.
enterprise_authenticate(Req, Action) ->
    case code:is_loaded(?ENTERPRISE_MOD) of
        {file, _} ->
            try
                ?ENTERPRISE_MOD:authenticate(Req, Action)
            catch
                _:_ -> passthrough
            end;
        false ->
            passthrough
    end.

%% @private Call enterprise authorization if module is loaded.
%% Returns {handled, Result} or 'passthrough'.
enterprise_authorize(Identity, Action, Ctx) ->
    case code:is_loaded(?ENTERPRISE_MOD) of
        {file, _} ->
            try
                ?ENTERPRISE_MOD:authorize(Identity, Action, Ctx)
            catch
                _:_ -> passthrough
            end;
        false ->
            passthrough
    end.

%% @private Call enterprise audit logging if module is loaded.
%% Returns {handled, ok} or 'passthrough'.
enterprise_audit(Ctx, Response, DurationUs) ->
    case code:is_loaded(?ENTERPRISE_MOD) of
        {file, _} ->
            try
                ?ENTERPRISE_MOD:audit(Ctx, Response, DurationUs)
            catch
                _:_ -> passthrough
            end;
        false ->
            passthrough
    end.
