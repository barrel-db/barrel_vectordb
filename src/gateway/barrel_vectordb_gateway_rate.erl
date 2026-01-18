%%%-------------------------------------------------------------------
%%% @doc Rate limiter for the gateway
%%%
%%% Implements a token bucket algorithm for rate limiting requests
%%% per tenant. Uses ETS for fast, ephemeral storage.
%%% Rate limits are per-node (not replicated).
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_gateway_rate).
-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([check_rate/1, check_rate/2]).
-export([get_bucket_info/1, reset_bucket/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE, gateway_rate_limits).
-define(CLEANUP_INTERVAL, 60000).  % 1 minute cleanup interval
-define(BUCKET_TTL, 300).  % 5 minutes TTL for idle buckets

%% Token bucket stored in ETS as tuple:
%% {TenantId, Tokens, LastRefill, RatePerSecond, MaxTokens}

-record(state, {
    cleanup_timer :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the rate limiter gen_server.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Check if a request is allowed for a tenant.
%% Uses the tenant's configured RPM limit.
-spec check_rate(binary()) -> ok | {error, rate_limited}.
check_rate(TenantId) when is_binary(TenantId) ->
    RpmLimit = get_tenant_rpm(TenantId),
    check_rate(TenantId, RpmLimit).

%% @doc Check if a request is allowed for a tenant with specific RPM limit.
-spec check_rate(binary(), pos_integer()) -> ok | {error, rate_limited}.
check_rate(TenantId, RpmLimit) when is_binary(TenantId), is_integer(RpmLimit), RpmLimit > 0 ->
    Now = erlang:system_time(second),
    RatePerSecond = RpmLimit / 60.0,
    MaxTokens = RpmLimit,

    case ets:lookup(?TABLE, TenantId) of
        [{TenantId, Tokens, LastRefill, _OldRate, _OldMax}] ->
            %% Refill tokens based on time elapsed
            Elapsed = Now - LastRefill,
            NewTokens = min(MaxTokens, Tokens + (Elapsed * RatePerSecond)),

            if
                NewTokens >= 1.0 ->
                    %% Consume one token
                    ets:insert(?TABLE, {TenantId, NewTokens - 1.0, Now, RatePerSecond, MaxTokens}),
                    ok;
                true ->
                    {error, rate_limited}
            end;
        [] ->
            %% Initialize new bucket with max tokens - 1 (consuming current request)
            ets:insert(?TABLE, {TenantId, MaxTokens - 1.0, Now, RatePerSecond, MaxTokens}),
            ok
    end.

%% @doc Get bucket info for a tenant (for debugging/monitoring).
-spec get_bucket_info(binary()) -> {ok, map()} | {error, not_found}.
get_bucket_info(TenantId) when is_binary(TenantId) ->
    case ets:lookup(?TABLE, TenantId) of
        [{TenantId, Tokens, LastRefill, Rate, MaxTokens}] ->
            {ok, #{
                tenant_id => TenantId,
                tokens => Tokens,
                last_refill => LastRefill,
                rate_per_second => Rate,
                max_tokens => MaxTokens
            }};
        [] ->
            {error, not_found}
    end.

%% @doc Reset the rate limit bucket for a tenant.
-spec reset_bucket(binary()) -> ok.
reset_bucket(TenantId) when is_binary(TenantId) ->
    ets:delete(?TABLE, TenantId),
    ok.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    %% Create ETS table for rate limiting
    %% public: any process can read/write (for fast path in check_rate)
    %% set: one entry per tenant
    %% named_table: accessible by name
    _ = ets:new(?TABLE, [named_table, public, set, {write_concurrency, true}, {read_concurrency, true}]),

    %% Start cleanup timer
    Timer = erlang:send_after(?CLEANUP_INTERVAL, self(), cleanup_stale_buckets),

    {ok, #state{cleanup_timer = Timer}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(cleanup_stale_buckets, State) ->
    %% Remove buckets that haven't been used recently
    Now = erlang:system_time(second),
    Cutoff = Now - ?BUCKET_TTL,

    %% Iterate through all buckets and delete stale ones
    ets:foldl(fun({TenantId, _Tokens, LastRefill, _Rate, _Max}, Acc) ->
        case LastRefill < Cutoff of
            true -> ets:delete(?TABLE, TenantId);
            false -> ok
        end,
        Acc
    end, ok, ?TABLE),

    %% Schedule next cleanup
    Timer = erlang:send_after(?CLEANUP_INTERVAL, self(), cleanup_stale_buckets),
    {noreply, State#state{cleanup_timer = Timer}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{cleanup_timer = Timer}) ->
    case Timer of
        undefined -> ok;
        _ -> _ = erlang:cancel_timer(Timer), ok
    end,
    _ = ets:delete(?TABLE),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @private Get RPM limit for a tenant from their API key configuration.
get_tenant_rpm(TenantId) ->
    case barrel_vectordb_gateway_keys:get_tenant_limits(TenantId) of
        {ok, #{rpm_limit := RpmLimit}} -> RpmLimit;
        {error, _} -> get_default_rpm()
    end.

%% @private Get default RPM from configuration.
get_default_rpm() ->
    case application:get_env(barrel_vectordb, gateway) of
        {ok, GatewayConfig} ->
            maps:get(default_rate_limit, GatewayConfig, 100);
        undefined ->
            100
    end.
