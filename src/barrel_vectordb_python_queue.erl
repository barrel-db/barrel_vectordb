%%%-------------------------------------------------------------------
%%% @doc Python execution rate limiter
%%%
%%% ETS-based counting semaphore to limit concurrent Python executions.
%%% Prevents resource exhaustion under concurrent load.
%%%
%%% == Usage ==
%%% ```
%%% %% In application start
%%% barrel_vectordb_python_queue:init().
%%%
%%% %% In provider code
%%% case barrel_vectordb_python_queue:acquire(Timeout) of
%%%     ok ->
%%%         try
%%%             do_python_call(...)
%%%         after
%%%             barrel_vectordb_python_queue:release()
%%%         end;
%%%     {error, timeout} ->
%%%         {error, queue_timeout}
%%% end.
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% %% In sys.config (optional, default is schedulers div 2 + 1)
%%% {barrel_vectordb, [
%%%     {python_max_concurrent, 4}
%%% ]}
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_python_queue).

-export([
    init/0,
    acquire/1,
    release/0,
    max_concurrent/0,
    current/0
]).

-define(TABLE, barrel_vectordb_python_queue).
-define(COUNTER_KEY, running).
-define(BACKOFF_MS, 10).

%%====================================================================
%% API
%%====================================================================

%% @doc Initialize the ETS table. Call once at application start.
-spec init() -> ok.
init() ->
    case ets:whereis(?TABLE) of
        undefined ->
            _ = ets:new(?TABLE, [named_table, public, {write_concurrency, true}]),
            ets:insert(?TABLE, {?COUNTER_KEY, 0}),
            ok;
        _Tid ->
            %% Already initialized
            ok
    end.

%% @doc Acquire a slot for Python execution.
%% Blocks until a slot is available or timeout expires.
-spec acquire(timeout()) -> ok | {error, timeout}.
acquire(Timeout) ->
    Max = max_concurrent(),
    StartTime = erlang:monotonic_time(millisecond),
    acquire_loop(Max, Timeout, StartTime).

%% @doc Release a slot after Python execution completes.
-spec release() -> ok.
release() ->
    _ = ets:update_counter(?TABLE, ?COUNTER_KEY, {2, -1, 0, 0}),
    ok.

%% @doc Get the maximum concurrent Python executions allowed.
-spec max_concurrent() -> pos_integer().
max_concurrent() ->
    case application:get_env(barrel_vectordb, python_max_concurrent) of
        {ok, N} when is_integer(N), N > 0 -> N;
        _ -> max(1, erlang:system_info(schedulers) div 2 + 1)
    end.

%% @doc Get the current number of running Python executions.
-spec current() -> non_neg_integer().
current() ->
    case ets:whereis(?TABLE) of
        undefined -> 0;
        _Tid ->
            case ets:lookup(?TABLE, ?COUNTER_KEY) of
                [{?COUNTER_KEY, N}] -> N;
                [] -> 0
            end
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

acquire_loop(Max, Timeout, StartTime) ->
    %% Atomically increment counter (no cap)
    N = ets:update_counter(?TABLE, ?COUNTER_KEY, {2, 1}),
    if
        N =< Max ->
            %% Got a slot
            ok;
        true ->
            %% Over limit - decrement back and wait
            _ = ets:update_counter(?TABLE, ?COUNTER_KEY, {2, -1}),
            Elapsed = erlang:monotonic_time(millisecond) - StartTime,
            case Timeout =/= infinity andalso Elapsed >= Timeout of
                true ->
                    {error, timeout};
                false ->
                    timer:sleep(?BACKOFF_MS),
                    acquire_loop(Max, Timeout, StartTime)
            end
    end.
