%%%-------------------------------------------------------------------
%%% @doc Compaction Service for Hybrid DiskANN Index
%%%
%%% Background service that manages compaction of hot HNSW layer
%%% into cold DiskANN layer. Features:
%%%
%%% - Automatic compaction triggers (capacity, age thresholds)
%%% - Manual compaction API
%%% - Non-blocking compaction (searches continue during merge)
%%% - Progress monitoring
%%%
%%% Based on FreshDiskANN StreamingMerge architecture.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_compaction).

-behaviour(gen_server).

%% API
-export([
    start_link/2,
    start_link/3,
    stop/1,
    trigger/1,
    status/1,
    set_index/2
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    owner :: pid(),                           %% Owner process (receives updates)
    index :: term() | undefined,              %% Current hybrid index
    config :: map(),                          %% Compaction config
    last_compaction :: integer() | undefined, %% Timestamp of last compaction
    compaction_count = 0 :: non_neg_integer(),%% Total compactions performed
    is_compacting = false :: boolean(),       %% Currently compacting?
    check_interval :: pos_integer(),          %% Ms between compaction checks
    timer_ref :: reference() | undefined      %% Timer reference
}).

%%====================================================================
%% API
%%====================================================================

%% @doc Start compaction service linked to calling process
-spec start_link(pid(), map()) -> {ok, pid()} | {error, term()}.
start_link(Owner, Config) ->
    start_link(Owner, Config, undefined).

%% @doc Start compaction service with initial index
-spec start_link(pid(), map(), term()) -> {ok, pid()} | {error, term()}.
start_link(Owner, Config, Index) ->
    gen_server:start_link(?MODULE, [Owner, Config, Index], []).

%% @doc Stop compaction service
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc Manually trigger compaction
-spec trigger(pid()) -> ok | {error, already_compacting}.
trigger(Pid) ->
    gen_server:call(Pid, trigger, infinity).

%% @doc Get compaction status
-spec status(pid()) -> map().
status(Pid) ->
    gen_server:call(Pid, status).

%% @doc Update the index (called after external modifications)
-spec set_index(pid(), term()) -> ok.
set_index(Pid, Index) ->
    gen_server:cast(Pid, {set_index, Index}).

%%====================================================================
%% gen_server Callbacks
%%====================================================================

init([Owner, Config, Index]) ->
    CheckInterval = maps:get(check_interval_ms, Config, 60000), %% Default 1 minute
    TimerRef = schedule_check(CheckInterval),

    {ok, #state{
        owner = Owner,
        index = Index,
        config = Config,
        check_interval = CheckInterval,
        timer_ref = TimerRef
    }}.

handle_call(trigger, _From, #state{is_compacting = true} = State) ->
    {reply, {error, already_compacting}, State};
handle_call(trigger, From, State) ->
    %% Start compaction in background, reply immediately
    Self = self(),
    spawn_link(fun() ->
        Result = do_compact(State),
        gen_server:reply(From, ok),
        gen_server:cast(Self, {compaction_done, Result})
    end),
    {noreply, State#state{is_compacting = true}};

handle_call(status, _From, #state{last_compaction = Last,
                                   compaction_count = Count,
                                   is_compacting = IsCompacting,
                                   index = Index} = State) ->
    IndexInfo = case Index of
        undefined -> #{};
        _ ->
            try barrel_vectordb_index_hybrid:info(Index)
            catch _:_ -> #{}
            end
    end,
    Status = #{
        last_compaction => Last,
        compaction_count => Count,
        is_compacting => IsCompacting,
        index_info => IndexInfo
    },
    {reply, Status, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({set_index, Index}, State) ->
    {noreply, State#state{index = Index}};

handle_cast({compaction_done, {ok, NewIndex}}, #state{owner = Owner,
                                                       compaction_count = Count} = State) ->
    Now = erlang:system_time(millisecond),
    %% Notify owner of new index
    Owner ! {compaction_complete, self(), NewIndex},
    {noreply, State#state{
        index = NewIndex,
        last_compaction = Now,
        compaction_count = Count + 1,
        is_compacting = false
    }};

handle_cast({compaction_done, {error, Reason}}, #state{owner = Owner} = State) ->
    %% Notify owner of failure
    Owner ! {compaction_failed, self(), Reason},
    {noreply, State#state{is_compacting = false}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_compaction, #state{index = undefined,
                                      check_interval = Interval} = State) ->
    %% No index yet, reschedule
    TimerRef = schedule_check(Interval),
    {noreply, State#state{timer_ref = TimerRef}};

handle_info(check_compaction, #state{is_compacting = true,
                                      check_interval = Interval} = State) ->
    %% Already compacting, reschedule
    TimerRef = schedule_check(Interval),
    {noreply, State#state{timer_ref = TimerRef}};

handle_info(check_compaction, #state{index = Index,
                                      check_interval = Interval} = State) ->
    NewState = case should_compact(Index) of
        true ->
            %% Start compaction in background
            Self = self(),
            spawn_link(fun() ->
                Result = do_compact(State),
                gen_server:cast(Self, {compaction_done, Result})
            end),
            State#state{is_compacting = true};
        false ->
            State
    end,
    TimerRef = schedule_check(Interval),
    {noreply, NewState#state{timer_ref = TimerRef}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{timer_ref = TimerRef}) ->
    case TimerRef of
        undefined -> ok;
        _ -> erlang:cancel_timer(TimerRef)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

schedule_check(Interval) ->
    erlang:send_after(Interval, self(), check_compaction).

should_compact(Index) ->
    try
        barrel_vectordb_index_hybrid:should_compact(Index)
    catch
        _:_ -> false
    end.

do_compact(#state{index = Index}) ->
    try
        barrel_vectordb_index_hybrid:compact(Index)
    catch
        Class:Reason:Stack ->
            {error, {Class, Reason, Stack}}
    end.
