%% @doc Async replication of writes to shard followers.
%%
%% Leaders queue writes after local success and this module
%% replicates them to followers via RPC.
%%
%% @end
-module(barrel_vectordb_shard_replicator).
-behaviour(gen_server).

-export([start_link/0]).
-export([replicate/3]).
-export([apply_replicated/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    %% Queue of pending replications per shard
    pending = #{} :: #{shard_id() => queue:queue()},
    %% Active replication workers
    workers = #{} :: #{shard_id() => pid()}
}).

-type shard_id() :: {binary(), non_neg_integer()}.

%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Queue a write operation for replication to followers
%% Called by leader after successful local write
replicate(ShardId, Operation, Args) ->
    gen_server:cast(?MODULE, {replicate, ShardId, Operation, Args}).

%% @doc Apply a replicated write on a follower
%% Called via RPC from leader
apply_replicated(ShardId, Operation, Args) ->
    case barrel_vectordb_shard_manager:get_local_store(ShardId) of
        {ok, Store} ->
            apply_operation(Store, Operation, Args);
        Error ->
            Error
    end.

%% gen_server callbacks

init([]) ->
    {ok, #state{}}.

handle_call({get_batch, ShardId, MaxCount}, _From, State) ->
    case maps:get(ShardId, State#state.pending, undefined) of
        undefined ->
            {reply, {ok, []}, State};
        Queue ->
            {Batch, NewQueue} = take_batch(Queue, MaxCount),
            NewPending = case queue:is_empty(NewQueue) of
                true -> maps:remove(ShardId, State#state.pending);
                false -> maps:put(ShardId, NewQueue, State#state.pending)
            end,
            {reply, {ok, Batch}, State#state{pending = NewPending}}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast({replicate, ShardId, Operation, Args}, State) ->
    %% Add to pending queue
    Queue = maps:get(ShardId, State#state.pending, queue:new()),
    NewQueue = queue:in({Operation, Args}, Queue),
    NewPending = maps:put(ShardId, NewQueue, State#state.pending),

    %% Ensure worker is running for this shard
    NewWorkers = ensure_worker(ShardId, State#state.workers),

    {noreply, State#state{pending = NewPending, workers = NewWorkers}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({worker_done, ShardId}, State) ->
    %% Worker finished, check if more work pending
    case maps:get(ShardId, State#state.pending, undefined) of
        undefined ->
            Workers = maps:remove(ShardId, State#state.workers),
            {noreply, State#state{workers = Workers}};
        Queue ->
            case queue:is_empty(Queue) of
                true ->
                    Workers = maps:remove(ShardId, State#state.workers),
                    Pending = maps:remove(ShardId, State#state.pending),
                    {noreply, State#state{pending = Pending, workers = Workers}};
                false ->
                    %% More work, restart worker
                    NewWorkers = ensure_worker(ShardId, maps:remove(ShardId, State#state.workers)),
                    {noreply, State#state{workers = NewWorkers}}
            end
    end;

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    %% Worker crashed, find and restart it
    ShardId = find_shard_for_worker(Pid, State#state.workers),
    case ShardId of
        undefined ->
            {noreply, State};
        _ ->
            Workers = maps:remove(ShardId, State#state.workers),
            %% Check if there's pending work
            case maps:get(ShardId, State#state.pending, undefined) of
                undefined ->
                    {noreply, State#state{workers = Workers}};
                Queue ->
                    case queue:is_empty(Queue) of
                        true ->
                            {noreply, State#state{workers = Workers}};
                        false ->
                            %% Restart worker
                            NewWorkers = ensure_worker(ShardId, Workers),
                            {noreply, State#state{workers = NewWorkers}}
                    end
            end
    end;

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% Internal functions

take_batch(Queue, MaxCount) ->
    take_batch(Queue, MaxCount, []).

take_batch(Queue, 0, Acc) ->
    {lists:reverse(Acc), Queue};
take_batch(Queue, N, Acc) ->
    case queue:out(Queue) of
        {{value, Item}, NewQueue} ->
            take_batch(NewQueue, N - 1, [Item | Acc]);
        {empty, _} ->
            {lists:reverse(Acc), Queue}
    end.

ensure_worker(ShardId, Workers) ->
    case maps:get(ShardId, Workers, undefined) of
        undefined ->
            Parent = self(),
            Pid = spawn_link(fun() -> replication_worker(Parent, ShardId) end),
            erlang:monitor(process, Pid),
            maps:put(ShardId, Pid, Workers);
        _Pid ->
            Workers
    end.

replication_worker(Parent, ShardId) ->
    %% Get followers for this shard
    case get_followers(ShardId) of
        {ok, Followers} ->
            %% Get pending operations
            case get_pending_batch(ShardId) of
                [] ->
                    ok;
                Batch ->
                    %% Replicate to each follower
                    replicate_batch_to_followers(ShardId, Batch, Followers)
            end;
        _ ->
            ok
    end,
    Parent ! {worker_done, ShardId}.

get_followers(ShardId) ->
    case barrel_vectordb_cluster_client:get_shards() of
        {ok, Shards} ->
            case maps:get(ShardId, Shards, undefined) of
                undefined ->
                    {error, shard_not_found};
                Assignment ->
                    LocalNodeId = barrel_vectordb_mesh:node_id(),
                    Replicas = element(4, Assignment),
                    %% Followers are all replicas except us (leader)
                    Followers = lists:delete(LocalNodeId, Replicas),
                    {ok, Followers}
            end;
        Error ->
            Error
    end.

get_pending_batch(ShardId) ->
    %% Get up to 100 pending operations
    case gen_server:call(?MODULE, {get_batch, ShardId, 100}, 5000) of
        {ok, Batch} -> Batch;
        _ -> []
    end.

replicate_batch_to_followers(ShardId, Batch, Followers) ->
    lists:foreach(
        fun(FollowerNodeId) ->
            replicate_to_follower(ShardId, Batch, FollowerNodeId)
        end,
        Followers).

replicate_to_follower(ShardId, Batch, {_Name, Node}) ->
    %% Send batch to follower via RPC
    lists:foreach(
        fun({Operation, Args}) ->
            case rpc:call(Node, ?MODULE, apply_replicated, [ShardId, Operation, Args], 5000) of
                {badrpc, _Reason} ->
                    %% Log error but continue
                    ok;
                _ ->
                    ok
            end
        end,
        Batch).

apply_operation(Store, add_vector, [Id, Text, Metadata, Vector]) ->
    barrel_vectordb:add_vector(Store, Id, Text, Metadata, Vector);
apply_operation(Store, delete, [Id]) ->
    barrel_vectordb:delete(Store, Id);
apply_operation(_Store, _Op, _Args) ->
    {error, unknown_operation}.

find_shard_for_worker(Pid, Workers) ->
    Result = maps:fold(
        fun(ShardId, WorkerPid, Acc) ->
            case WorkerPid of
                Pid -> ShardId;
                _ -> Acc
            end
        end,
        undefined,
        Workers),
    Result.
