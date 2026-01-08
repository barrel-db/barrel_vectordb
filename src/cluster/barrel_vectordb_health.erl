%% @doc aten-based node health monitoring.
%%
%% Uses aten (adaptive accrual failure detector) for fast failure
%% detection (1-5s) instead of traditional heartbeat timeout (45-90s).
%%
%% @end
-module(barrel_vectordb_health).
-behaviour(gen_server).

-export([start_link/0]).
-export([register_node/1, unregister_node/1]).
-export([is_healthy/1, healthy_nodes/0]).
-export([subscribe/1, unsubscribe/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    %% Nodes we're monitoring via aten
    monitored = #{} :: #{node() => up | down},
    %% Subscribers for health events
    subscribers = [] :: [pid()]
}).

%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Register a node for health monitoring via aten
register_node(Node) when is_atom(Node) ->
    gen_server:call(?MODULE, {register, Node}).

%% @doc Unregister a node from health monitoring
unregister_node(Node) when is_atom(Node) ->
    gen_server:call(?MODULE, {unregister, Node}).

%% @doc Check if a node is healthy
is_healthy(Node) when is_atom(Node) ->
    gen_server:call(?MODULE, {is_healthy, Node}).

%% @doc Get list of all healthy nodes
healthy_nodes() ->
    gen_server:call(?MODULE, healthy_nodes).

%% @doc Subscribe to health events
subscribe(Pid) ->
    gen_server:call(?MODULE, {subscribe, Pid}).

%% @doc Unsubscribe from health events
unsubscribe(Pid) ->
    gen_server:call(?MODULE, {unsubscribe, Pid}).

%% gen_server callbacks

init([]) ->
    %% aten is started as part of Ra
    {ok, #state{}}.

handle_call({register, Node}, _From, State) ->
    case Node =:= node() of
        true ->
            %% Don't monitor ourselves
            {reply, ok, State};
        false ->
            %% Register with aten for monitoring
            ok = aten:register(Node),
            Monitored = maps:put(Node, up, State#state.monitored),
            {reply, ok, State#state{monitored = Monitored}}
    end;

handle_call({unregister, Node}, _From, State) ->
    case maps:is_key(Node, State#state.monitored) of
        true ->
            ok = aten:unregister(Node),
            Monitored = maps:remove(Node, State#state.monitored),
            {reply, ok, State#state{monitored = Monitored}};
        false ->
            {reply, ok, State}
    end;

handle_call({is_healthy, Node}, _From, State) ->
    case Node =:= node() of
        true ->
            {reply, true, State};
        false ->
            Status = maps:get(Node, State#state.monitored, unknown),
            {reply, Status =:= up, State}
    end;

handle_call(healthy_nodes, _From, State) ->
    Healthy = [Node || {Node, Status} <- maps:to_list(State#state.monitored),
                       Status =:= up],
    %% Include ourselves
    AllHealthy = [node() | Healthy],
    {reply, AllHealthy, State};

handle_call({subscribe, Pid}, _From, State) ->
    erlang:monitor(process, Pid),
    Subscribers = [Pid | State#state.subscribers],
    {reply, ok, State#state{subscribers = Subscribers}};

handle_call({unsubscribe, Pid}, _From, State) ->
    Subscribers = lists:delete(Pid, State#state.subscribers),
    {reply, ok, State#state{subscribers = Subscribers}};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% aten sends {node_event, Node, up | down} messages
handle_info({node_event, Node, Status}, State) ->
    case maps:is_key(Node, State#state.monitored) of
        true ->
            OldStatus = maps:get(Node, State#state.monitored),
            case Status of
                OldStatus ->
                    %% No change
                    {noreply, State};
                _ ->
                    %% Status changed
                    Monitored = maps:put(Node, Status, State#state.monitored),
                    NewState = State#state{monitored = Monitored},
                    notify_subscribers({node_status, Node, Status}, NewState),
                    %% If node went down, trigger cluster handling
                    case Status of
                        down ->
                            handle_node_down(Node);
                        up ->
                            handle_node_up(Node)
                    end,
                    {noreply, NewState}
            end;
        false ->
            {noreply, State}
    end;

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    Subscribers = lists:delete(Pid, State#state.subscribers),
    {noreply, State#state{subscribers = Subscribers}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    %% Unregister all monitored nodes
    maps:foreach(
        fun(Node, _) ->
            catch aten:unregister(Node)
        end,
        State#state.monitored),
    ok.

%% Internal functions

notify_subscribers(Event, #state{subscribers = Subscribers}) ->
    [Pid ! {health_event, Event} || Pid <- Subscribers],
    ok.

handle_node_down(Node) ->
    %% Notify cluster that node is down
    NodeId = {barrel_vectordb, Node},
    barrel_vectordb_cluster_events:node_left(NodeId),
    %% Trigger shard coordinator to handle failure
    barrel_vectordb_shard_coordinator:handle_node_leave(NodeId).

handle_node_up(Node) ->
    %% Node came back - it will re-join via discovery
    NodeId = {barrel_vectordb, Node},
    barrel_vectordb_cluster_events:node_joined(NodeId, #{node => Node, status => active}).
