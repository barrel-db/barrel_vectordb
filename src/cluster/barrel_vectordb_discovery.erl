%% @doc Node discovery for barrel_vectordb cluster.
%%
%% Supports multiple discovery modes:
%% - manual: Explicit join via cluster_join/1
%% - seed: Join via configured seed nodes
%% - dns: SRV or A record lookup
%%
%% @end
-module(barrel_vectordb_discovery).
-behaviour(gen_server).

-export([start_link/0, start_link/1]).
-export([discover/0, join_via_seed/1]).
-export([get_config/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(state, {
    mode :: seed | dns | manual,
    seed_nodes = [] :: [atom()],
    dns_config = #{} :: map(),
    discover_interval :: pos_integer(),
    discover_timer :: reference() | undefined
}).

-define(DEFAULT_DISCOVER_INTERVAL, 30000).

%% API

start_link() ->
    start_link(#{}).

start_link(Config) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

%% @doc Trigger discovery process
discover() ->
    gen_server:call(?MODULE, discover).

%% @doc Join cluster via a specific seed node
join_via_seed(SeedNode) when is_atom(SeedNode) ->
    gen_server:call(?MODULE, {join_via_seed, SeedNode}, 30000).

%% @doc Get current discovery configuration
get_config() ->
    gen_server:call(?MODULE, get_config).

%% gen_server callbacks

init(Config) ->
    ClusterOpts = application:get_env(barrel_vectordb, cluster_options, #{}),
    MergedConfig = maps:merge(ClusterOpts, Config),

    Mode = maps:get(discovery_mode, MergedConfig, manual),
    SeedNodes = maps:get(seed_nodes, MergedConfig, []),
    DnsConfig = maps:get(dns, MergedConfig, #{}),
    Interval = maps:get(discover_interval, MergedConfig, ?DEFAULT_DISCOVER_INTERVAL),

    State = #state{
        mode = Mode,
        seed_nodes = SeedNodes,
        dns_config = DnsConfig,
        discover_interval = Interval
    },

    %% Auto-discover on startup if not manual mode
    case Mode of
        manual ->
            {ok, State};
        _ ->
            self() ! discover,
            {ok, State}
    end.

handle_call(discover, _From, State) ->
    Result = do_discover(State),
    {reply, Result, maybe_schedule_discover(State)};

handle_call({join_via_seed, SeedNode}, _From, State) ->
    Result = barrel_vectordb_ra:join(SeedNode),
    case Result of
        {ok, _} ->
            %% Register the seed node for health monitoring
            barrel_vectordb_health:register_node(SeedNode);
        _ ->
            ok
    end,
    {reply, Result, State};

handle_call(get_config, _From, State) ->
    Config = #{
        mode => State#state.mode,
        seed_nodes => State#state.seed_nodes,
        dns_config => State#state.dns_config,
        discover_interval => State#state.discover_interval
    },
    {reply, Config, State};

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(discover, State) ->
    do_discover(State),
    {noreply, maybe_schedule_discover(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{discover_timer = Timer}) ->
    case Timer of
        undefined -> ok;
        Ref -> erlang:cancel_timer(Ref)
    end,
    ok.

%% Internal functions

do_discover(#state{mode = manual}) ->
    {ok, []};

do_discover(#state{mode = seed, seed_nodes = Seeds}) ->
    discover_from_seeds(Seeds);

do_discover(#state{mode = dns, dns_config = DnsConfig}) ->
    discover_from_dns(DnsConfig).

discover_from_seeds([]) ->
    {ok, []};
discover_from_seeds(Seeds) ->
    case barrel_vectordb_mesh:is_clustered() of
        true ->
            %% Already in cluster, just register nodes for health monitoring
            lists:foreach(fun(Node) ->
                barrel_vectordb_health:register_node(Node)
            end, Seeds),
            {ok, Seeds};
        false ->
            %% Try each seed until one works
            try_seeds(Seeds)
    end.

try_seeds([]) ->
    {error, no_seeds_available};
try_seeds([SeedNode | Rest]) ->
    case barrel_vectordb_ra:join(SeedNode) of
        {ok, _} ->
            %% Successfully joined, register seed for health monitoring
            barrel_vectordb_health:register_node(SeedNode),
            {ok, SeedNode};
        {error, _Reason} ->
            try_seeds(Rest)
    end.

discover_from_dns(#{domain := Domain} = Config) ->
    Type = maps:get(type, Config, srv),
    case Type of
        srv ->
            discover_dns_srv(Domain);
        a ->
            discover_dns_a(Domain)
    end;
discover_from_dns(_) ->
    {error, missing_dns_domain}.

discover_dns_srv(Domain) ->
    SrvName = "_barrel-vdb._tcp." ++ binary_to_list(Domain),
    case inet_res:lookup(SrvName, in, srv) of
        [] ->
            {error, no_srv_records};
        Records ->
            %% Sort by priority and weight
            Sorted = lists:sort(
                fun({P1, W1, _, _}, {P2, W2, _, _}) ->
                    {P1, -W1} =< {P2, -W2}
                end,
                Records),
            Nodes = [list_to_atom("barrel@" ++ Host) || {_, _, _, Host} <- Sorted],
            case barrel_vectordb_mesh:is_clustered() of
                true ->
                    lists:foreach(fun barrel_vectordb_health:register_node/1, Nodes),
                    {ok, Nodes};
                false ->
                    try_seeds(Nodes)
            end
    end.

discover_dns_a(Domain) ->
    case inet_res:lookup(binary_to_list(Domain), in, a) of
        [] ->
            {error, no_a_records};
        IPs ->
            Nodes = [list_to_atom("barrel@" ++ inet:ntoa(IP)) || IP <- IPs],
            case barrel_vectordb_mesh:is_clustered() of
                true ->
                    lists:foreach(fun barrel_vectordb_health:register_node/1, Nodes),
                    {ok, Nodes};
                false ->
                    try_seeds(Nodes)
            end
    end.

maybe_schedule_discover(#state{mode = manual} = State) ->
    State;
maybe_schedule_discover(#state{discover_interval = Interval} = State) ->
    Timer = erlang:send_after(Interval, self(), discover),
    State#state{discover_timer = Timer}.
