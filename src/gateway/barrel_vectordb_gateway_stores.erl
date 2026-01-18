%%%-------------------------------------------------------------------
%%% @doc Gateway store manager
%%%
%%% Supervises collection stores created through the gateway in standalone
%%% mode. Uses a simple_one_for_one supervisor strategy.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_gateway_stores).
-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([start_store/1, stop_store/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the store supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% @doc Start a new store under supervision.
-spec start_store(map()) -> {ok, pid()} | {error, term()}.
start_store(Config) ->
    supervisor:start_child(?SERVER, [Config]).

%% @doc Stop a store.
-spec stop_store(atom()) -> ok | {error, term()}.
stop_store(Name) when is_atom(Name) ->
    case whereis(Name) of
        undefined ->
            {error, not_found};
        Pid ->
            supervisor:terminate_child(?SERVER, Pid)
    end.

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 60
    },

    ChildSpec = #{
        id => barrel_vectordb_store,
        start => {barrel_vectordb, start_link, []},
        restart => transient,
        shutdown => 5000,
        type => worker,
        modules => [barrel_vectordb]
    },

    {ok, {SupFlags, [ChildSpec]}}.
