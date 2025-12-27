%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb top level supervisor
%%%
%%% This supervisor manages the barrel_vectordb application. Individual
%%% stores are started dynamically via {@link barrel_vectordb:start_link/1}.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the barrel_vectordb supervisor.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% @private
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 60
    },

    %% No default children - stores are started dynamically
    %% via barrel_vectordb:start_link/1
    Children = [],

    {ok, {SupFlags, Children}}.
