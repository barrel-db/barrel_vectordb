%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb top level supervisor
%%%
%%% This supervisor manages the barrel_vectordb application. Individual
%%% stores are started dynamically via {@link barrel_vectordb:start_link/1}.
%%%
%%% When clustering is enabled (enable_cluster = true), the mesh
%%% supervisor is automatically started.
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

    %% Start mesh supervisor if clustering is enabled
    Children = case barrel_vectordb_app:cluster_enabled() of
        true ->
            [mesh_sup_child()];
        false ->
            []
    end,

    {ok, {SupFlags, Children}}.

%%====================================================================
%% Internal functions
%%====================================================================

mesh_sup_child() ->
    #{
        id => barrel_vectordb_mesh_sup,
        start => {barrel_vectordb_mesh_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [barrel_vectordb_mesh_sup]
    }.
