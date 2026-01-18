%%%-------------------------------------------------------------------
%%% @doc Gateway HTTP listener wrapper
%%%
%%% Manages the cowboy HTTP listener lifecycle. Starts cowboy in init
%%% and stops it in terminate.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_gateway_listener).
-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(LISTENER_NAME, gateway_http_listener).

-record(state, {
    port :: pos_integer()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Config, []).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    Port = maps:get(port, Config, 8080),

    %% Ensure cowboy is started
    case application:ensure_all_started(cowboy) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok
    end,

    %% Build dispatch routes
    Dispatch = cowboy_router:compile([
        {'_', barrel_vectordb_gateway:routes()}
    ]),

    %% Transport options
    TransOpts = #{
        socket_opts => [{port, Port}],
        num_acceptors => maps:get(num_acceptors, Config, 100)
    },

    %% Protocol options
    ProtoOpts = #{
        env => #{
            dispatch => Dispatch,
            gateway_config => Config
        }
    },

    %% Start the listener
    case cowboy:start_clear(?LISTENER_NAME, TransOpts, ProtoOpts) of
        {ok, _Pid} ->
            {ok, #state{port = Port}};
        {error, {already_started, _Pid}} ->
            {ok, #state{port = Port}};
        {error, Reason} ->
            {stop, {listener_start_failed, Reason}}
    end.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    _ = cowboy:stop_listener(?LISTENER_NAME),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
