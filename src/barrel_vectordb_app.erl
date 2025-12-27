%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb application module
%%%
%%% This module implements the OTP application behaviour for barrel_vectordb.
%%% It starts the top-level supervisor which manages the vector database
%%% components.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_app).
-behaviour(application).

-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

%% @doc Start the barrel_vectordb application.
%% @private
start(_StartType, _StartArgs) ->
    barrel_vectordb_sup:start_link().

%% @doc Stop the barrel_vectordb application.
%% @private
stop(_State) ->
    ok.
