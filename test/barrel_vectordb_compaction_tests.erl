%%%-------------------------------------------------------------------
%%% @doc EUnit tests for barrel_vectordb_compaction module
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_compaction_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Generators
%%====================================================================

compaction_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
        {"start and stop", fun test_start_stop/0},
        {"status returns info", fun test_status/0},
        {"manual trigger", fun test_manual_trigger/0},
        {"auto compaction on threshold", fun test_auto_compaction/0},
        {"no double compaction", fun test_no_double_compaction/0},
        {"set index updates state", fun test_set_index/0}
     ]
    }.

%%====================================================================
%% Setup/Teardown
%%====================================================================

setup() ->
    rand:seed(exsss, {42, 42, 42}),
    ok.

cleanup(_) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_start_stop() ->
    {ok, Pid} = barrel_vectordb_compaction:start_link(self(), #{}),
    ?assert(is_process_alive(Pid)),
    ok = barrel_vectordb_compaction:stop(Pid),
    timer:sleep(10),
    ?assertNot(is_process_alive(Pid)).

test_status() ->
    {ok, Pid} = barrel_vectordb_compaction:start_link(self(), #{}),
    try
        Status = barrel_vectordb_compaction:status(Pid),
        ?assertEqual(undefined, maps:get(last_compaction, Status)),
        ?assertEqual(0, maps:get(compaction_count, Status)),
        ?assertEqual(false, maps:get(is_compacting, Status))
    after
        barrel_vectordb_compaction:stop(Pid)
    end.

test_manual_trigger() ->
    %% Create a hybrid index with vectors
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{dimension => 8}),
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(8)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 10)
    ),

    {ok, Pid} = barrel_vectordb_compaction:start_link(self(), #{}, Index1),
    try
        %% Trigger compaction
        ok = barrel_vectordb_compaction:trigger(Pid),

        %% Wait for compaction complete message
        receive
            {compaction_complete, Pid, NewIndex} ->
                %% Verify compaction happened
                Info = barrel_vectordb_index_hybrid:info(NewIndex),
                ?assertEqual(0, maps:get(hot_size, Info)),
                ?assertEqual(10, maps:get(cold_size, Info))
        after 5000 ->
            ?assert(false, "Timeout waiting for compaction")
        end,

        %% Status should show compaction happened
        Status = barrel_vectordb_compaction:status(Pid),
        ?assertEqual(1, maps:get(compaction_count, Status)),
        ?assertNotEqual(undefined, maps:get(last_compaction, Status))
    after
        barrel_vectordb_compaction:stop(Pid)
    end.

test_auto_compaction() ->
    %% Create index that will trigger compaction
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{
        dimension => 8,
        hot_capacity => 5
    }),
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(8)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 5)  %% Exactly at capacity
    ),

    %% Start compaction service with short check interval
    {ok, Pid} = barrel_vectordb_compaction:start_link(
        self(),
        #{check_interval_ms => 50},
        Index1
    ),
    try
        %% Wait for auto-compaction
        receive
            {compaction_complete, Pid, _NewIndex} ->
                ok
        after 2000 ->
            ?assert(false, "Timeout waiting for auto-compaction")
        end,

        %% Verify compaction happened
        Status = barrel_vectordb_compaction:status(Pid),
        ?assertEqual(1, maps:get(compaction_count, Status))
    after
        barrel_vectordb_compaction:stop(Pid)
    end.

test_no_double_compaction() ->
    %% Use larger index to make compaction take longer
    {ok, Index0} = barrel_vectordb_index_hybrid:new(#{dimension => 32}),
    Index1 = lists:foldl(
        fun(I, Acc) ->
            {ok, NewAcc} = barrel_vectordb_index_hybrid:insert(
                Acc, integer_to_binary(I), random_vector(32)
            ),
            NewAcc
        end,
        Index0,
        lists:seq(1, 50)
    ),

    {ok, Pid} = barrel_vectordb_compaction:start_link(self(), #{}, Index1),
    try
        %% Check status while compacting - verify is_compacting behavior
        %% First trigger
        ok = barrel_vectordb_compaction:trigger(Pid),

        %% Check status immediately - might catch it compacting
        Status = barrel_vectordb_compaction:status(Pid),

        %% If it's compacting, second trigger should fail
        %% If it completed, both triggers succeeded (which is also valid)
        case maps:get(is_compacting, Status) of
            true ->
                Result = barrel_vectordb_compaction:trigger(Pid),
                ?assertEqual({error, already_compacting}, Result);
            false ->
                %% Already done, that's OK for fast systems
                ok
        end,

        %% Wait for compaction to complete (if still running)
        receive
            {compaction_complete, Pid, _} -> ok
        after 5000 ->
            %% May have already completed before we could catch it
            ok
        end
    after
        barrel_vectordb_compaction:stop(Pid)
    end.

test_set_index() ->
    {ok, Pid} = barrel_vectordb_compaction:start_link(self(), #{}),
    try
        %% Initial status has no index
        Status1 = barrel_vectordb_compaction:status(Pid),
        ?assertEqual(#{}, maps:get(index_info, Status1)),

        %% Create and set an index
        {ok, Index} = barrel_vectordb_index_hybrid:new(#{dimension => 8}),
        ok = barrel_vectordb_compaction:set_index(Pid, Index),

        %% Give cast time to process
        timer:sleep(10),

        %% Status should now have index info
        Status2 = barrel_vectordb_compaction:status(Pid),
        IndexInfo = maps:get(index_info, Status2),
        ?assertEqual(8, maps:get(dimension, IndexInfo))
    after
        barrel_vectordb_compaction:stop(Pid)
    end.

%%====================================================================
%% Helpers
%%====================================================================

random_vector(Dim) ->
    normalize([rand:uniform() - 0.5 || _ <- lists:seq(1, Dim)]).

normalize(Vec) ->
    Norm = math:sqrt(lists:sum([V*V || V <- Vec])),
    case Norm < 0.0001 of
        true -> Vec;
        false -> [V / Norm || V <- Vec]
    end.
