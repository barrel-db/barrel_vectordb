%%%-------------------------------------------------------------------
%%% @doc Unit tests for barrel_vectordb_python_queue
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_python_queue_tests).

-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Test Fixtures
%%====================================================================

setup() ->
    %% Clean up any existing table
    catch ets:delete(barrel_vectordb_python_queue),
    barrel_vectordb_python_queue:init().

cleanup(_) ->
    catch ets:delete(barrel_vectordb_python_queue),
    ok.

%%====================================================================
%% Tests
%%====================================================================

init_test() ->
    catch ets:delete(barrel_vectordb_python_queue),
    ?assertEqual(ok, barrel_vectordb_python_queue:init()),
    ?assertEqual(0, barrel_vectordb_python_queue:current()).

init_idempotent_test() ->
    catch ets:delete(barrel_vectordb_python_queue),
    ?assertEqual(ok, barrel_vectordb_python_queue:init()),
    ?assertEqual(ok, barrel_vectordb_python_queue:init()),
    ?assertEqual(0, barrel_vectordb_python_queue:current()).

acquire_release_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun(_) ->
         [
          ?_assertEqual(ok, barrel_vectordb_python_queue:acquire(1000)),
          ?_assertEqual(1, barrel_vectordb_python_queue:current()),
          ?_assertEqual(ok, barrel_vectordb_python_queue:release()),
          ?_assertEqual(0, barrel_vectordb_python_queue:current())
         ]
     end}.

max_concurrent_default_test() ->
    %% Default should be schedulers div 2 + 1, minimum 1
    Max = barrel_vectordb_python_queue:max_concurrent(),
    Expected = max(1, erlang:system_info(schedulers) div 2 + 1),
    ?assertEqual(Expected, Max).

max_concurrent_configured_test() ->
    OldVal = application:get_env(barrel_vectordb, python_max_concurrent),
    try
        application:set_env(barrel_vectordb, python_max_concurrent, 42),
        ?assertEqual(42, barrel_vectordb_python_queue:max_concurrent())
    after
        case OldVal of
            undefined -> application:unset_env(barrel_vectordb, python_max_concurrent);
            {ok, V} -> application:set_env(barrel_vectordb, python_max_concurrent, V)
        end
    end.

current_no_table_test() ->
    catch ets:delete(barrel_vectordb_python_queue),
    ?assertEqual(0, barrel_vectordb_python_queue:current()).

concurrency_limit_test_() ->
    {timeout, 10,
     fun() ->
         catch ets:delete(barrel_vectordb_python_queue),
         barrel_vectordb_python_queue:init(),

         %% Set a low limit for testing
         OldVal = application:get_env(barrel_vectordb, python_max_concurrent),
         application:set_env(barrel_vectordb, python_max_concurrent, 2),

         try
             %% Acquire 2 slots (should succeed)
             ?assertEqual(ok, barrel_vectordb_python_queue:acquire(100)),
             ?assertEqual(ok, barrel_vectordb_python_queue:acquire(100)),
             ?assertEqual(2, barrel_vectordb_python_queue:current()),

             %% Third acquire should timeout
             ?assertEqual({error, timeout}, barrel_vectordb_python_queue:acquire(50)),

             %% Release one and try again
             ?assertEqual(ok, barrel_vectordb_python_queue:release()),
             ?assertEqual(1, barrel_vectordb_python_queue:current()),
             ?assertEqual(ok, barrel_vectordb_python_queue:acquire(100)),
             ?assertEqual(2, barrel_vectordb_python_queue:current()),

             %% Cleanup
             barrel_vectordb_python_queue:release(),
             barrel_vectordb_python_queue:release()
         after
             case OldVal of
                 undefined -> application:unset_env(barrel_vectordb, python_max_concurrent);
                 {ok, V} -> application:set_env(barrel_vectordb, python_max_concurrent, V)
             end
         end
     end}.

concurrent_acquire_test_() ->
    {timeout, 10,
     fun() ->
         catch ets:delete(barrel_vectordb_python_queue),
         barrel_vectordb_python_queue:init(),

         OldVal = application:get_env(barrel_vectordb, python_max_concurrent),
         application:set_env(barrel_vectordb, python_max_concurrent, 3),

         try
             Parent = self(),

             %% Spawn 5 processes trying to acquire
             Pids = [spawn_link(fun() ->
                 Result = barrel_vectordb_python_queue:acquire(500),
                 Parent ! {self(), acquired, Result},
                 receive release -> ok end,
                 barrel_vectordb_python_queue:release(),
                 Parent ! {self(), released}
             end) || _ <- lists:seq(1, 5)],

             %% Wait for acquire results
             timer:sleep(100),

             %% Should have exactly 3 acquired (the limit)
             ?assertEqual(3, barrel_vectordb_python_queue:current()),

             %% Release all
             [Pid ! release || Pid <- Pids],

             %% Wait for all to finish
             timer:sleep(200),
             ?assertEqual(0, barrel_vectordb_python_queue:current())
         after
             case OldVal of
                 undefined -> application:unset_env(barrel_vectordb, python_max_concurrent);
                 {ok, V} -> application:set_env(barrel_vectordb, python_max_concurrent, V)
             end
         end
     end}.

infinity_timeout_test_() ->
    {timeout, 5,
     fun() ->
         catch ets:delete(barrel_vectordb_python_queue),
         barrel_vectordb_python_queue:init(),

         OldVal = application:get_env(barrel_vectordb, python_max_concurrent),
         application:set_env(barrel_vectordb, python_max_concurrent, 1),

         try
             %% Acquire the only slot
             ?assertEqual(ok, barrel_vectordb_python_queue:acquire(100)),

             %% Spawn a process that will wait with infinity timeout
             Parent = self(),
             Pid = spawn_link(fun() ->
                 Parent ! waiting,
                 Result = barrel_vectordb_python_queue:acquire(infinity),
                 Parent ! {acquired, Result}
             end),

             %% Wait for it to start waiting
             receive waiting -> ok end,
             timer:sleep(50),

             %% Still should be waiting (count still 1)
             ?assertEqual(1, barrel_vectordb_python_queue:current()),

             %% Release our slot
             barrel_vectordb_python_queue:release(),

             %% The waiting process should now acquire
             receive
                 {acquired, Result} -> ?assertEqual(ok, Result)
             after 500 ->
                 exit(Pid, kill),
                 ?assert(false)
             end
         after
             case OldVal of
                 undefined -> application:unset_env(barrel_vectordb, python_max_concurrent);
                 {ok, V} -> application:set_env(barrel_vectordb, python_max_concurrent, V)
             end
         end
     end}.
