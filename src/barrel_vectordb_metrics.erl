%% @doc ETS-based metrics for barrel_vectordb.
%%
%% Stores metrics internally without external dependencies.
%% Can export to prometheus text format on demand.
%%
%% Metrics:
%% - barrel_vectordb_documents_total: Total documents (gauge, per collection)
%% - barrel_vectordb_search_duration_seconds: Search latency histogram
%% - barrel_vectordb_add_duration_seconds: Add latency histogram
%% - barrel_vectordb_cluster_nodes: Number of cluster nodes (gauge)
%% - barrel_vectordb_cluster_shards: Number of shards (gauge, per collection)
%%
%% @end
-module(barrel_vectordb_metrics).

-export([setup/0]).
-export([observe_search/2, observe_add/2]).
-export([set_documents/2, set_cluster_nodes/1, set_shards/2]).
-export([get_metrics/0, to_prometheus/0]).

-define(METRICS_TABLE, barrel_vectordb_metrics).
-define(HISTOGRAM_TABLE, barrel_vectordb_histograms).

%% Histogram bucket boundaries (seconds)
-define(SEARCH_BUCKETS, [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]).
-define(ADD_BUCKETS, [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]).

%% @doc Setup ETS tables for metrics. Call once at startup.
setup() ->
    _ = case ets:whereis(?METRICS_TABLE) of
        undefined ->
            _ = ets:new(?METRICS_TABLE, [named_table, public, set]),
            _ = ets:new(?HISTOGRAM_TABLE, [named_table, public, set]);
        _ ->
            ok
    end,
    ok.

%% @doc Set document count for collection
set_documents(Collection, Count) when is_binary(Collection), is_integer(Count) ->
    ensure_table(),
    ets:insert(?METRICS_TABLE, {{documents, Collection}, Count}).

%% @doc Set cluster node count
set_cluster_nodes(Count) when is_integer(Count) ->
    ensure_table(),
    ets:insert(?METRICS_TABLE, {cluster_nodes, Count}).

%% @doc Set shard count for collection
set_shards(Collection, Count) when is_binary(Collection), is_integer(Count) ->
    ensure_table(),
    ets:insert(?METRICS_TABLE, {{shards, Collection}, Count}).

%% @doc Observe search duration
observe_search(Collection, DurationMs) when is_binary(Collection) ->
    ensure_table(),
    observe_histogram(search, Collection, DurationMs / 1000, ?SEARCH_BUCKETS).

%% @doc Observe add duration
observe_add(Collection, DurationMs) when is_binary(Collection) ->
    ensure_table(),
    observe_histogram(add, Collection, DurationMs / 1000, ?ADD_BUCKETS).

%% @doc Export metrics as map for introspection
get_metrics() ->
    ensure_table(),
    Gauges = ets:tab2list(?METRICS_TABLE),
    Histograms = ets:tab2list(?HISTOGRAM_TABLE),
    #{gauges => maps:from_list(Gauges),
      histograms => group_histograms(Histograms)}.

%% @doc Export metrics in prometheus text exposition format
to_prometheus() ->
    ensure_table(),
    Gauges = format_gauges(),
    Histograms = format_histograms(),
    iolist_to_binary([Gauges, Histograms]).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ensure_table() ->
    case ets:whereis(?METRICS_TABLE) of
        undefined -> setup();
        _ -> ok
    end.

observe_histogram(Type, Collection, Value, Buckets) ->
    Key = {Type, Collection},
    %% Increment +Inf counter (total count)
    _ = ets:update_counter(?HISTOGRAM_TABLE, {Key, inf}, 1, {{Key, inf}, 0}),
    %% Accumulate sum (stored as microseconds to avoid float issues)
    _ = ets:update_counter(?HISTOGRAM_TABLE, {Key, sum}, round(Value * 1000000), {{Key, sum}, 0}),
    %% Increment matching bucket counters
    _ = lists:foreach(fun(B) ->
        case Value =< B of
            true -> ets:update_counter(?HISTOGRAM_TABLE, {Key, B}, 1, {{Key, B}, 0});
            false -> ok
        end
    end, Buckets),
    ok.

group_histograms(Histograms) ->
    lists:foldl(fun({{Key, Bucket}, Count}, Acc) ->
        KeyMap = maps:get(Key, Acc, #{}),
        maps:put(Key, maps:put(Bucket, Count, KeyMap), Acc)
    end, #{}, Histograms).

format_gauges() ->
    Gauges = ets:tab2list(?METRICS_TABLE),
    DocumentsHelp = <<"# HELP barrel_vectordb_documents_total Total documents in collection\n"
                      "# TYPE barrel_vectordb_documents_total gauge\n">>,
    NodesHelp = <<"# HELP barrel_vectordb_cluster_nodes Number of cluster nodes\n"
                  "# TYPE barrel_vectordb_cluster_nodes gauge\n">>,
    ShardsHelp = <<"# HELP barrel_vectordb_cluster_shards Number of shards per collection\n"
                   "# TYPE barrel_vectordb_cluster_shards gauge\n">>,

    {Docs, Nodes, Shards} = lists:foldl(fun
        ({{documents, Collection}, Count}, {D, N, S}) ->
            Line = io_lib:format("barrel_vectordb_documents_total{collection=\"~s\"} ~B~n",
                                 [Collection, Count]),
            {[Line | D], N, S};
        ({cluster_nodes, Count}, {D, N, S}) ->
            Line = io_lib:format("barrel_vectordb_cluster_nodes ~B~n", [Count]),
            {D, [Line | N], S};
        ({{shards, Collection}, Count}, {D, N, S}) ->
            Line = io_lib:format("barrel_vectordb_cluster_shards{collection=\"~s\"} ~B~n",
                                 [Collection, Count]),
            {D, N, [Line | S]}
    end, {[], [], []}, Gauges),

    [case Docs of [] -> <<>>; _ -> [DocumentsHelp | lists:reverse(Docs)] end,
     case Nodes of [] -> <<>>; _ -> [NodesHelp | lists:reverse(Nodes)] end,
     case Shards of [] -> <<>>; _ -> [ShardsHelp | lists:reverse(Shards)] end].

format_histograms() ->
    Histograms = group_histograms(ets:tab2list(?HISTOGRAM_TABLE)),
    SearchHelp = <<"# HELP barrel_vectordb_search_duration_seconds Search latency\n"
                   "# TYPE barrel_vectordb_search_duration_seconds histogram\n">>,
    AddHelp = <<"# HELP barrel_vectordb_add_duration_seconds Add latency\n"
                "# TYPE barrel_vectordb_add_duration_seconds histogram\n">>,

    {SearchLines, AddLines} = maps:fold(fun({Type, Collection}, BucketMap, {Search, Add}) ->
        Buckets = case Type of
            search -> ?SEARCH_BUCKETS;
            add -> ?ADD_BUCKETS
        end,
        Name = case Type of
            search -> "barrel_vectordb_search_duration_seconds";
            add -> "barrel_vectordb_add_duration_seconds"
        end,
        Lines = format_histogram_buckets(Name, Collection, Buckets, BucketMap),
        case Type of
            search -> {[Lines | Search], Add};
            add -> {Search, [Lines | Add]}
        end
    end, {[], []}, Histograms),

    [case SearchLines of [] -> <<>>; _ -> [SearchHelp | lists:reverse(SearchLines)] end,
     case AddLines of [] -> <<>>; _ -> [AddHelp | lists:reverse(AddLines)] end].

format_histogram_buckets(Name, Collection, Buckets, BucketMap) ->
    BucketLines = lists:map(fun(B) ->
        Count = maps:get(B, BucketMap, 0),
        io_lib:format("~s_bucket{collection=\"~s\",le=\"~.3f\"} ~B~n",
                      [Name, Collection, B, Count])
    end, Buckets),
    InfCount = maps:get(inf, BucketMap, 0),
    SumMicros = maps:get(sum, BucketMap, 0),
    InfLine = io_lib:format("~s_bucket{collection=\"~s\",le=\"+Inf\"} ~B~n",
                            [Name, Collection, InfCount]),
    SumLine = io_lib:format("~s_sum{collection=\"~s\"} ~.6f~n",
                            [Name, Collection, SumMicros / 1000000]),
    CountLine = io_lib:format("~s_count{collection=\"~s\"} ~B~n",
                              [Name, Collection, InfCount]),
    [BucketLines, InfLine, SumLine, CountLine].
