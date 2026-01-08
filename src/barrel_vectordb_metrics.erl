%% @doc Prometheus metrics for barrel_vectordb.
%%
%% Registers metrics that can be scraped via the host app's /metrics endpoint.
%% No HTTP endpoint is provided here - barrel_memory exposes /metrics.
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

%% @doc Setup/register all metrics. Call once at startup.
setup() ->
    %% Only setup if prometheus is available
    case code:which(prometheus_gauge) of
        non_existing ->
            ok;
        _ ->
            setup_metrics()
    end.

setup_metrics() ->
    %% Gauges
    prometheus_gauge:new([
        {name, barrel_vectordb_documents_total},
        {help, "Total number of documents in collection"},
        {labels, [collection]}
    ]),
    prometheus_gauge:new([
        {name, barrel_vectordb_cluster_nodes},
        {help, "Number of nodes in the cluster"}
    ]),
    prometheus_gauge:new([
        {name, barrel_vectordb_cluster_shards},
        {help, "Number of shards for collection"},
        {labels, [collection]}
    ]),

    %% Histograms
    prometheus_histogram:new([
        {name, barrel_vectordb_search_duration_seconds},
        {help, "Search operation duration in seconds"},
        {labels, [collection]},
        {buckets, [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]}
    ]),
    prometheus_histogram:new([
        {name, barrel_vectordb_add_duration_seconds},
        {help, "Add operation duration in seconds"},
        {labels, [collection]},
        {buckets, [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]}
    ]),
    ok.

%% @doc Observe search duration
observe_search(Collection, DurationMs) when is_binary(Collection) ->
    case code:which(prometheus_histogram) of
        non_existing -> ok;
        _ ->
            prometheus_histogram:observe(
                barrel_vectordb_search_duration_seconds,
                [Collection],
                DurationMs / 1000)
    end.

%% @doc Observe add duration
observe_add(Collection, DurationMs) when is_binary(Collection) ->
    case code:which(prometheus_histogram) of
        non_existing -> ok;
        _ ->
            prometheus_histogram:observe(
                barrel_vectordb_add_duration_seconds,
                [Collection],
                DurationMs / 1000)
    end.

%% @doc Set document count for collection
set_documents(Collection, Count) when is_binary(Collection), is_integer(Count) ->
    case code:which(prometheus_gauge) of
        non_existing -> ok;
        _ ->
            prometheus_gauge:set(
                barrel_vectordb_documents_total,
                [Collection],
                Count)
    end.

%% @doc Set cluster node count
set_cluster_nodes(Count) when is_integer(Count) ->
    case code:which(prometheus_gauge) of
        non_existing -> ok;
        _ ->
            prometheus_gauge:set(barrel_vectordb_cluster_nodes, Count)
    end.

%% @doc Set shard count for collection
set_shards(Collection, Count) when is_binary(Collection), is_integer(Count) ->
    case code:which(prometheus_gauge) of
        non_existing -> ok;
        _ ->
            prometheus_gauge:set(
                barrel_vectordb_cluster_shards,
                [Collection],
                Count)
    end.
