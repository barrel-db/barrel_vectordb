%% @doc Embeddable HTTP routes for barrel_vectordb.
%%
%% Provides separate route groups for flexible embedding:
%% - routes/0,1: All routes combined
%% - cluster_routes/0,1: Cluster status endpoints only
%% - collection_routes/0,1: Collection/document/search endpoints only
%%
%% == Usage (embedding in barrel_memory) ==
%% ```
%% %% Embed all routes
%% VectorRoutes = barrel_vectordb_http_routes:routes(),
%%
%% %% Or embed selectively
%% ClusterRoutes = barrel_vectordb_http_routes:cluster_routes(),
%% CollectionRoutes = barrel_vectordb_http_routes:collection_routes(),
%% '''
%%
%% @end
-module(barrel_vectordb_http_routes).

-export([routes/0, routes/1]).
-export([cluster_routes/0, cluster_routes/1]).
-export([collection_routes/0, collection_routes/1]).

%% @doc Get all routes with default /vectordb prefix
-spec routes() -> list().
routes() ->
    routes(<<"/vectordb">>).

%% @doc Get all routes with custom prefix
-spec routes(binary()) -> list().
routes(Prefix) ->
    cluster_routes(Prefix) ++ collection_routes(Prefix).

%% @doc Get cluster status routes with default /vectordb prefix
-spec cluster_routes() -> list().
cluster_routes() ->
    cluster_routes(<<"/vectordb">>).

%% @doc Get cluster status routes with custom prefix
-spec cluster_routes(binary()) -> list().
cluster_routes(Prefix) ->
    [
        {<<Prefix/binary, "/cluster/status">>, barrel_vectordb_http_handlers, #{action => cluster_status}},
        {<<Prefix/binary, "/cluster/nodes">>, barrel_vectordb_http_handlers, #{action => cluster_nodes}},
        {<<Prefix/binary, "/cluster/leave">>, barrel_vectordb_http_handlers, #{action => cluster_leave}}
    ].

%% @doc Get collection routes with default /vectordb prefix
-spec collection_routes() -> list().
collection_routes() ->
    collection_routes(<<"/vectordb">>).

%% @doc Get collection routes with custom prefix
-spec collection_routes(binary()) -> list().
collection_routes(Prefix) ->
    [
        %% Collection management
        {<<Prefix/binary, "/collections">>, barrel_vectordb_http_handlers, #{action => list_collections}},
        {<<Prefix/binary, "/collections/:collection">>, barrel_vectordb_http_handlers, #{action => collection}},
        {<<Prefix/binary, "/collections/:collection/reshard">>, barrel_vectordb_http_handlers, #{action => reshard}},

        %% Document operations
        {<<Prefix/binary, "/collections/:collection/docs">>, barrel_vectordb_http_handlers, #{action => docs}},
        {<<Prefix/binary, "/collections/:collection/docs/:id">>, barrel_vectordb_http_handlers, #{action => doc}},

        %% Search
        {<<Prefix/binary, "/collections/:collection/search">>, barrel_vectordb_http_handlers, #{action => search}}
    ].
