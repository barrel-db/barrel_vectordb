%% @doc Embeddable HTTP routes for barrel_vectordb cluster.
%%
%% Returns cowboy route specs that can be mounted by a parent application
%% (like barrel_memory) to expose vector database endpoints.
%%
%% == Usage (embedding in barrel_memory) ==
%% ```
%% %% In barrel_memory_http.erl
%% routes() ->
%%     MemoryRoutes = barrel_memory_http_routes:routes(),
%%     VectorRoutes = barrel_vectordb_http_routes:routes(),
%%     MemoryRoutes ++ VectorRoutes.
%% '''
%%
%% @end
-module(barrel_vectordb_http_routes).

-export([routes/0, routes/1]).

%% @doc Get default routes with /vectordb prefix
-spec routes() -> list().
routes() ->
    routes(<<"/vectordb">>).

%% @doc Get routes with custom prefix
-spec routes(binary()) -> list().
routes(Prefix) ->
    [
        %% Collection management
        {<<Prefix/binary, "/collections">>, barrel_vectordb_http_handlers, #{action => list_collections}},
        {<<Prefix/binary, "/collections/:collection">>, barrel_vectordb_http_handlers, #{action => collection}},

        %% Document operations
        {<<Prefix/binary, "/collections/:collection/docs">>, barrel_vectordb_http_handlers, #{action => docs}},
        {<<Prefix/binary, "/collections/:collection/docs/:id">>, barrel_vectordb_http_handlers, #{action => doc}},

        %% Search
        {<<Prefix/binary, "/collections/:collection/search">>, barrel_vectordb_http_handlers, #{action => search}},

        %% Cluster status
        {<<Prefix/binary, "/cluster/status">>, barrel_vectordb_http_handlers, #{action => cluster_status}},
        {<<Prefix/binary, "/cluster/nodes">>, barrel_vectordb_http_handlers, #{action => cluster_nodes}}
    ].
