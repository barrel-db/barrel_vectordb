%% @doc HTTP handlers for barrel_vectordb API.
%%
%% Works in both clustered and standalone modes:
%% - Clustered: routes to appropriate shards
%% - Standalone: uses local store with collection name as store
%%
%% @end
-module(barrel_vectordb_http_handlers).
-behaviour(cowboy_handler).

-export([init/2]).

%% Cowboy handler callback
init(Req0, State) ->
    Action = maps:get(action, State),
    Method = cowboy_req:method(Req0),
    IsClustered = barrel_vectordb:is_clustered(),
    handle(Action, Method, IsClustered, Req0, State).

%% Collection management

handle(list_collections, <<"GET">>, true, Req0, State) ->
    case barrel_vectordb:list_collections() of
        {ok, Collections} when is_map(Collections) ->
            %% Convert collection records to JSON-serializable maps
            JsonCollections = maps:map(fun(_, Meta) -> format_collection_meta(Meta) end, Collections),
            json_response(200, JsonCollections, Req0, State);
        {ok, Collections} ->
            json_response(200, Collections, Req0, State);
        {error, Reason} ->
            json_error(500, <<"error">>, format_error(Reason), Req0, State)
    end;
handle(list_collections, <<"GET">>, false, Req0, State) ->
    json_response(200, #{}, Req0, State);

handle(collection, <<"GET">>, IsClustered, Req0, State) ->
    Collection = cowboy_req:binding(collection, Req0),
    case maybe_get_collection(IsClustered, Collection) of
        {ok, Meta} ->
            JsonMeta = format_collection_meta(Meta),
            json_response(200, JsonMeta, Req0, State);
        {error, not_found} ->
            json_error(404, <<"not_found">>, <<"Collection not found">>, Req0, State);
        {error, Reason} ->
            json_error(500, <<"error">>, format_error(Reason), Req0, State)
    end;

handle(collection, <<"PUT">>, IsClustered, Req0, State) ->
    Collection = cowboy_req:binding(collection, Req0),
    case read_json_body(Req0) of
        {ok, Body, Req1} ->
            case maybe_create_collection(IsClustered, Collection, Body) of
                ok ->
                    json_response(201, #{status => <<"created">>}, Req1, State);
                {ok, _Meta} ->
                    %% Clustered mode returns {ok, Meta}
                    json_response(201, #{status => <<"created">>}, Req1, State);
                {error, already_exists} ->
                    json_error(409, <<"already_exists">>, <<"Collection already exists">>, Req1, State);
                {error, Reason} ->
                    json_error(500, <<"error">>, format_error(Reason), Req1, State)
            end;
        {error, Req1} ->
            json_error(400, <<"bad_request">>, <<"Invalid JSON body">>, Req1, State)
    end;

handle(collection, <<"DELETE">>, IsClustered, Req0, State) ->
    Collection = cowboy_req:binding(collection, Req0),
    case maybe_delete_collection(IsClustered, Collection) of
        ok ->
            json_response(200, #{status => <<"deleted">>}, Req0, State);
        {error, not_found} ->
            json_error(404, <<"not_found">>, <<"Collection not found">>, Req0, State);
        {error, Reason} ->
            json_error(500, <<"error">>, format_error(Reason), Req0, State)
    end;

%% Document operations

handle(docs, <<"POST">>, IsClustered, Req0, State) ->
    Collection = cowboy_req:binding(collection, Req0),
    case read_json_body(Req0) of
        {ok, Body, Req1} ->
            Id = maps:get(<<"id">>, Body),
            Text = maps:get(<<"text">>, Body, <<>>),
            Metadata = maps:get(<<"metadata">>, Body, #{}),
            Vector = maps:get(<<"vector">>, Body, undefined),
            Result = case Vector of
                undefined ->
                    maybe_add(IsClustered, Collection, Id, Text, Metadata);
                _ ->
                    maybe_add_vector(IsClustered, Collection, Id, Text, Metadata, Vector)
            end,
            case Result of
                ok ->
                    json_response(201, #{status => <<"created">>, id => Id}, Req1, State);
                {error, Reason} ->
                    json_error(500, <<"error">>, format_error(Reason), Req1, State)
            end;
        {error, Req1} ->
            json_error(400, <<"bad_request">>, <<"Invalid JSON body">>, Req1, State)
    end;

handle(doc, <<"GET">>, IsClustered, Req0, State) ->
    Collection = cowboy_req:binding(collection, Req0),
    Id = cowboy_req:binding(id, Req0),
    case maybe_get(IsClustered, Collection, Id) of
        {ok, Doc} ->
            json_response(200, Doc, Req0, State);
        not_found ->
            json_error(404, <<"not_found">>, <<"Document not found">>, Req0, State);
        {error, Reason} ->
            json_error(500, <<"error">>, format_error(Reason), Req0, State)
    end;

handle(doc, <<"DELETE">>, IsClustered, Req0, State) ->
    Collection = cowboy_req:binding(collection, Req0),
    Id = cowboy_req:binding(id, Req0),
    case maybe_delete(IsClustered, Collection, Id) of
        ok ->
            json_response(200, #{status => <<"deleted">>}, Req0, State);
        {error, Reason} ->
            json_error(500, <<"error">>, format_error(Reason), Req0, State)
    end;

%% Search

handle(search, <<"POST">>, IsClustered, Req0, State) ->
    Collection = cowboy_req:binding(collection, Req0),
    case read_json_body(Req0) of
        {ok, Body, Req1} ->
            Query = maps:get(<<"query">>, Body, undefined),
            Vector = maps:get(<<"vector">>, Body, undefined),
            K = maps:get(<<"k">>, Body, 10),
            Opts = #{k => K},
            Result = case {Query, Vector} of
                {undefined, undefined} ->
                    {error, missing_query};
                {_, VectorVal} when is_list(VectorVal) ->
                    maybe_search_vector(IsClustered, Collection, VectorVal, Opts);
                {QueryVal, _} when is_binary(QueryVal) ->
                    maybe_search(IsClustered, Collection, QueryVal, Opts)
            end,
            case Result of
                {ok, Results} ->
                    json_response(200, #{results => Results}, Req1, State);
                {error, missing_query} ->
                    json_error(400, <<"bad_request">>, <<"Either 'query' or 'vector' required">>, Req1, State);
                {error, Reason} ->
                    json_error(500, <<"error">>, format_error(Reason), Req1, State)
            end;
        {error, Req1} ->
            json_error(400, <<"bad_request">>, <<"Invalid JSON body">>, Req1, State)
    end;

%% Cluster status

handle(cluster_status, <<"GET">>, _IsClustered, Req0, State) ->
    Status = barrel_vectordb:cluster_status(),
    JsonStatus = format_cluster_status(Status),
    json_response(200, JsonStatus, Req0, State);

handle(cluster_nodes, <<"GET">>, _IsClustered, Req0, State) ->
    %% Get nodes from Ra state machine (authoritative source)
    case barrel_vectordb_cluster_client:get_nodes() of
        {ok, Nodes} when is_map(Nodes) ->
            %% Nodes is a map of NodeId => NodeInfo
            NodeList = [format_node_id(NodeId) || NodeId <- maps:keys(Nodes)],
            json_response(200, #{nodes => NodeList}, Req0, State);
        _ ->
            %% Fallback to healthy nodes if not clustered or error
            Nodes = barrel_vectordb:cluster_nodes(),
            NodeList = [atom_to_binary(N, utf8) || N <- Nodes],
            json_response(200, #{nodes => NodeList}, Req0, State)
    end;

%% Method not allowed

handle(_Action, _Method, _IsClustered, Req0, State) ->
    json_error(405, <<"method_not_allowed">>, <<"Method not allowed">>, Req0, State).

%% Mode-aware operations

maybe_get_collection(true, Collection) ->
    barrel_vectordb:get_collection(Collection);
maybe_get_collection(false, Collection) ->
    StoreName = collection_to_store(Collection),
    case whereis(StoreName) of
        undefined -> {error, not_found};
        _Pid -> barrel_vectordb:stats(StoreName)
    end.

maybe_create_collection(true, Collection, Body) ->
    Opts = #{
        dimensions => maps:get(<<"dimensions">>, Body, 768),
        num_shards => maps:get(<<"num_shards">>, Body, 4),
        replication_factor => maps:get(<<"replication_factor">>, Body, 2)
    },
    barrel_vectordb:create_collection(Collection, Opts);
maybe_create_collection(false, Collection, Body) ->
    StoreName = collection_to_store(Collection),
    case whereis(StoreName) of
        undefined ->
            Dimension = maps:get(<<"dimensions">>, Body, 768),
            Path = maps:get(<<"path">>, Body, default_path(Collection)),
            Config = #{name => StoreName, path => Path, dimensions => Dimension},
            case barrel_vectordb:start_link(Config) of
                {ok, _Pid} -> ok;
                {error, _} = Error -> Error
            end;
        _Pid ->
            {error, already_exists}
    end.

maybe_delete_collection(true, Collection) ->
    barrel_vectordb:delete_collection(Collection);
maybe_delete_collection(false, Collection) ->
    StoreName = collection_to_store(Collection),
    case whereis(StoreName) of
        undefined -> {error, not_found};
        _Pid -> barrel_vectordb:stop(StoreName)
    end.

maybe_add(true, Collection, Id, Text, Metadata) ->
    barrel_vectordb:cluster_add(Collection, Id, Text, Metadata);
maybe_add(false, Collection, Id, Text, Metadata) ->
    StoreName = collection_to_store(Collection),
    barrel_vectordb:add(StoreName, Id, Text, Metadata).

maybe_add_vector(true, Collection, Id, Text, Metadata, Vector) ->
    barrel_vectordb:cluster_add_vector(Collection, Id, Text, Metadata, Vector);
maybe_add_vector(false, Collection, Id, Text, Metadata, Vector) ->
    StoreName = collection_to_store(Collection),
    barrel_vectordb:add_vector(StoreName, Id, Text, Metadata, Vector).

maybe_get(true, Collection, Id) ->
    barrel_vectordb:cluster_get(Collection, Id);
maybe_get(false, Collection, Id) ->
    StoreName = collection_to_store(Collection),
    barrel_vectordb:get(StoreName, Id).

maybe_delete(true, Collection, Id) ->
    barrel_vectordb:cluster_delete(Collection, Id);
maybe_delete(false, Collection, Id) ->
    StoreName = collection_to_store(Collection),
    barrel_vectordb:delete(StoreName, Id).

maybe_search(true, Collection, Query, Opts) ->
    barrel_vectordb:cluster_search(Collection, Query, Opts);
maybe_search(false, Collection, Query, Opts) ->
    StoreName = collection_to_store(Collection),
    barrel_vectordb:search(StoreName, Query, Opts).

maybe_search_vector(true, Collection, Vector, Opts) ->
    barrel_vectordb:cluster_search_vector(Collection, Vector, Opts);
maybe_search_vector(false, Collection, Vector, Opts) ->
    StoreName = collection_to_store(Collection),
    barrel_vectordb:search_vector(StoreName, Vector, Opts).

%% Internal helpers

collection_to_store(Collection) when is_binary(Collection) ->
    binary_to_atom(Collection, utf8).

default_path(Collection) when is_binary(Collection) ->
    "priv/barrel_vectordb_" ++ binary_to_list(Collection).

read_json_body(Req0) ->
    case cowboy_req:read_body(Req0) of
        {ok, Body, Req1} ->
            try
                Json = jsx:decode(Body, [return_maps]),
                {ok, Json, Req1}
            catch
                _:_ -> {error, Req1}
            end;
        {more, _, Req1} ->
            {error, Req1}
    end.

json_response(Status, Data, Req0, State) ->
    Body = jsx:encode(Data),
    Req = cowboy_req:reply(Status, #{
        <<"content-type">> => <<"application/json">>
    }, Body, Req0),
    {ok, Req, State}.

json_error(Status, Code, Message, Req0, State) ->
    Body = jsx:encode(#{error => Code, message => Message}),
    Req = cowboy_req:reply(Status, #{
        <<"content-type">> => <<"application/json">>
    }, Body, Req0),
    {ok, Req, State}.

format_error(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason, utf8);
format_error(Reason) when is_binary(Reason) ->
    Reason;
format_error(Reason) ->
    iolist_to_binary(io_lib:format("~p", [Reason])).

%% @private Convert cluster status to JSON-serializable format
format_cluster_status(Status) when is_map(Status) ->
    maps:fold(fun format_cluster_status_field/3, #{}, Status).

format_cluster_status_field(state, Val, Acc) when is_atom(Val) ->
    Acc#{<<"state">> => atom_to_binary(Val, utf8)};
format_cluster_status_field(node, Val, Acc) when is_atom(Val) ->
    Acc#{<<"node">> => atom_to_binary(Val, utf8)};
format_cluster_status_field(node_id, {Name, Node}, Acc) ->
    %% Ra server ID is {ClusterName, Node}
    Acc#{<<"node_id">> => #{
        <<"cluster">> => atom_to_binary(Name, utf8),
        <<"node">> => atom_to_binary(Node, utf8)
    }};
format_cluster_status_field(nodes, Val, Acc) when is_list(Val) ->
    Acc#{<<"nodes">> => [format_node(N) || N <- Val]};
format_cluster_status_field(healthy_nodes, Val, Acc) when is_list(Val) ->
    Acc#{<<"healthy_nodes">> => [format_node(N) || N <- Val]};
format_cluster_status_field(leader, undefined, Acc) ->
    Acc#{<<"leader">> => null};
format_cluster_status_field(leader, {Name, Node}, Acc) when is_atom(Name), is_atom(Node) ->
    %% Ra server ID tuple
    Acc#{<<"leader">> => #{
        <<"cluster">> => atom_to_binary(Name, utf8),
        <<"node">> => atom_to_binary(Node, utf8)
    }};
format_cluster_status_field(leader, Val, Acc) when is_atom(Val) ->
    Acc#{<<"leader">> => atom_to_binary(Val, utf8)};
format_cluster_status_field(is_leader, Val, Acc) when is_boolean(Val) ->
    Acc#{<<"is_leader">> => Val};
format_cluster_status_field(Key, Val, Acc) when is_atom(Key) ->
    %% Fallback for any other fields
    Acc#{atom_to_binary(Key, utf8) => format_json_value(Val)}.

format_node(Node) when is_atom(Node) ->
    atom_to_binary(Node, utf8);
format_node({Name, Node}) when is_atom(Name), is_atom(Node) ->
    #{<<"cluster">> => atom_to_binary(Name, utf8), <<"node">> => atom_to_binary(Node, utf8)}.

%% @private Format NodeId (Ra server ID) to binary for JSON
format_node_id({_ClusterName, Node}) when is_atom(Node) ->
    atom_to_binary(Node, utf8);
format_node_id(Node) when is_atom(Node) ->
    atom_to_binary(Node, utf8).

%% @private Convert collection_meta record to JSON-serializable map
format_collection_meta(Meta) when is_tuple(Meta), element(1, Meta) =:= collection_meta ->
    %% collection_meta record: {collection_meta, Name, Dimension, NumShards, RF, CreatedAt, Status}
    #{
        <<"name">> => element(2, Meta),
        <<"dimension">> => element(3, Meta),
        <<"num_shards">> => element(4, Meta),
        <<"replication_factor">> => element(5, Meta),
        <<"created_at">> => element(6, Meta),
        <<"status">> => atom_to_binary(element(7, Meta), utf8)
    };
format_collection_meta(Meta) when is_map(Meta) ->
    %% Already a map, just ensure atoms are converted
    maps:fold(
        fun(K, V, Acc) when is_atom(K) ->
            Acc#{atom_to_binary(K, utf8) => format_json_value(V)};
           (K, V, Acc) ->
            Acc#{K => format_json_value(V)}
        end, #{}, Meta).

format_json_value(Val) when is_atom(Val) -> atom_to_binary(Val, utf8);
format_json_value(Val) when is_binary(Val) -> Val;
format_json_value(Val) when is_integer(Val) -> Val;
format_json_value(Val) when is_float(Val) -> Val;
format_json_value(Val) when is_boolean(Val) -> Val;
format_json_value(Val) when is_list(Val) -> [format_json_value(V) || V <- Val];
format_json_value(Val) when is_map(Val) -> maps:map(fun(_, V) -> format_json_value(V) end, Val);
format_json_value(Val) -> iolist_to_binary(io_lib:format("~p", [Val])).
