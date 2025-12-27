%%%-------------------------------------------------------------------
%%% @doc Pure Erlang HNSW (Hierarchical Navigable Small World) implementation
%%%
%%% This module implements the HNSW algorithm for approximate nearest
%%% neighbor search. Optimized for small datasets (<10K vectors).
%%%
%%% Reference: https://arxiv.org/abs/1603.09320
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_hnsw).

-include("barrel_vectordb.hrl").

%% API
-export([
    new/0,
    new/1,
    insert/3,
    search/3,
    search/4,
    delete/2,
    size/1,
    info/1,
    get_node/2
]).

%% Serialization
-export([
    serialize/1,
    deserialize/1,
    serialize_node/1,
    deserialize_node/1
]).

%% Distance functions (exported for testing)
-export([
    cosine_distance/2,
    euclidean_distance/2,
    cosine_similarity/2
]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new empty HNSW index with default configuration
-spec new() -> hnsw_index().
new() ->
    new(#{}).

%% @doc Create a new empty HNSW index with custom configuration
-spec new(map()) -> hnsw_index().
new(Options) ->
    M = maps:get(m, Options, 16),
    MMax0 = maps:get(m_max0, Options, M * 2),
    EfConstruction = maps:get(ef_construction, Options, 200),
    DistanceFn = maps:get(distance_fn, Options, cosine),
    Dimension = maps:get(dimension, Options, ?DEFAULT_DIMENSION),

    %% ml = 1 / ln(M) - controls level distribution
    Ml = 1.0 / math:log(M),

    Config = #hnsw_config{
        m = M,
        m_max0 = MMax0,
        ef_construction = EfConstruction,
        ml = Ml,
        distance_fn = DistanceFn
    },

    #hnsw_index{
        entry_point = undefined,
        max_layer = 0,
        nodes = #{},
        config = Config,
        size = 0,
        dimension = Dimension
    }.

%% @doc Insert a vector with given ID into the index
-spec insert(hnsw_index(), binary(), [float()]) -> hnsw_index().
insert(#hnsw_index{entry_point = undefined, config = Config, dimension = Dim} = Index,
       Id, Vector) when length(Vector) =:= Dim ->
    %% First node - becomes entry point
    NodeLayer = random_layer(Config#hnsw_config.ml),
    Node = #hnsw_node{
        id = Id,
        vector = Vector,
        layer = NodeLayer,
        neighbors = init_neighbors(NodeLayer)
    },
    Index#hnsw_index{
        entry_point = Id,
        max_layer = NodeLayer,
        nodes = #{Id => Node},
        size = 1
    };
insert(#hnsw_index{dimension = Dim} = Index, Id, Vector) when length(Vector) =:= Dim ->
    insert_node(Index, Id, Vector);
insert(#hnsw_index{dimension = Dim}, _Id, Vector) ->
    error({invalid_dimension, Dim, length(Vector)}).

%% @doc Search for k nearest neighbors
-spec search(hnsw_index(), [float()], pos_integer()) -> [{binary(), float()}].
search(Index, Query, K) ->
    search(Index, Query, K, #{}).

%% @doc Search for k nearest neighbors with options
-spec search(hnsw_index(), [float()], pos_integer(), map()) -> [{binary(), float()}].
search(#hnsw_index{entry_point = undefined}, _Query, _K, _Options) ->
    [];
search(#hnsw_index{entry_point = EP, max_layer = MaxLayer, nodes = Nodes,
                   config = Config} = Index, Query, K, Options) ->
    EfSearch = maps:get(ef_search, Options, max(K, 50)),

    %% Start from entry point
    EPNode = maps:get(EP, Nodes),
    EPDist = distance(Query, EPNode#hnsw_node.vector, Config#hnsw_config.distance_fn),

    %% Traverse from top layer down to layer 1, greedy search
    {CurrentBest, _} = lists:foldl(
        fun(Layer, {BestId, _BestDist}) ->
            search_layer_greedy(Index, Query, BestId, Layer)
        end,
        {EP, EPDist},
        lists:seq(MaxLayer, 1, -1)
    ),

    %% At layer 0, do full ef-search
    Candidates = search_layer(Index, Query, CurrentBest, 0, EfSearch),

    %% Return top K results
    TopK = lists:sublist(Candidates, K),
    [{Id, Dist} || #candidate{id = Id, distance = Dist} <- TopK].

%% @doc Delete a node from the index (marks as deleted, compact later)
-spec delete(hnsw_index(), binary()) -> hnsw_index().
delete(#hnsw_index{nodes = Nodes, size = Size} = Index, Id) ->
    case maps:is_key(Id, Nodes) of
        true ->
            %% Remove from neighbors of all connected nodes
            Node = maps:get(Id, Nodes),
            NewNodes = remove_from_neighbors(maps:remove(Id, Nodes), Id, Node#hnsw_node.neighbors),

            %% Update entry point if needed
            NewIndex = case Index#hnsw_index.entry_point of
                Id ->
                    %% Pick a new entry point
                    case maps:keys(NewNodes) of
                        [] -> Index#hnsw_index{entry_point = undefined, max_layer = 0};
                        [NewEP | _] ->
                            NewEPNode = maps:get(NewEP, NewNodes),
                            Index#hnsw_index{
                                entry_point = NewEP,
                                max_layer = NewEPNode#hnsw_node.layer
                            }
                    end;
                _ -> Index
            end,
            NewIndex#hnsw_index{nodes = NewNodes, size = Size - 1};
        false ->
            Index
    end.

%% @doc Get index size
-spec size(hnsw_index()) -> non_neg_integer().
size(#hnsw_index{size = Size}) -> Size.

%% @doc Get index info
-spec info(hnsw_index()) -> map().
info(#hnsw_index{entry_point = EP, max_layer = MaxLayer, size = Size,
                  config = Config, dimension = Dim}) ->
    #{
        entry_point => EP,
        max_layer => MaxLayer,
        size => Size,
        dimension => Dim,
        config => #{
            m => Config#hnsw_config.m,
            m_max0 => Config#hnsw_config.m_max0,
            ef_construction => Config#hnsw_config.ef_construction,
            distance_fn => Config#hnsw_config.distance_fn
        }
    }.

%% @doc Get a node by ID
-spec get_node(hnsw_index(), binary()) -> {ok, hnsw_node()} | not_found.
get_node(#hnsw_index{nodes = Nodes}, Id) ->
    case maps:find(Id, Nodes) of
        {ok, Node} -> {ok, Node};
        error -> not_found
    end.

%%====================================================================
%% Serialization
%%====================================================================

%% @doc Serialize entire index to binary
-spec serialize(hnsw_index()) -> binary().
serialize(Index) ->
    term_to_binary(Index, [compressed]).

%% @doc Deserialize index from binary
-spec deserialize(binary()) -> {ok, hnsw_index()} | {error, term()}.
deserialize(Binary) ->
    try
        Index = binary_to_term(Binary),
        case is_record(Index, hnsw_index) of
            true -> {ok, Index};
            false -> {error, invalid_index_format}
        end
    catch
        _:Reason -> {error, {deserialization_failed, Reason}}
    end.

%% @doc Serialize a single node (for incremental persistence)
-spec serialize_node(hnsw_node()) -> binary().
serialize_node(#hnsw_node{id = _Id, layer = Layer, neighbors = Neighbors}) ->
    %% Note: vector is stored separately in vectors CF
    NeighborBin = serialize_neighbors(Neighbors),
    NumLayers = maps:size(Neighbors),
    <<?HNSW_NODE_VERSION:8, Layer:8, NumLayers:8, NeighborBin/binary>>.

%% @doc Deserialize a single node
-spec deserialize_node(binary()) -> {ok, map()} | {error, term()}.
deserialize_node(<<?HNSW_NODE_VERSION:8, Layer:8, NumLayers:8, Rest/binary>>) ->
    case deserialize_neighbors(Rest, NumLayers, #{}) of
        {ok, Neighbors, <<>>} ->
            {ok, #{layer => Layer, neighbors => Neighbors}};
        {ok, _, _} ->
            {error, trailing_data};
        {error, _} = Error ->
            Error
    end;
deserialize_node(_) ->
    {error, invalid_node_format}.

%%====================================================================
%% Distance Functions
%%====================================================================

%% @doc Cosine distance (1 - cosine similarity)
-spec cosine_distance([float()], [float()]) -> float().
cosine_distance(Vec1, Vec2) ->
    1.0 - cosine_similarity(Vec1, Vec2).

%% @doc Cosine similarity
-spec cosine_similarity([float()], [float()]) -> float().
cosine_similarity(Vec1, Vec2) ->
    Dot = dot_product(Vec1, Vec2),
    Norm1 = math:sqrt(dot_product(Vec1, Vec1)),
    Norm2 = math:sqrt(dot_product(Vec2, Vec2)),
    case Norm1 * Norm2 of
        +0.0 -> +0.0;
        Denom -> Dot / Denom
    end.

%% @doc Euclidean (L2) distance
-spec euclidean_distance([float()], [float()]) -> float().
euclidean_distance(Vec1, Vec2) ->
    SumSq = lists:foldl(
        fun({A, B}, Acc) ->
            Diff = A - B,
            Acc + Diff * Diff
        end,
        0.0,
        lists:zip(Vec1, Vec2)
    ),
    math:sqrt(SumSq).

%%====================================================================
%% Internal Functions
%%====================================================================

%% Insert a new node into existing index
insert_node(#hnsw_index{entry_point = EP, max_layer = MaxLayer, nodes = Nodes,
                        config = Config, size = Size} = Index, Id, Vector) ->
    %% Determine layer for new node
    NodeLayer = random_layer(Config#hnsw_config.ml),

    %% Get entry point node
    EPNode = maps:get(EP, Nodes),
    EPDist = distance(Vector, EPNode#hnsw_node.vector, Config#hnsw_config.distance_fn),

    %% Phase 1: Traverse from top to insertion layer + 1
    TopLayer = max(MaxLayer, NodeLayer),
    {CurrentBest, _} = lists:foldl(
        fun(Layer, {BestId, _BestDist}) ->
            search_layer_greedy(Index, Vector, BestId, Layer)
        end,
        {EP, EPDist},
        lists:seq(TopLayer, NodeLayer + 1, -1)
    ),

    %% Phase 2: Insert into layers from min(MaxLayer, NodeLayer) down to 0
    InsertLayers = lists:seq(min(MaxLayer, NodeLayer), 0, -1),

    %% Find neighbors at each layer
    {NewNode, UpdatedNodes} = lists:foldl(
        fun(Layer, {AccNode, AccNodes}) ->
            %% Find ef_construction nearest neighbors at this layer
            EfC = Config#hnsw_config.ef_construction,
            Candidates = search_layer(Index#hnsw_index{nodes = AccNodes},
                                      Vector, CurrentBest, Layer, EfC),

            %% Select M best neighbors
            M = case Layer of
                0 -> Config#hnsw_config.m_max0;
                _ -> Config#hnsw_config.m
            end,
            SelectedNeighbors = select_neighbors(Candidates, M, Vector, Config),

            %% Update node's neighbors for this layer
            NewNeighbors = (AccNode#hnsw_node.neighbors)#{Layer => SelectedNeighbors},
            NewAccNode = AccNode#hnsw_node{neighbors = NewNeighbors},

            %% Add bidirectional connections
            UpdatedAccNodes = lists:foldl(
                fun(NeighborId, NodesAcc) ->
                    add_connection(NodesAcc, NeighborId, Id, Layer, M)
                end,
                AccNodes,
                SelectedNeighbors
            ),

            {NewAccNode, UpdatedAccNodes}
        end,
        {#hnsw_node{id = Id, vector = Vector, layer = NodeLayer,
                    neighbors = init_neighbors(NodeLayer)}, Nodes},
        InsertLayers
    ),

    %% Update index
    FinalNodes = UpdatedNodes#{Id => NewNode},
    NewMaxLayer = max(MaxLayer, NodeLayer),
    NewEP = if NodeLayer > MaxLayer -> Id; true -> EP end,

    Index#hnsw_index{
        entry_point = NewEP,
        max_layer = NewMaxLayer,
        nodes = FinalNodes,
        size = Size + 1
    }.

%% Greedy search at a single layer (find single closest node)
search_layer_greedy(#hnsw_index{nodes = Nodes, config = Config}, Query, StartId, Layer) ->
    StartNode = maps:get(StartId, Nodes),
    StartDist = distance(Query, StartNode#hnsw_node.vector, Config#hnsw_config.distance_fn),

    search_layer_greedy_loop(Nodes, Query, StartId, StartDist, Layer, Config).

search_layer_greedy_loop(Nodes, Query, BestId, BestDist, Layer, Config) ->
    Node = maps:get(BestId, Nodes),
    Neighbors = maps:get(Layer, Node#hnsw_node.neighbors, []),

    %% Find best neighbor
    {NewBestId, NewBestDist} = lists:foldl(
        fun(NeighborId, {AccBestId, AccBestDist}) ->
            case maps:find(NeighborId, Nodes) of
                {ok, NeighborNode} ->
                    Dist = distance(Query, NeighborNode#hnsw_node.vector,
                                   Config#hnsw_config.distance_fn),
                    if Dist < AccBestDist -> {NeighborId, Dist};
                       true -> {AccBestId, AccBestDist}
                    end;
                error ->
                    {AccBestId, AccBestDist}
            end
        end,
        {BestId, BestDist},
        Neighbors
    ),

    if NewBestId =:= BestId ->
        %% No improvement, return current best
        {BestId, BestDist};
    true ->
        %% Continue searching
        search_layer_greedy_loop(Nodes, Query, NewBestId, NewBestDist, Layer, Config)
    end.

%% Full ef-search at a layer (returns sorted candidates)
search_layer(#hnsw_index{nodes = Nodes, config = Config}, Query, StartId, Layer, Ef) ->
    StartNode = maps:get(StartId, Nodes),
    StartDist = distance(Query, StartNode#hnsw_node.vector, Config#hnsw_config.distance_fn),
    StartCandidate = #candidate{id = StartId, distance = StartDist},

    %% Initialize with start node
    Visited = sets:from_list([StartId]),
    Candidates = [StartCandidate],  %% min-heap (sorted by distance asc)
    Results = [StartCandidate],     %% max-heap (sorted by distance desc for easy pruning)

    search_layer_loop(Nodes, Query, Layer, Config, Ef, Visited, Candidates, Results).

search_layer_loop(_Nodes, _Query, _Layer, _Config, _Ef, _Visited, [], Results) ->
    %% Sort results by distance ascending
    lists:sort(fun(#candidate{distance = D1}, #candidate{distance = D2}) -> D1 =< D2 end, Results);
search_layer_loop(Nodes, Query, Layer, Config, Ef, Visited, [Current | RestCandidates], Results) ->
    %% Get furthest result distance for pruning
    FurthestDist = case Results of
        [] -> infinity;
        _ ->
            Sorted = lists:sort(fun(#candidate{distance = D1}, #candidate{distance = D2}) ->
                D1 >= D2
            end, Results),
            (hd(Sorted))#candidate.distance
    end,

    %% If current candidate is further than furthest result, we're done
    if Current#candidate.distance > FurthestDist ->
        lists:sort(fun(#candidate{distance = D1}, #candidate{distance = D2}) -> D1 =< D2 end, Results);
    true ->
        %% Explore neighbors
        Node = maps:get(Current#candidate.id, Nodes),
        Neighbors = maps:get(Layer, Node#hnsw_node.neighbors, []),

        {NewVisited, NewCandidates, NewResults} = lists:foldl(
            fun(NeighborId, {VisAcc, CandAcc, ResAcc}) ->
                case sets:is_element(NeighborId, VisAcc) of
                    true -> {VisAcc, CandAcc, ResAcc};
                    false ->
                        NewVisAcc = sets:add_element(NeighborId, VisAcc),
                        case maps:find(NeighborId, Nodes) of
                            {ok, NeighborNode} ->
                                Dist = distance(Query, NeighborNode#hnsw_node.vector,
                                               Config#hnsw_config.distance_fn),
                                Candidate = #candidate{id = NeighborId, distance = Dist},

                                %% Get current furthest in results
                                CurrFurthest = case ResAcc of
                                    [] -> infinity;
                                    _ ->
                                        SortedRes = lists:sort(
                                            fun(#candidate{distance = D1}, #candidate{distance = D2}) ->
                                                D1 >= D2
                                            end, ResAcc),
                                        (hd(SortedRes))#candidate.distance
                                end,

                                if Dist < CurrFurthest orelse length(ResAcc) < Ef ->
                                    %% Add to candidates (sorted insert)
                                    NewCandAcc = insert_sorted(Candidate, CandAcc),
                                    %% Add to results, trim if needed
                                    NewResAcc0 = [Candidate | ResAcc],
                                    NewResAcc = if length(NewResAcc0) > Ef ->
                                        %% Remove furthest
                                        TrimSorted = lists:sort(
                                            fun(#candidate{distance = D1}, #candidate{distance = D2}) ->
                                                D1 =< D2
                                            end, NewResAcc0),
                                        lists:sublist(TrimSorted, Ef);
                                    true ->
                                        NewResAcc0
                                    end,
                                    {NewVisAcc, NewCandAcc, NewResAcc};
                                true ->
                                    {NewVisAcc, CandAcc, ResAcc}
                                end;
                            error ->
                                {NewVisAcc, CandAcc, ResAcc}
                        end
                end
            end,
            {Visited, RestCandidates, Results},
            Neighbors
        ),

        search_layer_loop(Nodes, Query, Layer, Config, Ef, NewVisited, NewCandidates, NewResults)
    end.

%% Insert candidate into sorted list (by distance ascending)
insert_sorted(Candidate, []) -> [Candidate];
insert_sorted(Candidate, [H | T] = List) ->
    if Candidate#candidate.distance =< H#candidate.distance ->
        [Candidate | List];
    true ->
        [H | insert_sorted(Candidate, T)]
    end.

%% Select M neighbors using simple selection
select_neighbors(Candidates, M, _QueryVec, _Config) ->
    %% Simple selection: take M closest
    Selected = lists:sublist(Candidates, M),
    [C#candidate.id || C <- Selected].

%% Add bidirectional connection
add_connection(Nodes, NodeId, NewNeighborId, Layer, MaxM) ->
    case maps:find(NodeId, Nodes) of
        {ok, Node} ->
            CurrentNeighbors = maps:get(Layer, Node#hnsw_node.neighbors, []),
            case lists:member(NewNeighborId, CurrentNeighbors) of
                true -> Nodes;
                false ->
                    NewNeighbors = [NewNeighborId | CurrentNeighbors],
                    %% Prune if too many neighbors
                    PrunedNeighbors = if length(NewNeighbors) > MaxM ->
                        lists:sublist(NewNeighbors, MaxM);
                    true ->
                        NewNeighbors
                    end,
                    UpdatedNode = Node#hnsw_node{
                        neighbors = (Node#hnsw_node.neighbors)#{Layer => PrunedNeighbors}
                    },
                    Nodes#{NodeId => UpdatedNode}
            end;
        error ->
            Nodes
    end.

%% Remove node from all its neighbors' neighbor lists
remove_from_neighbors(Nodes, RemovedId, NeighborsByLayer) ->
    maps:fold(
        fun(_Layer, NeighborIds, AccNodes) ->
            lists:foldl(
                fun(NeighborId, NodesAcc) ->
                    case maps:find(NeighborId, NodesAcc) of
                        {ok, NeighborNode} ->
                            UpdatedNeighbors = maps:map(
                                fun(_L, Ns) ->
                                    lists:delete(RemovedId, Ns)
                                end,
                                NeighborNode#hnsw_node.neighbors
                            ),
                            NodesAcc#{NeighborId => NeighborNode#hnsw_node{neighbors = UpdatedNeighbors}};
                        error ->
                            NodesAcc
                    end
                end,
                AccNodes,
                NeighborIds
            )
        end,
        Nodes,
        NeighborsByLayer
    ).

%% Initialize empty neighbors map for all layers
init_neighbors(MaxLayer) ->
    maps:from_list([{L, []} || L <- lists:seq(0, MaxLayer)]).

%% Generate random layer for new node
random_layer(Ml) ->
    %% Layer = floor(-ln(uniform) * ml)
    U = rand:uniform(),
    floor(-math:log(U) * Ml).

%% Distance function dispatcher
distance(Vec1, Vec2, cosine) -> cosine_distance(Vec1, Vec2);
distance(Vec1, Vec2, euclidean) -> euclidean_distance(Vec1, Vec2).

%% Dot product
dot_product(Vec1, Vec2) ->
    lists:sum([A * B || {A, B} <- lists:zip(Vec1, Vec2)]).

%% Serialize neighbors map
serialize_neighbors(Neighbors) ->
    Sorted = lists:sort(maps:to_list(Neighbors)),
    << <<(serialize_layer_neighbors(L, Ns))/binary>> || {L, Ns} <- Sorted >>.

serialize_layer_neighbors(Layer, NeighborIds) ->
    NumNeighbors = length(NeighborIds),
    NeighborsBin = << <<(byte_size(Id)):16, Id/binary>> || Id <- NeighborIds >>,
    <<Layer:8, NumNeighbors:16, NeighborsBin/binary>>.

%% Deserialize neighbors
deserialize_neighbors(Rest, 0, Acc) ->
    {ok, Acc, Rest};
deserialize_neighbors(<<L:8, NumNeighbors:16, Rest/binary>>, N, Acc) ->
    case deserialize_neighbor_ids(Rest, NumNeighbors, []) of
        {ok, NeighborIds, Rest2} ->
            deserialize_neighbors(Rest2, N - 1, Acc#{L => NeighborIds});
        {error, _} = Error ->
            Error
    end;
deserialize_neighbors(_, _, _) ->
    {error, truncated_data}.

deserialize_neighbor_ids(Rest, 0, Acc) ->
    {ok, lists:reverse(Acc), Rest};
deserialize_neighbor_ids(<<Len:16, Id:Len/binary, Rest/binary>>, N, Acc) ->
    deserialize_neighbor_ids(Rest, N - 1, [Id | Acc]);
deserialize_neighbor_ids(_, _, _) ->
    {error, truncated_neighbor_data}.
