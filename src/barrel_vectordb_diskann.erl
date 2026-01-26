%%%-------------------------------------------------------------------
%%% @doc DiskANN Vamana Graph Implementation
%%%
%%% Implements the Vamana graph algorithm from the DiskANN paper with:
%%% - Two-pass construction (alpha=1.0 then alpha>1.0)
%%% - RobustPrune for alpha-RNG pruning
%%% - GreedySearch for graph traversal
%%% - FreshVamana insert/delete for streaming updates
%%% - Consolidate deletes for batch cleanup
%%% - BeamSearch with PQ for SSD-resident search
%%%
%%% The alpha parameter (>1) is critical for maintaining graph quality
%%% under streaming updates. It keeps more long-range edges which
%%% are essential for fast convergence during search.
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_diskann).

-include("barrel_vectordb.hrl").

%% API
-export([
    new/1,
    build/2,
    insert/3,
    delete/2,
    search/3,
    search/4,
    size/1,
    info/1,
    get_vector/2,
    consolidate_deletes/1
]).

%% Internal exports for testing
-export([
    greedy_search/5,
    robust_prune/5,
    find_medoid/2
]).

-record(diskann_config, {
    r = 64 :: pos_integer(),              %% Max out-degree
    l_build = 100 :: pos_integer(),       %% Build search width
    l_search = 100 :: pos_integer(),      %% Query search width
    alpha = 1.2 :: float(),               %% Pruning factor (>1 for long-range)
    dimension :: pos_integer(),
    distance_fn = cosine :: cosine | euclidean
}).

-record(diskann_index, {
    config :: #diskann_config{},
    size = 0 :: non_neg_integer(),
    medoid_id :: binary() | undefined,    %% Entry point (centroid)
    nodes = #{} :: #{binary() => diskann_node()},
    vectors = #{} :: #{binary() => [float()]},
    deleted_set = sets:new() :: sets:set(binary()),
    pq_state :: term() | undefined        %% Optional PQ for compression
}).

-record(diskann_node, {
    id :: binary(),
    neighbors = [] :: [binary()]
}).

-type diskann_index() :: #diskann_index{}.
-type diskann_node() :: #diskann_node{}.
-type diskann_config() :: #diskann_config{}.

-export_type([diskann_index/0, diskann_config/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Create a new empty DiskANN index
-spec new(map()) -> {ok, diskann_index()} | {error, term()}.
new(Options) ->
    R = maps:get(r, Options, 64),
    LBuild = maps:get(l_build, Options, 100),
    LSearch = maps:get(l_search, Options, 100),
    Alpha = maps:get(alpha, Options, 1.2),
    Dimension = maps:get(dimension, Options, undefined),
    DistanceFn = maps:get(distance_fn, Options, cosine),

    case Dimension of
        undefined ->
            {error, dimension_required};
        _ when Dimension > 0 ->
            Config = #diskann_config{
                r = R,
                l_build = LBuild,
                l_search = LSearch,
                alpha = Alpha,
                dimension = Dimension,
                distance_fn = DistanceFn
            },
            {ok, #diskann_index{config = Config}};
        _ ->
            {error, {invalid_dimension, Dimension}}
    end.

%% @doc Build index from a list of {Id, Vector} pairs using two-pass Vamana
-spec build(map(), [{binary(), [float()]}]) -> {ok, diskann_index()} | {error, term()}.
build(Options, Vectors) when length(Vectors) > 0 ->
    case new(Options) of
        {ok, Index0} ->
            %% Store all vectors
            VectorMap = maps:from_list(Vectors),
            Index1 = Index0#diskann_index{
                vectors = VectorMap,
                size = length(Vectors)
            },

            %% Find medoid (centroid) as entry point
            MedoidId = find_medoid(Vectors, Index1#diskann_index.config),
            Index2 = Index1#diskann_index{medoid_id = MedoidId},

            %% Initialize random graph
            Index3 = init_random_graph(Index2, maps:keys(VectorMap)),

            %% Two-pass Vamana construction
            Config = Index3#diskann_index.config,
            R = Config#diskann_config.r,
            L = Config#diskann_config.l_build,
            Alpha = Config#diskann_config.alpha,

            %% Pass 1: alpha = 1.0 (finds good short edges)
            Index4 = vamana_pass(Index3, 1.0, L, R),

            %% Pass 2: alpha > 1.0 (adds long-range edges for fast convergence)
            Index5 = vamana_pass(Index4, Alpha, L, R),

            {ok, Index5};
        {error, _} = Error ->
            Error
    end;
build(_, []) ->
    {error, empty_vectors}.

%% @doc Insert a new vector (FreshVamana algorithm)
-spec insert(diskann_index(), binary(), [float()]) -> {ok, diskann_index()} | {error, term()}.
insert(#diskann_index{medoid_id = undefined} = Index, Id, Vector) ->
    %% First insertion
    Config = Index#diskann_index.config,
    Dim = Config#diskann_config.dimension,
    case length(Vector) of
        Dim ->
            NewIndex = Index#diskann_index{
                medoid_id = Id,
                nodes = #{Id => #diskann_node{id = Id, neighbors = []}},
                vectors = #{Id => Vector},
                size = 1
            },
            {ok, NewIndex};
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end;
insert(#diskann_index{config = Config, medoid_id = S} = Index, Id, Vector) ->
    Dim = Config#diskann_config.dimension,
    case length(Vector) of
        Dim ->
            #diskann_config{l_build = L, alpha = Alpha, r = R} = Config,

            %% Add vector to index
            Index1 = Index#diskann_index{
                vectors = maps:put(Id, Vector, Index#diskann_index.vectors),
                nodes = maps:put(Id, #diskann_node{id = Id, neighbors = []},
                                 Index#diskann_index.nodes),
                size = Index#diskann_index.size + 1
            },

            %% Search to find candidate neighbors
            {_Results, Visited} = greedy_search(Index1, S, Vector, 1, L),

            %% Prune to select R out-neighbors
            Index2 = robust_prune(Index1, Id, sets:to_list(Visited), Alpha, R),
            Neighbors = get_neighbors(Index2, Id),

            %% Add backward edges (critical for navigability)
            Index3 = lists:foldl(
                fun(J, AccIndex) ->
                    JNeighbors = get_neighbors(AccIndex, J),
                    case length(JNeighbors) + 1 > R of
                        true ->
                            %% Prune if degree exceeded
                            robust_prune(AccIndex, J, [Id | JNeighbors], Alpha, R);
                        false ->
                            add_neighbor(AccIndex, J, Id)
                    end
                end,
                Index2,
                Neighbors
            ),

            {ok, Index3};
        Other ->
            {error, {dimension_mismatch, Dim, Other}}
    end.

%% @doc Lazy delete - marks node as deleted
-spec delete(diskann_index(), binary()) -> {ok, diskann_index()}.
delete(Index, Id) ->
    NewDeleted = sets:add_element(Id, Index#diskann_index.deleted_set),
    {ok, Index#diskann_index{deleted_set = NewDeleted}}.

%% @doc Search for K nearest neighbors
-spec search(diskann_index(), [float()], pos_integer()) -> [{binary(), float()}].
search(Index, Query, K) ->
    search(Index, Query, K, #{}).

%% @doc Search with options
-spec search(diskann_index(), [float()], pos_integer(), map()) -> [{binary(), float()}].
search(#diskann_index{medoid_id = undefined}, _Query, _K, _Opts) ->
    [];
search(#diskann_index{medoid_id = S, config = Config, deleted_set = DeletedSet} = Index,
       Query, K, Opts) ->
    L = maps:get(l_search, Opts, Config#diskann_config.l_search),

    %% Greedy search from medoid
    {Results, _Visited} = greedy_search(Index, S, Query, K * 2, L),

    %% Filter deleted nodes
    Filtered = [{D, Id} || {D, Id} <- Results,
                           not sets:is_element(Id, DeletedSet)],

    %% Return top K
    TopK = lists:sublist(Filtered, K),
    [{Id, D} || {D, Id} <- TopK].

%% @doc Get index size (excluding deleted)
-spec size(diskann_index()) -> non_neg_integer().
size(#diskann_index{size = Size, deleted_set = Deleted}) ->
    Size - sets:size(Deleted).

%% @doc Get index info
-spec info(diskann_index()) -> map().
info(#diskann_index{config = Config, size = Size, medoid_id = Medoid,
                    deleted_set = Deleted, nodes = Nodes}) ->
    AvgDegree = case maps:size(Nodes) of
        0 -> 0.0;
        N ->
            TotalDegree = maps:fold(
                fun(_, #diskann_node{neighbors = Ns}, Acc) ->
                    Acc + length(Ns)
                end,
                0,
                Nodes
            ),
            TotalDegree / N
    end,
    #{
        size => Size,
        active_size => Size - sets:size(Deleted),
        deleted_count => sets:size(Deleted),
        medoid => Medoid,
        avg_degree => AvgDegree,
        config => #{
            r => Config#diskann_config.r,
            l_build => Config#diskann_config.l_build,
            l_search => Config#diskann_config.l_search,
            alpha => Config#diskann_config.alpha,
            dimension => Config#diskann_config.dimension,
            distance_fn => Config#diskann_config.distance_fn
        }
    }.

%% @doc Get vector by ID
-spec get_vector(diskann_index(), binary()) -> {ok, [float()]} | not_found.
get_vector(#diskann_index{vectors = Vectors}, Id) ->
    case maps:find(Id, Vectors) of
        {ok, Vec} -> {ok, Vec};
        error -> not_found
    end.

%% @doc Consolidate deleted nodes (batch cleanup)
%% This repairs the graph by removing edges to deleted nodes
%% and adding new edges to maintain navigability
-spec consolidate_deletes(diskann_index()) -> {ok, diskann_index()}.
consolidate_deletes(#diskann_index{deleted_set = DeletedSet, config = Config,
                                   nodes = Nodes, vectors = Vectors} = Index) ->
    case sets:size(DeletedSet) of
        0 ->
            {ok, Index};
        _ ->
            consolidate_deletes_impl(Index, DeletedSet, Config, Nodes, Vectors)
    end.

consolidate_deletes_impl(Index, DeletedSet, Config, Nodes, Vectors) ->
    #diskann_config{alpha = Alpha, r = R} = Config,

    %% For each node with edges to deleted nodes, repair neighborhood
    UpdatedNodes = maps:fold(
        fun(P, #diskann_node{neighbors = Neighbors} = Node, AccNodes) ->
            case sets:is_element(P, DeletedSet) of
                true ->
                    %% Skip deleted nodes
                    AccNodes;
                false ->
                    DeletedNeighbors = [N || N <- Neighbors,
                                             sets:is_element(N, DeletedSet)],
                    case DeletedNeighbors of
                        [] ->
                            AccNodes#{P => Node};
                        _ ->
                            %% Repair: find new candidates from deleted nodes' neighbors
                            SurvivingNeighbors = Neighbors -- DeletedNeighbors,
                            DeletedOutNeighbors = lists:flatmap(
                                fun(V) ->
                                    case maps:find(V, Nodes) of
                                        {ok, #diskann_node{neighbors = VNs}} ->
                                            [N || N <- VNs,
                                                  not sets:is_element(N, DeletedSet)];
                                        error -> []
                                    end
                                end,
                                DeletedNeighbors
                            ),
                            Candidates = lists:usort(SurvivingNeighbors ++ DeletedOutNeighbors) -- [P],

                            %% Re-prune with alpha
                            NewNeighbors = prune_neighbors(
                                Index, P, Candidates, Alpha, R
                            ),
                            AccNodes#{P => Node#diskann_node{neighbors = NewNeighbors}}
                    end
            end
        end,
        #{},
        Nodes
    ),

    %% Remove deleted nodes from index
    NewVectors = maps:without(sets:to_list(DeletedSet), Vectors),
    NewSize = maps:size(UpdatedNodes),

    %% Update medoid if it was deleted
    NewMedoid = case sets:is_element(Index#diskann_index.medoid_id, DeletedSet) of
        true ->
            %% Pick new medoid from remaining nodes
            case maps:keys(UpdatedNodes) of
                [] -> undefined;
                [First | _] -> First
            end;
        false ->
            Index#diskann_index.medoid_id
    end,

    {ok, Index#diskann_index{
        nodes = UpdatedNodes,
        vectors = NewVectors,
        deleted_set = sets:new(),
        size = NewSize,
        medoid_id = NewMedoid
    }}.

%%====================================================================
%% Internal: Vamana Build
%%====================================================================

%% Find medoid (vector closest to centroid)
find_medoid(Vectors, #diskann_config{dimension = Dim}) ->
    %% Compute centroid
    N = length(Vectors),
    Centroid = lists:foldl(
        fun({_Id, Vec}, Acc) ->
            [A + V || {A, V} <- lists:zip(Acc, Vec)]
        end,
        [0.0 || _ <- lists:seq(1, Dim)],
        Vectors
    ),
    NormCentroid = [C / N || C <- Centroid],

    %% Find closest to centroid
    {MedoidId, _MinDist} = lists:foldl(
        fun({Id, Vec}, {BestId, BestDist}) ->
            Dist = euclidean_distance(Vec, NormCentroid),
            case Dist < BestDist of
                true -> {Id, Dist};
                false -> {BestId, BestDist}
            end
        end,
        {undefined, infinity},
        Vectors
    ),
    MedoidId.

%% Initialize random R-regular graph
init_random_graph(#diskann_index{config = Config} = Index, Ids) ->
    R = Config#diskann_config.r,
    N = length(Ids),
    IdsArray = list_to_tuple(Ids),

    Nodes = lists:foldl(
        fun(Id, Acc) ->
            %% Pick R random neighbors (excluding self)
            Neighbors = random_neighbors(Id, IdsArray, N, R),
            Acc#{Id => #diskann_node{id = Id, neighbors = Neighbors}}
        end,
        #{},
        Ids
    ),
    Index#diskann_index{nodes = Nodes}.

random_neighbors(Id, IdsArray, N, R) ->
    NumNeighbors = min(R, N - 1),
    random_neighbors(Id, IdsArray, N, NumNeighbors, []).

random_neighbors(_Id, _IdsArray, _N, 0, Acc) ->
    Acc;
random_neighbors(Id, IdsArray, N, Remaining, Acc) ->
    Idx = rand:uniform(N),
    Neighbor = element(Idx, IdsArray),
    case Neighbor =:= Id orelse lists:member(Neighbor, Acc) of
        true ->
            random_neighbors(Id, IdsArray, N, Remaining, Acc);
        false ->
            random_neighbors(Id, IdsArray, N, Remaining - 1, [Neighbor | Acc])
    end.

%% Single pass of Vamana construction
vamana_pass(#diskann_index{medoid_id = S, nodes = Nodes} = Index,
            Alpha, L, R) ->
    %% Random permutation of all node IDs
    Ids = maps:keys(Nodes),
    Sigma = shuffle(Ids),

    lists:foldl(
        fun(Id, AccIndex) ->
            Vec = maps:get(Id, AccIndex#diskann_index.vectors),

            %% Search to find candidates
            {_Results, Visited} = greedy_search(AccIndex, S, Vec, 1, L),

            %% Prune to select R out-neighbors
            AccIndex2 = robust_prune(AccIndex, Id, sets:to_list(Visited), Alpha, R),
            Neighbors = get_neighbors(AccIndex2, Id),

            %% Add backward edges (bidirectional)
            lists:foldl(
                fun(J, AccInner) ->
                    JNeighbors = get_neighbors(AccInner, J),
                    case length(JNeighbors) + 1 > R of
                        true ->
                            robust_prune(AccInner, J, [Id | JNeighbors], Alpha, R);
                        false ->
                            add_neighbor(AccInner, J, Id)
                    end
                end,
                AccIndex2,
                Neighbors
            )
        end,
        Index,
        Sigma
    ).

shuffle(List) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), N} || N <- List])].

%%====================================================================
%% Internal: GreedySearch (Algorithm 1 from DiskANN paper)
%%====================================================================

%% @doc Core search algorithm - optimized version
%% Uses separate candidate queue and result set for efficiency
%% Returns ({SortedResults, VisitedSet})
greedy_search(Index, StartId, Query, K, L) ->
    StartDist = distance(Index, StartId, Query),
    %% Candidates: min-heap of nodes to explore
    Candidates = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Results: best L nodes found so far
    Results = gb_trees:from_orddict([{{StartDist, StartId}, true}]),
    %% Visited: nodes we've already expanded
    Visited = sets:from_list([StartId]),
    %% Track furthest result distance for pruning
    FurthestDist = StartDist,

    greedy_loop(Index, Query, Candidates, Results, Visited, FurthestDist, K, L).

greedy_loop(_Index, _Query, {0, nil}, Results, Visited, _FurthestDist, K, _L) ->
    %% No more candidates
    ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
    TopK = lists:sublist(ResultList, K),
    {TopK, Visited};
greedy_loop(Index, Query, Candidates, Results, Visited, FurthestDist, K, L) ->
    %% Get closest candidate
    {{CurrentDist, CurrentId}, _, RestCandidates} = gb_trees:take_smallest(Candidates),

    %% If closest candidate is further than our furthest result, we're done
    case CurrentDist > FurthestDist of
        true ->
            ResultList = [Item || {Item, _} <- gb_trees:to_list(Results)],
            TopK = lists:sublist(ResultList, K),
            {TopK, Visited};
        false ->
            %% Expand neighbors
            Neighbors = get_neighbors(Index, CurrentId),
            {NewCandidates, NewResults, NewVisited, NewFurthestDist} = lists:foldl(
                fun(N, {CandAcc, ResAcc, VisAcc, FurthAcc}) ->
                    case sets:is_element(N, VisAcc) of
                        true ->
                            {CandAcc, ResAcc, VisAcc, FurthAcc};
                        false ->
                            NewVisAcc = sets:add_element(N, VisAcc),
                            D = distance(Index, N, Query),
                            ResSize = gb_trees:size(ResAcc),
                            ShouldAdd = D < FurthAcc orelse ResSize < L,
                            case ShouldAdd of
                                true ->
                                    NewCandAcc = gb_trees:insert({D, N}, true, CandAcc),
                                    NewResAcc0 = gb_trees:insert({D, N}, true, ResAcc),
                                    %% Trim results if too many
                                    NewResSize = gb_trees:size(NewResAcc0),
                                    {NewResAcc, NewFurthAcc} = case NewResSize > L of
                                        true ->
                                            {_, _, Trimmed} = gb_trees:take_largest(NewResAcc0),
                                            {{LastD, _}, _} = gb_trees:largest(Trimmed),
                                            {Trimmed, LastD};
                                        false ->
                                            {{MaxD, _}, _} = gb_trees:largest(NewResAcc0),
                                            {NewResAcc0, MaxD}
                                    end,
                                    {NewCandAcc, NewResAcc, NewVisAcc, NewFurthAcc};
                                false ->
                                    {CandAcc, ResAcc, NewVisAcc, FurthAcc}
                            end
                    end
                end,
                {RestCandidates, Results, Visited, FurthestDist},
                Neighbors
            ),

            greedy_loop(Index, Query, NewCandidates, NewResults, NewVisited, NewFurthestDist, K, L)
    end.

%%====================================================================
%% Internal: RobustPrune (Algorithm 2 from DiskANN paper)
%%====================================================================

%% @doc RobustPrune: Select R neighbors for node P using alpha-RNG pruning
%% V = candidate neighbors
%% Alpha > 1 keeps more long-range edges
%% Optimized: cache distance computations
robust_prune(Index, P, V, Alpha, R) ->
    %% V <- (V ∪ N_out(P)) \ {P}
    CurrentNeighbors = get_neighbors(Index, P),
    Candidates = lists:usort(V ++ CurrentNeighbors) -- [P],

    %% Pre-compute distances from P to all candidates (cache)
    PVec = maps:get(P, Index#diskann_index.vectors),
    Config = Index#diskann_index.config,
    Vectors = Index#diskann_index.vectors,

    %% Build list with cached distances: [{Dist, Id, Vec}]
    CandidatesWithDist = [{distance_vec(Config, PVec, maps:get(C, Vectors)), C, maps:get(C, Vectors)}
                          || C <- Candidates],

    %% Sort by distance to P
    SortedCandidates = lists:sort(fun({D1, _, _}, {D2, _, _}) -> D1 =< D2 end, CandidatesWithDist),

    %% Prune with cached data
    NewNeighbors = prune_loop_cached(Config, PVec, SortedCandidates, Alpha, R, []),

    %% Update node
    set_neighbors(Index, P, NewNeighbors).

%% Alpha-RNG pruning with cached vectors - avoids repeated map lookups
prune_loop_cached(_Config, _PVec, [], _Alpha, _R, Acc) ->
    lists:reverse(Acc);
prune_loop_cached(_Config, _PVec, _Candidates, _Alpha, R, Acc) when length(Acc) >= R ->
    lists:reverse(Acc);
prune_loop_cached(Config, PVec, [{_Dist, PStar, PStarVec} | Rest], Alpha, R, Acc) ->
    %% Add p* to neighbors
    NewAcc = [PStar | Acc],

    %% Filter out candidates that are closer to p* than to P (with alpha factor)
    FilteredRest = lists:filter(
        fun({DistP_PPrime, _PPrime, PPrimeVec}) ->
            DistPStar_PPrime = distance_vec(Config, PStarVec, PPrimeVec),
            %% Keep p' only if alpha * d(p*, p') > d(p, p')
            Alpha * DistPStar_PPrime > DistP_PPrime
        end,
        Rest
    ),

    prune_loop_cached(Config, PVec, FilteredRest, Alpha, R, NewAcc).

%% Original prune_neighbors kept for compatibility with consolidate_deletes
prune_neighbors(Index, P, Candidates, Alpha, R) ->
    PVec = maps:get(P, Index#diskann_index.vectors),
    Config = Index#diskann_index.config,
    Vectors = Index#diskann_index.vectors,
    CandidatesWithDist = [{distance_vec(Config, PVec, maps:get(C, Vectors)), C, maps:get(C, Vectors)}
                          || C <- Candidates],
    SortedCandidates = lists:sort(fun({D1, _, _}, {D2, _, _}) -> D1 =< D2 end, CandidatesWithDist),
    prune_loop_cached(Config, PVec, SortedCandidates, Alpha, R, []).

%%====================================================================
%% Internal: Distance Functions
%%====================================================================

distance(#diskann_index{vectors = Vectors, config = Config}, Id, QueryVec) ->
    case maps:find(Id, Vectors) of
        {ok, NodeVec} ->
            distance_vec(Config, QueryVec, NodeVec);
        error ->
            infinity
    end.

distance_vec(#diskann_config{distance_fn = cosine}, Vec1, Vec2) ->
    cosine_distance(Vec1, Vec2);
distance_vec(#diskann_config{distance_fn = euclidean}, Vec1, Vec2) ->
    euclidean_distance(Vec1, Vec2).

cosine_distance(Vec1, Vec2) ->
    Dot = dot_product(Vec1, Vec2),
    Norm1 = math:sqrt(dot_product(Vec1, Vec1)),
    Norm2 = math:sqrt(dot_product(Vec2, Vec2)),
    Denom = Norm1 * Norm2,
    case Denom < 1.0e-10 of
        true -> 1.0;
        false -> 1.0 - (Dot / Denom)
    end.

euclidean_distance(Vec1, Vec2) ->
    SumSq = lists:sum([math:pow(A - B, 2) || {A, B} <- lists:zip(Vec1, Vec2)]),
    math:sqrt(SumSq).

dot_product(Vec1, Vec2) ->
    lists:sum([A * B || {A, B} <- lists:zip(Vec1, Vec2)]).

%%====================================================================
%% Internal: Graph Operations
%%====================================================================

get_neighbors(#diskann_index{nodes = Nodes}, Id) ->
    case maps:find(Id, Nodes) of
        {ok, #diskann_node{neighbors = Ns}} -> Ns;
        error -> []
    end.

set_neighbors(#diskann_index{nodes = Nodes} = Index, Id, Neighbors) ->
    case maps:find(Id, Nodes) of
        {ok, Node} ->
            NewNode = Node#diskann_node{neighbors = Neighbors},
            Index#diskann_index{nodes = Nodes#{Id => NewNode}};
        error ->
            Index
    end.

add_neighbor(#diskann_index{nodes = Nodes} = Index, NodeId, NewNeighborId) ->
    case maps:find(NodeId, Nodes) of
        {ok, #diskann_node{neighbors = Ns} = Node} ->
            case lists:member(NewNeighborId, Ns) of
                true -> Index;
                false ->
                    NewNode = Node#diskann_node{neighbors = [NewNeighborId | Ns]},
                    Index#diskann_index{nodes = Nodes#{NodeId => NewNode}}
            end;
        error ->
            Index
    end.
