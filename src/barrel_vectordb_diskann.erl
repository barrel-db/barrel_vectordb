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

%% @doc Core search algorithm
%% Returns ({SortedResults, VisitedSet})
greedy_search(Index, StartId, Query, K, L) ->
    StartDist = distance(Index, StartId, Query),
    %% Result list: {Distance, Id} sorted by distance
    ResultList = gb_sets:singleton({StartDist, StartId}),
    %% Visited tracks nodes we've EXPANDED (not just added to result list)
    Visited = sets:new(),

    greedy_loop(Index, Query, ResultList, Visited, K, L).

greedy_loop(Index, Query, ResultList, Visited, K, L) ->
    %% Find closest unvisited node
    Unvisited = gb_sets:filter(
        fun({_D, Id}) -> not sets:is_element(Id, Visited) end,
        ResultList
    ),

    case gb_sets:is_empty(Unvisited) of
        true ->
            %% Return K closest and all visited
            Results = gb_sets:to_list(ResultList),
            TopK = lists:sublist(Results, K),
            {TopK, Visited};
        false ->
            {{_Dist, P}, _} = gb_sets:take_smallest(Unvisited),
            NewVisited = sets:add_element(P, Visited),

            %% Expand neighbors
            Neighbors = get_neighbors(Index, P),
            {NewResultList, _} = lists:foldl(
                fun(N, {AccResult, AccVisited}) ->
                    case sets:is_element(N, AccVisited) of
                        true ->
                            {AccResult, AccVisited};
                        false ->
                            D = distance(Index, N, Query),
                            NewResult = gb_sets:add({D, N}, AccResult),
                            {NewResult, AccVisited}
                    end
                end,
                {ResultList, NewVisited},
                Neighbors
            ),

            %% Keep only L closest
            Pruned = case gb_sets:size(NewResultList) > L of
                true ->
                    PrunedList = lists:sublist(gb_sets:to_list(NewResultList), L),
                    gb_sets:from_list(PrunedList);
                false ->
                    NewResultList
            end,

            greedy_loop(Index, Query, Pruned, NewVisited, K, L)
    end.

%%====================================================================
%% Internal: RobustPrune (Algorithm 2 from DiskANN paper)
%%====================================================================

%% @doc RobustPrune: Select R neighbors for node P using alpha-RNG pruning
%% V = candidate neighbors
%% Alpha > 1 keeps more long-range edges
robust_prune(Index, P, V, Alpha, R) ->
    %% V <- (V ∪ N_out(P)) \ {P}
    CurrentNeighbors = get_neighbors(Index, P),
    Candidates = lists:usort(V ++ CurrentNeighbors) -- [P],

    %% Sort candidates by distance to P
    PVec = maps:get(P, Index#diskann_index.vectors),
    SortedCandidates = lists:sort(
        fun(A, B) ->
            DistA = distance(Index, A, PVec),
            DistB = distance(Index, B, PVec),
            DistA =< DistB
        end,
        Candidates
    ),

    %% Prune to get new neighbors
    NewNeighbors = prune_neighbors(Index, P, SortedCandidates, Alpha, R),

    %% Update node
    set_neighbors(Index, P, NewNeighbors).

%% Alpha-RNG pruning: remove p' if there's a better path through p*
prune_neighbors(Index, P, Candidates, Alpha, R) ->
    PVec = maps:get(P, Index#diskann_index.vectors),
    prune_loop(Index, PVec, Candidates, Alpha, R, []).

prune_loop(_Index, _PVec, [], _Alpha, _R, Acc) ->
    lists:reverse(Acc);
prune_loop(_Index, _PVec, _Candidates, _Alpha, R, Acc) when length(Acc) >= R ->
    lists:reverse(Acc);
prune_loop(Index, PVec, [PStar | Rest], Alpha, R, Acc) ->
    %% p* is the closest remaining candidate
    %% Add p* to neighbors
    NewAcc = [PStar | Acc],

    %% Filter out candidates that are closer to p* than to P (with alpha factor)
    PStarVec = maps:get(PStar, Index#diskann_index.vectors),

    FilteredRest = lists:filter(
        fun(PPrime) ->
            PPrimeVec = maps:get(PPrime, Index#diskann_index.vectors),
            DistPStar_PPrime = distance_vec(Index#diskann_index.config, PStarVec, PPrimeVec),
            DistP_PPrime = distance_vec(Index#diskann_index.config, PVec, PPrimeVec),
            %% Keep p' only if alpha * d(p*, p') > d(p, p')
            %% (i.e., p* is NOT a good detour to reach p')
            Alpha * DistPStar_PPrime > DistP_PPrime
        end,
        Rest
    ),

    prune_loop(Index, PVec, FilteredRest, Alpha, R, NewAcc).

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
