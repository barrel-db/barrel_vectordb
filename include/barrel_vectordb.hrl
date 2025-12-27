%%%-------------------------------------------------------------------
%%% @doc barrel_vectordb header file with records and types
%%% @end
%%%-------------------------------------------------------------------

-ifndef(BARREL_VECTORDB_HRL).
-define(BARREL_VECTORDB_HRL, true).

%%====================================================================
%% Common Types
%%====================================================================

-type id() :: binary().
-type vector() :: [float()] | binary().
-type distance() :: float().
-type dimension() :: pos_integer().

%%====================================================================
%% HNSW Types and Records
%%====================================================================

%% HNSW configuration
-record(hnsw_config, {
    m = 16 :: pos_integer(),           %% Max connections per layer
    m_max0 = 32 :: pos_integer(),      %% Max connections at layer 0
    ef_construction = 200 :: pos_integer(), %% Build-time ef
    ml :: float() | undefined,          %% Level multiplier (1/ln(M))
    distance_fn = cosine :: cosine | euclidean
}).

-type hnsw_config() :: #hnsw_config{}.

%% Quantized vector: 8-bit signed integers with scale factor
%% Format: <<Scale:32/float, Components/binary>> where each component is int8
-type quantized_vector() :: binary().

%% HNSW node with quantized vector and cached norm
-record(hnsw_node, {
    id :: binary(),
    vector :: quantized_vector(),       %% 8-bit quantized vector binary
    norm :: float(),                    %% Pre-computed L2 norm (of original)
    layer :: non_neg_integer(),         %% Max layer this node exists in
    neighbors = #{} :: #{non_neg_integer() => [binary()]}
}).

-type hnsw_node() :: #hnsw_node{}.

%% HNSW index
-record(hnsw_index, {
    entry_point :: binary() | undefined,
    max_layer = 0 :: non_neg_integer(),
    nodes = #{} :: #{binary() => hnsw_node()},
    config :: hnsw_config(),
    size = 0 :: non_neg_integer(),
    dimension :: pos_integer()
}).

-type hnsw_index() :: #hnsw_index{}.

%% Search candidate (for priority queue operations)
-record(candidate, {
    id :: binary(),
    distance :: float()
}).

-type candidate() :: #candidate{}.

%%====================================================================
%% Vector Store Types
%%====================================================================

-type metadata() :: #{
    file => binary(),
    start_line => pos_integer(),
    end_line => pos_integer(),
    type => atom(),
    atom() => term()
}.

-type search_result() :: #{
    id := binary(),
    text := binary(),
    metadata := metadata(),
    score := float(),
    vector => vector()
}.

-type search_options() :: #{
    k => pos_integer(),
    filters => map(),
    include_vectors => boolean()
}.

%%====================================================================
%% Constants
%%====================================================================

-define(DEFAULT_DIMENSION, 768).
-define(FLOAT32_SIZE, 4).

%% Column family names
-define(CF_DEFAULT, "default").
-define(CF_VECTORS, "vectors").
-define(CF_METADATA, "metadata").
-define(CF_TEXT, "text").
-define(CF_HNSW, "hnsw_graph").

%% HNSW persistence keys
-define(HNSW_META_KEY, <<"__hnsw_meta__">>).
-define(HNSW_NODE_VERSION, 1).

-endif.
