%%%-------------------------------------------------------------------
%%% @doc Vector Distance Computation Module
%%%
%%% Provides SIMD-accelerated distance computations via NIF.
%%% Falls back to pure Erlang implementation if NIF is not available.
%%%
%%% Supports:
%%% - Cosine distance
%%% - Euclidean distance
%%% - Dot product
%%%
%%% Vectors can be represented as:
%%% - Lists of floats (Erlang native, slower)
%%% - Binaries of 32-bit floats (NIF optimized, faster)
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_distance).

-export([
    %% Distance functions (work with both list and binary vectors)
    dot_product/2,
    cosine_distance/2,
    cosine_distance_normalized/2,
    euclidean_distance/2,

    %% Conversion functions
    to_binary/1,
    from_binary/1,

    %% Info
    simd_info/0,
    nif_available/0
]).

-on_load(init/0).

%% NIF loading
init() ->
    PrivDir = case code:priv_dir(barrel_vectordb) of
        {error, _} ->
            %% Fallback for development
            EbinDir = filename:dirname(code:which(?MODULE)),
            filename:join(filename:dirname(EbinDir), "priv");
        Dir -> Dir
    end,
    SoPath = filename:join(PrivDir, "barrel_vectordb_distance_nif"),
    case erlang:load_nif(SoPath, 0) of
        ok -> ok;
        {error, _Reason} ->
            %% NIF not available, use Erlang fallback
            ok
    end.

%% ============================================================
%% Distance functions
%% ============================================================

%% @doc Compute dot product of two vectors
-spec dot_product(vector(), vector()) -> float().
dot_product(Vec1, Vec2) when is_list(Vec1), is_list(Vec2) ->
    dot_product_list(Vec1, Vec2, 0.0);
dot_product(Bin1, Bin2) when is_binary(Bin1), is_binary(Bin2) ->
    nif_dot_product(Bin1, Bin2);
dot_product(Vec1, Vec2) when is_list(Vec1), is_binary(Vec2) ->
    dot_product(to_binary(Vec1), Vec2);
dot_product(Vec1, Vec2) when is_binary(Vec1), is_list(Vec2) ->
    dot_product(Vec1, to_binary(Vec2)).

%% @doc Compute cosine distance: 1 - cos(a, b)
-spec cosine_distance(vector(), vector()) -> float().
cosine_distance(Vec1, Vec2) when is_list(Vec1), is_list(Vec2) ->
    cosine_distance_list(Vec1, Vec2);
cosine_distance(Bin1, Bin2) when is_binary(Bin1), is_binary(Bin2) ->
    nif_cosine_distance(Bin1, Bin2);
cosine_distance(Vec1, Vec2) when is_list(Vec1), is_binary(Vec2) ->
    cosine_distance(to_binary(Vec1), Vec2);
cosine_distance(Vec1, Vec2) when is_binary(Vec1), is_list(Vec2) ->
    cosine_distance(Vec1, to_binary(Vec2)).

%% @doc Compute cosine distance for pre-normalized vectors (faster)
-spec cosine_distance_normalized(vector(), vector()) -> float().
cosine_distance_normalized(Vec1, Vec2) when is_list(Vec1), is_list(Vec2) ->
    1.0 - dot_product_list(Vec1, Vec2, 0.0);
cosine_distance_normalized(Bin1, Bin2) when is_binary(Bin1), is_binary(Bin2) ->
    nif_cosine_distance_normalized(Bin1, Bin2);
cosine_distance_normalized(Vec1, Vec2) when is_list(Vec1), is_binary(Vec2) ->
    cosine_distance_normalized(to_binary(Vec1), Vec2);
cosine_distance_normalized(Vec1, Vec2) when is_binary(Vec1), is_list(Vec2) ->
    cosine_distance_normalized(Vec1, to_binary(Vec2)).

%% @doc Compute Euclidean distance
-spec euclidean_distance(vector(), vector()) -> float().
euclidean_distance(Vec1, Vec2) when is_list(Vec1), is_list(Vec2) ->
    euclidean_distance_list(Vec1, Vec2);
euclidean_distance(Bin1, Bin2) when is_binary(Bin1), is_binary(Bin2) ->
    nif_euclidean_distance(Bin1, Bin2);
euclidean_distance(Vec1, Vec2) when is_list(Vec1), is_binary(Vec2) ->
    euclidean_distance(to_binary(Vec1), Vec2);
euclidean_distance(Vec1, Vec2) when is_binary(Vec1), is_list(Vec2) ->
    euclidean_distance(Vec1, to_binary(Vec2)).

%% ============================================================
%% Conversion functions
%% ============================================================

%% @doc Convert list of floats to binary vector (32-bit floats)
-spec to_binary([float()]) -> binary().
to_binary(List) when is_list(List) ->
    nif_to_binary(List).

%% @doc Convert binary vector to list of floats
-spec from_binary(binary()) -> [float()].
from_binary(Bin) when is_binary(Bin) ->
    nif_from_binary(Bin).

%% ============================================================
%% Info functions
%% ============================================================

%% @doc Return SIMD backend information
-spec simd_info() -> #{backend => avx2 | neon | scalar | erlang}.
simd_info() ->
    nif_simd_info().

%% @doc Check if NIF is available
-spec nif_available() -> boolean().
nif_available() ->
    try
        _ = simd_info(),
        true
    catch
        error:undef -> false
    end.

%% ============================================================
%% NIF stubs (replaced when NIF loads successfully)
%% ============================================================

nif_dot_product(Bin1, Bin2) ->
    %% Fallback if NIF not loaded
    dot_product_list(binary_to_list_fallback(Bin1), binary_to_list_fallback(Bin2), 0.0).

nif_cosine_distance(Bin1, Bin2) ->
    %% Fallback if NIF not loaded
    cosine_distance_list(binary_to_list_fallback(Bin1), binary_to_list_fallback(Bin2)).

nif_cosine_distance_normalized(Bin1, Bin2) ->
    %% Fallback if NIF not loaded
    1.0 - nif_dot_product(Bin1, Bin2).

nif_euclidean_distance(Bin1, Bin2) ->
    %% Fallback if NIF not loaded
    euclidean_distance_list(binary_to_list_fallback(Bin1), binary_to_list_fallback(Bin2)).

nif_to_binary(List) ->
    %% Fallback if NIF not loaded
    << <<F:32/float-little>> || F <- List >>.

nif_from_binary(Bin) ->
    %% Fallback if NIF not loaded
    binary_to_list_fallback(Bin).

nif_simd_info() ->
    %% Fallback if NIF not loaded
    #{backend => erlang}.

%% ============================================================
%% Erlang fallback implementations
%% ============================================================

-type vector() :: [float()] | binary().

binary_to_list_fallback(<<>>) -> [];
binary_to_list_fallback(<<F:32/float-little, Rest/binary>>) ->
    [F | binary_to_list_fallback(Rest)].

dot_product_list([], [], Acc) -> Acc;
dot_product_list([A | R1], [B | R2], Acc) ->
    dot_product_list(R1, R2, Acc + A * B).

cosine_distance_list(Vec1, Vec2) ->
    Dot = dot_product_list(Vec1, Vec2, 0.0),
    Norm1 = math:sqrt(dot_product_list(Vec1, Vec1, 0.0)),
    Norm2 = math:sqrt(dot_product_list(Vec2, Vec2, 0.0)),
    Denom = Norm1 * Norm2,
    case Denom < 1.0e-10 of
        true -> 1.0;
        false -> 1.0 - (Dot / Denom)
    end.

euclidean_distance_list(Vec1, Vec2) ->
    SqDist = euclidean_sq_list(Vec1, Vec2, 0.0),
    math:sqrt(SqDist).

euclidean_sq_list([], [], Acc) -> Acc;
euclidean_sq_list([A | R1], [B | R2], Acc) ->
    Diff = A - B,
    euclidean_sq_list(R1, R2, Acc + Diff * Diff).
