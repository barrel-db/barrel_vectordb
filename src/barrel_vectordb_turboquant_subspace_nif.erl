%%%-------------------------------------------------------------------
%%% @doc NIF wrapper for SIMD-accelerated Subspace-TurboQuant ADC distance.
%%%
%%% Provides SIMD acceleration for the hot path in Subspace-TurboQuant
%%% distance computation across M independent subspaces.
%%% Supports AVX2 on x86_64 and NEON on ARM.
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_turboquant_subspace_nif).

-export([
    adc_distance/4,
    batch_adc_distance/4,
    simd_info/0
]).

-on_load(init/0).

%%====================================================================
%% NIF Loading
%%====================================================================

init() ->
    PrivDir = case code:priv_dir(barrel_vectordb) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            filename:join(filename:dirname(EbinDir), "priv");
        Dir -> Dir
    end,
    SoPath = filename:join(PrivDir, "barrel_vectordb_turboquant_subspace_nif"),
    erlang:load_nif(SoPath, 0).

%%====================================================================
%% NIF Functions
%%====================================================================

%% @doc Compute ADC distance across M subspaces using SIMD-accelerated NIF.
%% @param Tables Precomputed distance tables (binary with M tables)
%% @param Code Encoded vector (binary with M subspace codes)
%% @param Bits Bits per angle (2-4)
%% @param M Number of subspaces
%% @returns Distance as float
-spec adc_distance(binary(), binary(), 2..4, pos_integer()) -> float().
adc_distance(_Tables, _Code, _Bits, _M) ->
    erlang:nif_error(not_loaded).

%% @doc Compute ADC distance for multiple codes across M subspaces.
%% Amortizes NIF call overhead for batch operations.
%% @param Tables Precomputed distance tables (binary)
%% @param Codes List of encoded vectors (list of binaries)
%% @param Bits Bits per angle (2-4)
%% @param M Number of subspaces
%% @returns List of distances
-spec batch_adc_distance(binary(), [binary()], 2..4, pos_integer()) -> [float()].
batch_adc_distance(_Tables, _Codes, _Bits, _M) ->
    erlang:nif_error(not_loaded).

%% @doc Return SIMD backend info.
%% @returns avx2 | neon | scalar
-spec simd_info() -> avx2 | neon | scalar.
simd_info() ->
    erlang:nif_error(not_loaded).
