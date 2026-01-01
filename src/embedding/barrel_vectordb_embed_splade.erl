%%%-------------------------------------------------------------------
%%% @doc SPLADE sparse embedding provider
%%%
%%% Uses SPLADE (Sparse Lexical and Expansion) models for neural sparse
%%% embeddings. Produces sparse vectors suitable for inverted index search.
%%%
%%% == Requirements ==
%%% ```
%%% pip install transformers torch
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     python => "python3",                     %% Python executable (default)
%%%     model => "prithivida/Splade_PP_en_v1",   %% Model name (default)
%%%     timeout => 120000                        %% Timeout in ms (default)
%%% }.
%%% '''
%%%
%%% == Sparse Vector Format ==
%%% Unlike dense embeddings, SPLADE produces sparse vectors:
%%% ```
%%% #{indices => [1, 5, 10], values => [0.5, 0.3, 0.8]}
%%% '''
%%% Where indices are vocabulary token IDs and values are weights.
%%%
%%% == Supported Models ==
%%% - `"prithivida/Splade_PP_en_v1"' - Default, SPLADE++ English
%%% - `"naver/splade-cocondenser-ensembledistil"' - NAVER's SPLADE
%%%
%%% == Use Cases ==
%%% - Lexical-semantic hybrid search
%%% - Term expansion (captures synonyms and related terms)
%%% - Efficient inverted index storage
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_splade).
-behaviour(barrel_vectordb_embed_provider).

%% Behaviour callbacks
-export([
    embed/2,
    embed_batch/2,
    dimension/1,
    name/0,
    init/1,
    available/1
]).

%% Additional exports for sparse vectors
-export([
    embed_sparse/2,
    embed_batch_sparse/2
]).

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "prithivida/Splade_PP_en_v1").
-define(DEFAULT_TIMEOUT, 120000).
-define(DEFAULT_VOCAB_SIZE, 30522).

%% Sparse vector type
-type sparse_vector() :: #{
    indices := [non_neg_integer()],
    values := [float()]
}.

-export_type([sparse_vector/0]).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> splade.

%% @doc Get dimension (vocab size) for this provider.
%% For sparse vectors, dimension is the vocabulary size.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(vocab_size, Config, ?DEFAULT_VOCAB_SIZE).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    Python = maps:get(python, Config, ?DEFAULT_PYTHON),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    %% Validate model against registry (warning only)
    validate_model(Model),

    %% Find the Python script
    ScriptPath = find_sparse_script(),

    case ScriptPath of
        {ok, Script} ->
            PortOpts = [
                {args, [Script, Model]},
                {line, 10000000},
                binary,
                use_stdio,
                exit_status
            ],
            try
                Port = open_port({spawn_executable, Python}, PortOpts),
                case port_command_sync(Port, #{action => info}, Timeout) of
                    {ok, #{<<"ok">> := true, <<"vocab_size">> := VocabSize}} ->
                        NewConfig = Config#{
                            port => Port,
                            vocab_size => VocabSize,
                            timeout => Timeout
                        },
                        {ok, NewConfig};
                    {ok, #{<<"ok">> := false, <<"error">> := Err}} ->
                        catch port_close(Port),
                        {error, {python_error, Err}};
                    {error, Reason} ->
                        catch port_close(Port),
                        {error, Reason}
                end
            catch
                error:PortReason ->
                    {error, {port_open_failed, PortReason}}
            end;
        {error, ScriptError} ->
            {error, ScriptError}
    end.

%% @doc Check if provider is available.
-spec available(map()) -> boolean().
available(#{port := Port}) ->
    erlang:port_info(Port) =/= undefined;
available(_Config) ->
    false.

%% @doc Generate dense embedding (converts sparse to dense).
%% Note: This is inefficient for large vocab sizes. Use embed_sparse/2 instead.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    case embed_sparse(Text, Config) of
        {ok, SparseVec} ->
            {ok, sparse_to_dense(SparseVec, dimension(Config))};
        {error, _} = Error ->
            Error
    end.

%% @doc Generate dense embeddings for batch (converts sparse to dense).
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    case embed_batch_sparse(Texts, Config) of
        {ok, SparseVecs} ->
            Dim = dimension(Config),
            DenseVecs = [sparse_to_dense(S, Dim) || S <- SparseVecs],
            {ok, DenseVecs};
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Sparse Vector API
%%====================================================================

%% @doc Generate sparse embedding for a single text.
-spec embed_sparse(binary(), map()) -> {ok, sparse_vector()} | {error, term()}.
embed_sparse(Text, Config) ->
    case embed_batch_sparse([Text], Config) of
        {ok, [SparseVec]} -> {ok, SparseVec};
        {error, _} = Error -> Error
    end.

%% @doc Generate sparse embeddings for multiple texts.
-spec embed_batch_sparse([binary()], map()) -> {ok, [sparse_vector()]} | {error, term()}.
embed_batch_sparse(Texts, #{port := Port, timeout := Timeout}) ->
    Request = #{action => embed, texts => Texts},
    case port_command_sync(Port, Request, Timeout) of
        {ok, #{<<"ok">> := true, <<"embeddings">> := Embeddings}} ->
            SparseVecs = [parse_sparse_vec(E) || E <- Embeddings],
            {ok, SparseVecs};
        {ok, #{<<"ok">> := false, <<"error">> := Err}} ->
            {error, {python_error, Err}};
        {error, Reason} ->
            {error, Reason}
    end;
embed_batch_sparse(_Texts, _Config) ->
    {error, port_not_initialized}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
find_sparse_script() ->
    case code:priv_dir(barrel_vectordb) of
        {error, bad_name} ->
            case filelib:is_file("priv/sparse_server.py") of
                true -> {ok, "priv/sparse_server.py"};
                false ->
                    case filelib:is_file("../priv/sparse_server.py") of
                        true -> {ok, "../priv/sparse_server.py"};
                        false -> {error, script_not_found}
                    end
            end;
        PrivDir ->
            Script = filename:join(PrivDir, "sparse_server.py"),
            case filelib:is_file(Script) of
                true -> {ok, Script};
                false -> {error, script_not_found}
            end
    end.

%% @private
port_command_sync(Port, Request, Timeout) ->
    Json = jsx:encode(Request),
    true = port_command(Port, [Json, "\n"]),

    receive
        {Port, {data, {eol, Line}}} ->
            try
                Response = jsx:decode(Line, [return_maps]),
                {ok, Response}
            catch
                _:Reason ->
                    {error, {json_decode_failed, Reason}}
            end;
        {Port, {exit_status, Status}} ->
            {error, {port_exited, Status}}
    after Timeout ->
        {error, timeout}
    end.

%% @private
validate_model(Model) ->
    ModelBin = to_binary(Model),
    case barrel_vectordb_models:is_known(ModelBin) of
        true -> ok;
        false ->
            error_logger:warning_msg(
                "Model ~s is not in the registry. "
                "It may still work if it's a valid SPLADE model.~n",
                [ModelBin]
            )
    end.

%% @private
to_binary(S) when is_binary(S) -> S;
to_binary(S) when is_list(S) -> list_to_binary(S).

%% @private
parse_sparse_vec(#{<<"indices">> := Indices, <<"values">> := Values}) ->
    #{indices => Indices, values => Values}.

%% @private
%% Convert sparse vector to dense (for compatibility with dense search)
sparse_to_dense(#{indices := Indices, values := Values}, Dim) ->
    %% Initialize zero vector
    Dense = array:new(Dim, {default, 0.0}),
    %% Set non-zero values
    Dense1 = lists:foldl(
        fun({Idx, Val}, Arr) ->
            array:set(Idx, Val, Arr)
        end,
        Dense,
        lists:zip(Indices, Values)
    ),
    array:to_list(Dense1).
