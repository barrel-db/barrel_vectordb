%%%-------------------------------------------------------------------
%%% @doc ColBERT late interaction embedding provider
%%%
%%% Uses ColBERT models for multi-vector embeddings. Each document
%%% produces multiple vectors (one per token) for fine-grained matching.
%%%
%%% == Requirements ==
%%% ```
%%% pip install transformers torch
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     python => "python3",                   %% Python executable (default)
%%%     model => "colbert-ir/colbertv2.0",     %% Model name (default, 128 dims)
%%%     timeout => 120000                      %% Timeout in ms (default)
%%% }.
%%% '''
%%%
%%% == Multi-Vector Format ==
%%% Unlike single-vector embeddings, ColBERT produces a list of vectors:
%%% ```
%%% [[0.1, 0.2, ...], [0.3, 0.4, ...], ...]  %% One vector per token
%%% '''
%%%
%%% == Late Interaction ==
%%% ColBERT scoring uses MaxSim:
%%% ```
%%% Score(Q, D) = sum(max(qi · dj for all dj in D) for all qi in Q)
%%% '''
%%% This enables fine-grained token-level matching.
%%%
%%% == Supported Models ==
%%% - `"colbert-ir/colbertv2.0"' - Default, 128 dimensions
%%% - `"answerdotai/answerai-colbert-small-v1"' - 96 dimensions, smaller
%%% - `"jinaai/jina-colbert-v2"' - 128 dimensions, long context (8192 tokens)
%%%
%%% == Use Cases ==
%%% - Fine-grained semantic matching
%%% - Passage retrieval with token-level scoring
%%% - Question answering
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_colbert).
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

%% Multi-vector API
-export([
    embed_multi/2,
    embed_batch_multi/2,
    maxsim_score/2
]).

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "colbert-ir/colbertv2.0").
-define(DEFAULT_TIMEOUT, 120000).
-define(DEFAULT_DIMENSION, 128).

%% Multi-vector type: list of token vectors
-type multi_vector() :: [[float()]].

-export_type([multi_vector/0]).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> colbert.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    Python = maps:get(python, Config, ?DEFAULT_PYTHON),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    %% Validate model against registry (warning only)
    validate_model(Model),

    %% Find the Python script
    ScriptPath = find_colbert_script(),

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
                    {ok, #{<<"ok">> := true, <<"dimensions">> := Dims}} ->
                        NewConfig = Config#{
                            port => Port,
                            dimension => Dims,
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

%% @doc Generate single-vector embedding (mean pooling of token vectors).
%% Note: For ColBERT, use embed_multi/2 to get full multi-vector output.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    case embed_multi(Text, Config) of
        {ok, MultiVec} ->
            {ok, mean_pool(MultiVec)};
        {error, _} = Error ->
            Error
    end.

%% @doc Generate single-vector embeddings for batch (mean pooling).
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, Config) ->
    case embed_batch_multi(Texts, Config) of
        {ok, MultiVecs} ->
            {ok, [mean_pool(MV) || MV <- MultiVecs]};
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Multi-Vector API
%%====================================================================

%% @doc Generate multi-vector embedding for a single text.
%% Returns a list of token vectors.
-spec embed_multi(binary(), map()) -> {ok, multi_vector()} | {error, term()}.
embed_multi(Text, Config) ->
    case embed_batch_multi([Text], Config) of
        {ok, [MultiVec]} -> {ok, MultiVec};
        {error, _} = Error -> Error
    end.

%% @doc Generate multi-vector embeddings for multiple texts.
-spec embed_batch_multi([binary()], map()) -> {ok, [multi_vector()]} | {error, term()}.
embed_batch_multi(Texts, #{port := Port, timeout := Timeout}) ->
    Request = #{action => embed, texts => Texts},
    case port_command_sync(Port, Request, Timeout) of
        {ok, #{<<"ok">> := true, <<"embeddings">> := Embeddings}} ->
            {ok, Embeddings};
        {ok, #{<<"ok">> := false, <<"error">> := Err}} ->
            {error, {python_error, Err}};
        {error, Reason} ->
            {error, Reason}
    end;
embed_batch_multi(_Texts, _Config) ->
    {error, port_not_initialized}.

%% @doc Calculate MaxSim score between query and document multi-vectors.
%% This is the standard ColBERT scoring function.
%% Score = sum(max(qi · dj for all dj in D) for all qi in Q)
-spec maxsim_score(multi_vector(), multi_vector()) -> float().
maxsim_score(QueryVecs, DocVecs) ->
    lists:sum([max_dot_product(QVec, DocVecs) || QVec <- QueryVecs]).

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
find_colbert_script() ->
    case code:priv_dir(barrel_vectordb) of
        {error, bad_name} ->
            case filelib:is_file("priv/colbert_server.py") of
                true -> {ok, "priv/colbert_server.py"};
                false ->
                    case filelib:is_file("../priv/colbert_server.py") of
                        true -> {ok, "../priv/colbert_server.py"};
                        false -> {error, script_not_found}
                    end
            end;
        PrivDir ->
            Script = filename:join(PrivDir, "colbert_server.py"),
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
                "It may still work if it's a valid ColBERT model.~n",
                [ModelBin]
            )
    end.

%% @private
to_binary(S) when is_binary(S) -> S;
to_binary(S) when is_list(S) -> list_to_binary(S).

%% @private
%% Mean pooling of token vectors to get single vector
mean_pool([]) -> [];
mean_pool(Vectors) ->
    N = length(Vectors),
    Dim = length(hd(Vectors)),
    %% Sum all vectors element-wise
    Sums = lists:foldl(
        fun(Vec, Acc) ->
            lists:zipwith(fun(A, B) -> A + B end, Vec, Acc)
        end,
        lists:duplicate(Dim, 0.0),
        Vectors
    ),
    %% Divide by N
    [S / N || S <- Sums].

%% @private
%% Find maximum dot product between query vector and all doc vectors
max_dot_product(QueryVec, DocVecs) ->
    DotProducts = [dot_product(QueryVec, DocVec) || DocVec <- DocVecs],
    lists:max(DotProducts).

%% @private
dot_product(V1, V2) ->
    lists:sum(lists:zipwith(fun(A, B) -> A * B end, V1, V2)).
