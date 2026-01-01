%%%-------------------------------------------------------------------
%%% @doc Cross-encoder reranking module
%%%
%%% Provides reranking of search results using cross-encoder models.
%%% Cross-encoders score query-document pairs directly, providing more
%%% accurate relevance scores than bi-encoder similarity.
%%%
%%% == Requirements ==
%%% ```
%%% pip install transformers torch
%%% '''
%%%
%%% == Usage ==
%%% ```
%%% %% Initialize reranker
%%% {ok, State} = barrel_vectordb_rerank:init(#{}).
%%%
%%% %% Rerank search results
%%% Query = <<"What is machine learning?">>,
%%% Documents = [
%%%     <<"Machine learning is a subset of AI...">>,
%%%     <<"Deep learning uses neural networks...">>,
%%%     <<"Python is a programming language...">>
%%% ],
%%% {ok, Ranked} = barrel_vectordb_rerank:rerank(Query, Documents, State),
%%% %% Returns: [{0, 0.95}, {1, 0.82}, {2, 0.15}]
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     python => "python3",                               %% Python executable
%%%     model => "cross-encoder/ms-marco-MiniLM-L-6-v2",   %% Model name
%%%     timeout => 120000                                  %% Timeout in ms
%%% }.
%%% '''
%%%
%%% == Supported Models ==
%%% - `"cross-encoder/ms-marco-MiniLM-L-6-v2"' - Default, fast, good quality
%%% - `"cross-encoder/ms-marco-MiniLM-L-12-v2"' - Better quality, slower
%%% - `"BAAI/bge-reranker-base"' - Good quality
%%% - `"BAAI/bge-reranker-large"' - Best quality, slowest
%%%
%%% == Integration with Search ==
%%% Typical two-stage retrieval:
%%% ```
%%% %% Stage 1: Fast vector search (top 100)
%%% {ok, Candidates} = barrel_vectordb:search(Store, Query, #{k => 100}),
%%%
%%% %% Stage 2: Rerank top candidates
%%% Docs = [maps:get(text, C) || C <- Candidates],
%%% {ok, Ranked} = barrel_vectordb_rerank:rerank(Query, Docs, RerankerState),
%%%
%%% %% Get top 10 after reranking
%%% Top10Indices = [Idx || {Idx, _Score} <- lists:sublist(Ranked, 10)],
%%% Top10 = [lists:nth(Idx + 1, Candidates) || Idx <- Top10Indices].
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_rerank).

-export([
    init/1,
    rerank/3,
    rerank/4,
    available/1,
    stop/1
]).

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "cross-encoder/ms-marco-MiniLM-L-6-v2").
-define(DEFAULT_TIMEOUT, 120000).

-type rerank_state() :: #{
    port := port(),
    model := binary(),
    timeout := pos_integer()
}.

-type rerank_result() :: {Index :: non_neg_integer(), Score :: float()}.

-export_type([rerank_state/0, rerank_result/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Initialize the reranker.
-spec init(map()) -> {ok, rerank_state()} | {error, term()}.
init(Config) ->
    Python = maps:get(python, Config, ?DEFAULT_PYTHON),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    %% Validate model against registry (warning only)
    validate_model(Model),

    %% Find the Python script
    ScriptPath = find_rerank_script(),

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
                    {ok, #{<<"ok">> := true, <<"model">> := ModelName}} ->
                        State = #{
                            port => Port,
                            model => ModelName,
                            timeout => Timeout
                        },
                        {ok, State};
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

%% @doc Rerank documents by relevance to query.
%% Returns list of {Index, Score} tuples sorted by score descending.
-spec rerank(binary(), [binary()], rerank_state()) ->
    {ok, [rerank_result()]} | {error, term()}.
rerank(Query, Documents, State) ->
    rerank(Query, Documents, #{}, State).

%% @doc Rerank documents with options.
%% Options:
%%   - top_k: Limit number of results returned
-spec rerank(binary(), [binary()], map(), rerank_state()) ->
    {ok, [rerank_result()]} | {error, term()}.
rerank(Query, Documents, Options, #{port := Port, timeout := Timeout}) ->
    Request = #{
        action => rerank,
        query => Query,
        documents => Documents
    },
    Request1 = case maps:get(top_k, Options, undefined) of
        undefined -> Request;
        TopK -> Request#{top_k => TopK}
    end,
    case port_command_sync(Port, Request1, Timeout) of
        {ok, #{<<"ok">> := true, <<"results">> := Results}} ->
            Parsed = [{maps:get(<<"index">>, R), maps:get(<<"score">>, R)} || R <- Results],
            {ok, Parsed};
        {ok, #{<<"ok">> := false, <<"error">> := Err}} ->
            {error, {python_error, Err}};
        {error, Reason} ->
            {error, Reason}
    end;
rerank(_Query, _Documents, _Options, _State) ->
    {error, not_initialized}.

%% @doc Check if reranker is available.
-spec available(rerank_state() | map()) -> boolean().
available(#{port := Port}) ->
    erlang:port_info(Port) =/= undefined;
available(_) ->
    false.

%% @doc Stop the reranker and close the port.
-spec stop(rerank_state()) -> ok.
stop(#{port := Port}) ->
    catch port_close(Port),
    ok;
stop(_) ->
    ok.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
find_rerank_script() ->
    case code:priv_dir(barrel_vectordb) of
        {error, bad_name} ->
            case filelib:is_file("priv/rerank_server.py") of
                true -> {ok, "priv/rerank_server.py"};
                false ->
                    case filelib:is_file("../priv/rerank_server.py") of
                        true -> {ok, "../priv/rerank_server.py"};
                        false -> {error, script_not_found}
                    end
            end;
        PrivDir ->
            Script = filename:join(PrivDir, "rerank_server.py"),
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
                "It may still work if it's a valid reranking model.~n",
                [ModelBin]
            )
    end.

%% @private
to_binary(S) when is_binary(S) -> S;
to_binary(S) when is_list(S) -> list_to_binary(S).
