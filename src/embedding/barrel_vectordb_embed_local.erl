%%%-------------------------------------------------------------------
%%% @doc Local Python embedding provider
%%%
%%% Uses a Python port with sentence-transformers for CPU-based embeddings.
%%% No GPU required, runs entirely on CPU.
%%%
%%% == Requirements ==
%%% ```
%%% pip install sentence-transformers
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     python => "python3",                     %% Python executable (default)
%%%     model => "BAAI/bge-base-en-v1.5",        %% Model name (default, 768 dims)
%%%     timeout => 120000                        %% Timeout in ms (default)
%%% }.
%%% '''
%%%
%%% == Supported Models ==
%%% Any model from sentence-transformers or HuggingFace:
%%%
%%% - `"BAAI/bge-base-en-v1.5"' - Default, 768 dimensions, good quality/speed balance
%%% - `"BAAI/bge-small-en-v1.5"' - 384 dimensions, faster, slightly lower quality
%%% - `"BAAI/bge-large-en-v1.5"' - 1024 dimensions, best quality, slower
%%% - `"sentence-transformers/all-MiniLM-L6-v2"' - 384 dims, fast, general purpose
%%% - `"sentence-transformers/all-mpnet-base-v2"' - 768 dims, high quality
%%% - `"nomic-ai/nomic-embed-text-v1.5"' - 768 dims, long context (8192 tokens)
%%%
%%% Note: The dimension is auto-detected from the model on initialization.
%%%
%%% == Protocol ==
%%% Communication via stdin/stdout using JSON lines:
%%% ```
%%% -> {"action": "info"}
%%% <- {"ok": true, "dimensions": 768, "model": "..."}
%%%
%%% -> {"action": "embed", "texts": ["hello", "world"]}
%%% <- {"ok": true, "embeddings": [[...], [...]]}
%%% '''
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_local).
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

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "BAAI/bge-base-en-v1.5").
-define(DEFAULT_TIMEOUT, 120000).
-define(DEFAULT_DIMENSION, 768).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> local.

%% @doc Get dimension for this provider.
-spec dimension(map()) -> pos_integer().
dimension(Config) ->
    maps:get(dimension, Config, ?DEFAULT_DIMENSION).

%% @doc Initialize the provider.
%% Starts the Python port if not already running.
-spec init(map()) -> {ok, map()} | {error, term()}.
init(Config) ->
    Python = maps:get(python, Config, ?DEFAULT_PYTHON),
    Model = maps:get(model, Config, ?DEFAULT_MODEL),
    Timeout = maps:get(timeout, Config, ?DEFAULT_TIMEOUT),

    %% Find the Python script
    ScriptPath = find_embed_script(),

    case ScriptPath of
        {ok, Script} ->
            %% Start the port
            PortOpts = [
                {args, [Script, Model]},
                {line, 10000000},  %% Large buffer for embeddings
                binary,
                use_stdio,
                exit_status
            ],
            try
                Port = open_port({spawn_executable, Python}, PortOpts),
                %% Get info to verify it's working
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

%% @doc Generate embedding for a single text.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    case embed_batch([Text], Config) of
        {ok, [Vector]} -> {ok, Vector};
        {error, _} = Error -> Error
    end.

%% @doc Generate embeddings for multiple texts.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, #{port := Port, timeout := Timeout}) ->
    Request = #{action => embed, texts => Texts},
    case port_command_sync(Port, Request, Timeout) of
        {ok, #{<<"ok">> := true, <<"embeddings">> := Embeddings}} ->
            {ok, Embeddings};
        {ok, #{<<"ok">> := false, <<"error">> := Err}} ->
            {error, {python_error, Err}};
        {error, Reason} ->
            {error, Reason}
    end;
embed_batch(_Texts, _Config) ->
    {error, port_not_initialized}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
find_embed_script() ->
    %% Try to find the embed script in priv
    case code:priv_dir(barrel_vectordb) of
        {error, bad_name} ->
            %% Development mode - try relative path
            case filelib:is_file("priv/embed_server.py") of
                true -> {ok, "priv/embed_server.py"};
                false ->
                    case filelib:is_file("../priv/embed_server.py") of
                        true -> {ok, "../priv/embed_server.py"};
                        false -> {error, script_not_found}
                    end
            end;
        PrivDir ->
            Script = filename:join(PrivDir, "embed_server.py"),
            case filelib:is_file(Script) of
                true -> {ok, Script};
                false -> {error, script_not_found}
            end
    end.

%% @private
%% Send command to port and wait for response
port_command_sync(Port, Request, Timeout) ->
    %% Encode and send
    Json = jsx:encode(Request),
    true = port_command(Port, [Json, "\n"]),

    %% Wait for response
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
