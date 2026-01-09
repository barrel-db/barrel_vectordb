%%%-------------------------------------------------------------------
%%% @doc CLIP image/text embedding provider
%%%
%%% Uses CLIP (Contrastive Language-Image Pre-training) models for
%%% cross-modal embeddings. Both images and text are encoded into the
%%% same vector space, enabling image-text similarity search.
%%%
%%% == Requirements ==
%%% ```
%%% pip install transformers torch pillow
%%% '''
%%%
%%% == Configuration ==
%%% ```
%%% Config = #{
%%%     python => "python3",                       %% Python executable (default)
%%%     model => "openai/clip-vit-base-patch32",   %% Model name (default)
%%%     timeout => 120000                          %% Timeout in ms (default)
%%% }.
%%% '''
%%%
%%% == Cross-Modal Search ==
%%% CLIP enables searching images with text queries and vice versa:
%%% ```
%%% %% Embed an image
%%% {ok, ImgVec} = embed_image(ImageBase64, Config),
%%%
%%% %% Embed a text query (in same space!)
%%% {ok, TextVec} = embed(<<"a photo of a cat">>, Config),
%%%
%%% %% Now you can compare ImgVec and TextVec with cosine similarity
%%% '''
%%%
%%% == Supported Models ==
%%% - `"openai/clip-vit-base-patch32"' - Default, 512 dimensions, fast
%%% - `"openai/clip-vit-base-patch16"' - 512 dimensions, higher quality
%%% - `"openai/clip-vit-large-patch14"' - 768 dimensions, best quality
%%% - `"laion/CLIP-ViT-B-32-laion2B-s34B-b79K"' - 512 dims, LAION trained
%%%
%%% == Use Cases ==
%%% - Image search with text queries
%%% - Finding similar images
%%% - Multi-modal content retrieval
%%% - Zero-shot image classification
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_embed_clip).
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

%% Image embedding API
-export([
    embed_image/2,
    embed_image_batch/2
]).

-define(DEFAULT_PYTHON, "python3").
-define(DEFAULT_MODEL, "openai/clip-vit-base-patch32").
-define(DEFAULT_TIMEOUT, 120000).
-define(DEFAULT_DIMENSION, 512).

%%====================================================================
%% Behaviour Callbacks
%%====================================================================

%% @doc Provider name.
-spec name() -> atom().
name() -> clip.

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
    ScriptPath = find_image_script(),

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

%% @doc Generate text embedding (for cross-modal search).
%% Text embeddings are in the same space as image embeddings.
-spec embed(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed(Text, Config) ->
    case embed_batch([Text], Config) of
        {ok, [Embedding]} -> {ok, Embedding};
        {error, _} = Error -> Error
    end.

%% @doc Generate text embeddings for batch.
-spec embed_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_batch(Texts, #{port := Port, timeout := Timeout}) ->
    case barrel_vectordb_python_queue:acquire(Timeout) of
        ok ->
            try
                Request = #{action => embed_text, texts => Texts},
                case port_command_sync(Port, Request, Timeout) of
                    {ok, #{<<"ok">> := true, <<"embeddings">> := Embeddings}} ->
                        {ok, Embeddings};
                    {ok, #{<<"ok">> := false, <<"error">> := Err}} ->
                        {error, {python_error, Err}};
                    {error, Reason} ->
                        {error, Reason}
                end
            after
                barrel_vectordb_python_queue:release()
            end;
        {error, timeout} ->
            {error, queue_timeout}
    end;
embed_batch(_Texts, _Config) ->
    {error, port_not_initialized}.

%%====================================================================
%% Image Embedding API
%%====================================================================

%% @doc Generate embedding for a single image.
%% Image should be base64-encoded.
-spec embed_image(binary(), map()) -> {ok, [float()]} | {error, term()}.
embed_image(ImageBase64, Config) ->
    case embed_image_batch([ImageBase64], Config) of
        {ok, [Embedding]} -> {ok, Embedding};
        {error, _} = Error -> Error
    end.

%% @doc Generate embeddings for multiple images.
%% Images should be base64-encoded.
-spec embed_image_batch([binary()], map()) -> {ok, [[float()]]} | {error, term()}.
embed_image_batch(Images, #{port := Port, timeout := Timeout}) ->
    case barrel_vectordb_python_queue:acquire(Timeout) of
        ok ->
            try
                Request = #{action => embed_image, images => Images},
                case port_command_sync(Port, Request, Timeout) of
                    {ok, #{<<"ok">> := true, <<"embeddings">> := Embeddings}} ->
                        {ok, Embeddings};
                    {ok, #{<<"ok">> := false, <<"error">> := Err}} ->
                        {error, {python_error, Err}};
                    {error, Reason} ->
                        {error, Reason}
                end
            after
                barrel_vectordb_python_queue:release()
            end;
        {error, timeout} ->
            {error, queue_timeout}
    end;
embed_image_batch(_Images, _Config) ->
    {error, port_not_initialized}.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
find_image_script() ->
    case code:priv_dir(barrel_vectordb) of
        {error, bad_name} ->
            case filelib:is_file("priv/image_server.py") of
                true -> {ok, "priv/image_server.py"};
                false ->
                    case filelib:is_file("../priv/image_server.py") of
                        true -> {ok, "../priv/image_server.py"};
                        false -> {error, script_not_found}
                    end
            end;
        PrivDir ->
            Script = filename:join(PrivDir, "image_server.py"),
            case filelib:is_file(Script) of
                true -> {ok, Script};
                false -> {error, script_not_found}
            end
    end.

%% @private
port_command_sync(Port, Request, Timeout) ->
    Json = iolist_to_binary(json:encode(Request)),
    true = port_command(Port, [Json, "\n"]),

    receive
        {Port, {data, {eol, Line}}} ->
            try
                Response = json:decode(Line),
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
                "It may still work if it's a valid CLIP model.~n",
                [ModelBin]
            )
    end.

%% @private
to_binary(S) when is_binary(S) -> S;
to_binary(S) when is_list(S) -> list_to_binary(S).
