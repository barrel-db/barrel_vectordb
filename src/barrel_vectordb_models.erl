%%%-------------------------------------------------------------------
%%% @doc Model registry for barrel_vectordb
%%%
%%% Provides information about supported embedding models including
%%% dimensions, max tokens, and descriptions.
%%%
%%% == Usage ==
%%% ```
%%% %% List all text embedding models
%%% {ok, Models} = barrel_vectordb_models:list(text).
%%%
%%% %% Get info about a specific model
%%% {ok, Info} = barrel_vectordb_models:info(<<"BAAI/bge-base-en-v1.5">>).
%%%
%%% %% Get the default text model
%%% {ok, Model} = barrel_vectordb_models:default(text).
%%%
%%% %% Get dimensions for a model
%%% {ok, 768} = barrel_vectordb_models:dimensions(<<"BAAI/bge-base-en-v1.5">>).
%%% '''
%%%
%%% == Model Types ==
%%% - `text' - Dense text embedding models
%%% - `sparse' - Sparse embedding models (BM25, SPLADE)
%%% - `late_interaction' - Late interaction models (ColBERT)
%%% - `image' - Image embedding models (CLIP, ResNet)
%%% - `rerank' - Reranking cross-encoder models
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(barrel_vectordb_models).

%% API
-export([
    list/0,
    list/1,
    info/1,
    default/1,
    dimensions/1,
    types/0,
    reload/0,
    %% Provider integration
    embedder_config/1,
    embedder_config/2,
    is_known/1,
    model_type/1
]).

%% Types
-type model_type() :: text | sparse | late_interaction | image | rerank.
%% Model info maps use binary keys as returned by JSON decoder
-type model_info() :: #{
    binary() => term()
}.

-export_type([model_type/0, model_info/0]).

%% Persistent term key for caching
-define(CACHE_KEY, {?MODULE, models_cache}).

%%====================================================================
%% API
%%====================================================================

%% @doc List all available model types.
-spec types() -> [model_type()].
types() ->
    [text, sparse, late_interaction, image, rerank].

%% @doc List all models across all types.
-spec list() -> {ok, #{model_type() => [model_info()]}} | {error, term()}.
list() ->
    case ensure_loaded() of
        {ok, #{<<"models">> := Models}} ->
            Result = maps:fold(
                fun(TypeBin, ModelList, Acc) ->
                    Type = binary_to_existing_atom(TypeBin, utf8),
                    Acc#{Type => ModelList}
                end,
                #{},
                Models
            ),
            {ok, Result};
        {error, _} = Error ->
            Error
    end.

%% @doc List models of a specific type.
-spec list(model_type()) -> {ok, [model_info()]} | {error, term()}.
list(Type) when is_atom(Type) ->
    case ensure_loaded() of
        {ok, #{<<"models">> := Models}} ->
            TypeBin = atom_to_binary(Type, utf8),
            case maps:get(TypeBin, Models, undefined) of
                undefined ->
                    {error, {unknown_type, Type}};
                ModelList ->
                    {ok, ModelList}
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Get information about a specific model by name.
-spec info(binary() | string()) -> {ok, model_info()} | {error, term()}.
info(Name) when is_list(Name) ->
    info(list_to_binary(Name));
info(Name) when is_binary(Name) ->
    case ensure_loaded() of
        {ok, #{<<"models">> := Models}} ->
            find_model(Name, Models);
        {error, _} = Error ->
            Error
    end.

%% @doc Get the default model for a type.
-spec default(model_type()) -> {ok, model_info()} | {error, term()}.
default(Type) when is_atom(Type) ->
    case list(Type) of
        {ok, ModelList} ->
            case find_default(ModelList) of
                {ok, _} = Result ->
                    Result;
                {error, no_default} ->
                    %% Return first model if no default specified
                    case ModelList of
                        [First | _] -> {ok, First};
                        [] -> {error, {no_models, Type}}
                    end
            end;
        {error, _} = Error ->
            Error
    end.

%% @doc Get dimensions for a model.
-spec dimensions(binary() | string()) -> {ok, pos_integer()} | {error, term()}.
dimensions(Name) ->
    case info(Name) of
        {ok, #{<<"dimensions">> := Dims}} ->
            {ok, Dims};
        {ok, _} ->
            {error, no_dimensions};
        {error, _} = Error ->
            Error
    end.

%% @doc Reload the model catalog from disk.
-spec reload() -> ok | {error, term()}.
reload() ->
    case load_models_file() of
        {ok, Data} ->
            persistent_term:put(?CACHE_KEY, Data),
            ok;
        {error, _} = Error ->
            Error
    end.

%% @doc Get embedder configuration for a model.
%% Returns a ready-to-use embedder config tuple for barrel_vectordb:start_link/1.
%%
%% Example:
%% ```
%% {ok, Config} = barrel_vectordb_models:embedder_config(<<"BAAI/bge-small-en-v1.5">>).
%% %% => {local, #{model => <<"BAAI/bge-small-en-v1.5">>, dimensions => 384}}
%% '''
-spec embedder_config(binary() | string()) -> {ok, {atom(), map()}} | {error, term()}.
embedder_config(Name) ->
    embedder_config(Name, #{}).

%% @doc Get embedder configuration with additional options.
%% Options are merged with the base config from the model registry.
%%
%% Example:
%% ```
%% {ok, Config} = barrel_vectordb_models:embedder_config(
%%     <<"BAAI/bge-small-en-v1.5">>,
%%     #{python => "/usr/bin/python3", timeout => 60000}
%% ).
%% '''
-spec embedder_config(binary() | string(), map()) -> {ok, {atom(), map()}} | {error, term()}.
embedder_config(Name, Options) when is_list(Name) ->
    embedder_config(list_to_binary(Name), Options);
embedder_config(Name, Options) when is_binary(Name), is_map(Options) ->
    case info(Name) of
        {ok, ModelInfo} ->
            Config = build_embedder_config(Name, ModelInfo, Options),
            {ok, Config};
        {error, _} = Error ->
            Error
    end.

%% @doc Check if a model is known in the registry.
-spec is_known(binary() | string()) -> boolean().
is_known(Name) ->
    case info(Name) of
        {ok, _} -> true;
        {error, _} -> false
    end.

%% @doc Get the type of a model (text, sparse, image, etc).
-spec model_type(binary() | string()) -> {ok, model_type()} | {error, term()}.
model_type(Name) when is_list(Name) ->
    model_type(list_to_binary(Name));
model_type(Name) when is_binary(Name) ->
    case ensure_loaded() of
        {ok, #{<<"models">> := Models}} ->
            find_model_type(Name, Models);
        {error, _} = Error ->
            Error
    end.

%%====================================================================
%% Internal Functions
%%====================================================================

%% @private
ensure_loaded() ->
    case persistent_term:get(?CACHE_KEY, undefined) of
        undefined ->
            case load_models_file() of
                {ok, Data} ->
                    persistent_term:put(?CACHE_KEY, Data),
                    {ok, Data};
                {error, _} = Error ->
                    Error
            end;
        Data ->
            {ok, Data}
    end.

%% @private
load_models_file() ->
    case find_models_file() of
        {ok, Path} ->
            case file:read_file(Path) of
                {ok, Content} ->
                    try
                        Data = json:decode(Content),
                        {ok, Data}
                    catch
                        _:Reason ->
                            {error, {json_decode_failed, Reason}}
                    end;
                {error, Reason} ->
                    {error, {file_read_failed, Reason}}
            end;
        {error, _} = Error ->
            Error
    end.

%% @private
find_models_file() ->
    case code:priv_dir(barrel_vectordb) of
        {error, bad_name} ->
            %% Development mode - try relative path
            Paths = ["priv/models.json", "../priv/models.json"],
            find_first_file(Paths);
        PrivDir ->
            Path = filename:join(PrivDir, "models.json"),
            case filelib:is_file(Path) of
                true -> {ok, Path};
                false -> {error, models_file_not_found}
            end
    end.

%% @private
find_first_file([]) ->
    {error, models_file_not_found};
find_first_file([Path | Rest]) ->
    case filelib:is_file(Path) of
        true -> {ok, Path};
        false -> find_first_file(Rest)
    end.

%% @private
find_model(Name, Models) ->
    find_model(Name, maps:to_list(Models), undefined).

find_model(_Name, [], undefined) ->
    {error, model_not_found};
find_model(Name, [{_Type, ModelList} | Rest], Acc) ->
    case find_in_list(Name, ModelList) of
        {ok, _} = Found ->
            Found;
        not_found ->
            find_model(Name, Rest, Acc)
    end.

%% @private
find_in_list(_Name, []) ->
    not_found;
find_in_list(Name, [#{<<"name">> := Name} = Model | _]) ->
    {ok, Model};
find_in_list(Name, [_ | Rest]) ->
    find_in_list(Name, Rest).

%% @private
find_default([]) ->
    {error, no_default};
find_default([#{<<"default">> := true} = Model | _]) ->
    {ok, Model};
find_default([_ | Rest]) ->
    find_default(Rest).

%% @private
%% Build embedder config based on model type
build_embedder_config(Name, ModelInfo, Options) ->
    %% Get dimensions if available
    BaseConfig = case maps:get(<<"dimensions">>, ModelInfo, undefined) of
        undefined -> #{model => Name};
        Dims -> #{model => Name, dimensions => Dims}
    end,
    %% Merge with user options (user options take precedence)
    MergedConfig = maps:merge(BaseConfig, Options),
    %% Return as local provider config (default for HuggingFace models)
    {local, MergedConfig}.

%% @private
find_model_type(Name, Models) ->
    find_model_type(Name, maps:to_list(Models), undefined).

find_model_type(_Name, [], undefined) ->
    {error, model_not_found};
find_model_type(Name, [{TypeBin, ModelList} | Rest], _Acc) ->
    case has_model(Name, ModelList) of
        true ->
            Type = binary_to_existing_atom(TypeBin, utf8),
            {ok, Type};
        false ->
            find_model_type(Name, Rest, undefined)
    end.

%% @private
has_model(_Name, []) ->
    false;
has_model(Name, [#{<<"name">> := Name} | _]) ->
    true;
has_model(Name, [_ | Rest]) ->
    has_model(Name, Rest).
