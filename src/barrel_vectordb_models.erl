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
    reload/0
]).

%% Types
-type model_type() :: text | sparse | late_interaction | image | rerank.
-type model_info() :: #{
    name := binary(),
    dimensions => pos_integer(),
    max_tokens => pos_integer(),
    vocab_size => pos_integer(),
    description := binary(),
    source := binary(),
    type => binary(),
    default => boolean()
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
                        Data = jsx:decode(Content, [return_maps]),
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
find_model(_Name, [], {ok, _} = Found) ->
    Found;
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
