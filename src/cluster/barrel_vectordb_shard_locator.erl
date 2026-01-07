%% @doc Determines which shard a document belongs to.
%%
%% Uses consistent hashing on document ID to distribute
%% documents across shards.
%%
%% @end
-module(barrel_vectordb_shard_locator).

-export([shard_for_key/2, all_shards/1]).

%% @doc Determine which shard a document belongs to
-spec shard_for_key(binary(), pos_integer()) -> non_neg_integer().
shard_for_key(Key, NumShards) when is_binary(Key), NumShards > 0 ->
    Hash = erlang:phash2(Key),
    Hash rem NumShards.

%% @doc Return list of all shard indices
-spec all_shards(pos_integer()) -> [non_neg_integer()].
all_shards(NumShards) when NumShards > 0 ->
    lists:seq(0, NumShards - 1).
