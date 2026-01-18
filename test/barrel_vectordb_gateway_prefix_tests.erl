-module(barrel_vectordb_gateway_prefix_tests).
-include_lib("eunit/include/eunit.hrl").

%%====================================================================
%% Hash Function Tests
%%====================================================================

tenant_hash_test_() ->
    [
        {"hash returns 4-char hex string", fun() ->
            Hash = barrel_vectordb_gateway:tenant_hash(<<"acme">>),
            ?assertEqual(4, byte_size(Hash)),
            %% Should be valid hex (io_lib:fread returns {ok, [Value], ""} on success)
            ?assertMatch({ok, [_], []}, io_lib:fread("~16u", binary_to_list(Hash)))
        end},

        {"hash is deterministic", fun() ->
            Hash1 = barrel_vectordb_gateway:tenant_hash(<<"tenant123">>),
            Hash2 = barrel_vectordb_gateway:tenant_hash(<<"tenant123">>),
            ?assertEqual(Hash1, Hash2)
        end},

        {"different tenants have different hashes (usually)", fun() ->
            %% Note: This is probabilistic but very unlikely to collide
            Hashes = [barrel_vectordb_gateway:tenant_hash(list_to_binary("tenant_" ++ integer_to_list(I)))
                      || I <- lists:seq(1, 100)],
            UniqueHashes = lists:usort(Hashes),
            %% Should have mostly unique hashes
            ?assert(length(UniqueHashes) > 90)
        end},

        {"hash handles special characters", fun() ->
            Hash1 = barrel_vectordb_gateway:tenant_hash(<<"tenant-with-dash">>),
            Hash2 = barrel_vectordb_gateway:tenant_hash(<<"tenant_with_underscore">>),
            Hash3 = barrel_vectordb_gateway:tenant_hash(<<"tenant.with.dots">>),
            ?assertEqual(4, byte_size(Hash1)),
            ?assertEqual(4, byte_size(Hash2)),
            ?assertEqual(4, byte_size(Hash3))
        end},

        {"hash handles unicode", fun() ->
            Hash = barrel_vectordb_gateway:tenant_hash(<<"тенант">>),
            ?assertEqual(4, byte_size(Hash))
        end}
    ].

%%====================================================================
%% Prefix Collection Tests
%%====================================================================

prefix_collection_test_() ->
    [
        {"prefix_collection creates correct format", fun() ->
            Prefixed = barrel_vectordb_gateway:prefix_collection(<<"acme">>, <<"documents">>),
            %% Format: {4-char-hash}_{tenant}_{collection}
            ?assertMatch(<<_:4/binary, "_acme_documents">>, Prefixed)
        end},

        {"prefix_collection is deterministic", fun() ->
            P1 = barrel_vectordb_gateway:prefix_collection(<<"tenant1">>, <<"col1">>),
            P2 = barrel_vectordb_gateway:prefix_collection(<<"tenant1">>, <<"col1">>),
            ?assertEqual(P1, P2)
        end},

        {"different collections have different prefixes", fun() ->
            P1 = barrel_vectordb_gateway:prefix_collection(<<"tenant1">>, <<"col1">>),
            P2 = barrel_vectordb_gateway:prefix_collection(<<"tenant1">>, <<"col2">>),
            ?assertNotEqual(P1, P2),
            %% But same hash (same tenant)
            Hash1 = binary:part(P1, 0, 4),
            Hash2 = binary:part(P2, 0, 4),
            ?assertEqual(Hash1, Hash2)
        end},

        {"different tenants have different hash prefixes", fun() ->
            P1 = barrel_vectordb_gateway:prefix_collection(<<"tenant1">>, <<"collection">>),
            P2 = barrel_vectordb_gateway:prefix_collection(<<"tenant2">>, <<"collection">>),
            ?assertNotEqual(P1, P2)
        end}
    ].

%%====================================================================
%% Strip Prefix Tests
%%====================================================================

strip_prefix_test_() ->
    [
        {"strip_prefix removes prefix correctly", fun() ->
            TenantId = <<"mycompany">>,
            Collection = <<"users">>,
            Prefixed = barrel_vectordb_gateway:prefix_collection(TenantId, Collection),

            Stripped = barrel_vectordb_gateway:strip_prefix(Prefixed, TenantId),
            ?assertEqual(Collection, Stripped)
        end},

        {"strip_prefix is inverse of prefix_collection", fun() ->
            TenantId = <<"tenant_xyz">>,
            Collection = <<"my_collection">>,

            Prefixed = barrel_vectordb_gateway:prefix_collection(TenantId, Collection),
            Stripped = barrel_vectordb_gateway:strip_prefix(Prefixed, TenantId),

            ?assertEqual(Collection, Stripped)
        end},

        {"strip_prefix returns original if prefix doesn't match", fun() ->
            FullName = <<"some_random_name">>,
            TenantId = <<"other_tenant">>,

            Result = barrel_vectordb_gateway:strip_prefix(FullName, TenantId),
            ?assertEqual(FullName, Result)
        end},

        {"strip_prefix handles different tenant", fun() ->
            TenantA = <<"tenant_a">>,
            TenantB = <<"tenant_b">>,
            Collection = <<"shared_name">>,

            PrefixedA = barrel_vectordb_gateway:prefix_collection(TenantA, Collection),

            %% Stripping with wrong tenant should not strip anything
            ResultB = barrel_vectordb_gateway:strip_prefix(PrefixedA, TenantB),
            ?assertEqual(PrefixedA, ResultB),

            %% Stripping with correct tenant should work
            ResultA = barrel_vectordb_gateway:strip_prefix(PrefixedA, TenantA),
            ?assertEqual(Collection, ResultA)
        end}
    ].

%%====================================================================
%% Integration Tests
%%====================================================================

roundtrip_test_() ->
    [
        {"roundtrip with various tenant/collection names", fun() ->
            TestCases = [
                {<<"simple">>, <<"test">>},
                {<<"tenant-with-dashes">>, <<"collection_with_underscores">>},
                {<<"t">>, <<"c">>},  % Single char
                {<<"long_tenant_name_here">>, <<"even_longer_collection_name_here">>},
                {<<"123numeric">>, <<"456numbers">>}
            ],

            lists:foreach(fun({TenantId, Collection}) ->
                Prefixed = barrel_vectordb_gateway:prefix_collection(TenantId, Collection),
                Stripped = barrel_vectordb_gateway:strip_prefix(Prefixed, TenantId),
                ?assertEqual(Collection, Stripped)
            end, TestCases)
        end},

        {"prefix groups same tenant collections", fun() ->
            TenantId = <<"grouped_tenant">>,
            Collections = [<<"c1">>, <<"c2">>, <<"c3">>, <<"c4">>, <<"c5">>],

            Prefixed = [barrel_vectordb_gateway:prefix_collection(TenantId, C) || C <- Collections],

            %% All should have the same first 4 characters (the hash)
            Hashes = [binary:part(P, 0, 4) || P <- Prefixed],
            UniqueHashes = lists:usort(Hashes),
            ?assertEqual(1, length(UniqueHashes))
        end}
    ].
