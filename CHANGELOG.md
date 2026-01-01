# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0] - 2026-01-01

### Added

- **Search options**: `include_text` and `include_metadata` options to skip unnecessary RocksDB lookups
- **Search option**: `ef_search` option to control search width at query time
- **Batch vector API**: `add_vector_batch/2` for efficient bulk vector insertion
- **Checkpoint API**: `checkpoint/1` for manual HNSW index persistence
- **gen_batch_server integration**: Automatic write batching for improved throughput
- **Benchmark framework**: Performance benchmark suite in `bench/` directory
- **Multi-writer tests**: Concurrent writer stress tests
- **Improved documentation**: Comprehensive README with all API functions and options

### Changed

- **HNSW search optimization**: Replaced `lists:sort/1` with `gb_trees` for O(log N) candidate management instead of O(N log N)
- **Batch RocksDB lookups**: Search now uses `rocksdb:multi_get/4` instead of sequential `rocksdb:get/3` calls
- **Vector storage**: Use float32 for vector storage (50% size reduction)
- **HNSW persistence**: Deferred HNSW persistence for faster inserts
- **Dependencies**: Updated `rocksdb` from 2.0.0 to 2.2.0 for `multi_get` support

### Fixed

- **Benchmark warmup**: Fixed store reset between warmup and actual benchmark runs
- **Search latency variance**: Reduced max search latency from 670ms to sub-10ms by fixing cold-start and optimizing lookups

### Performance

These changes significantly reduced search latency variance:

| Metric | Before | After |
|--------|--------|-------|
| P50 | 1.3ms | ~1ms |
| Max | 670ms | <10ms |
| Variance | 500x | <10x |

Key optimizations:
1. Fixed benchmark warmup keeping HNSW index warm
2. Batch RocksDB lookups with `multi_get` (2 calls per result -> 2 total)
3. Skip text/metadata lookups with `include_text => false`
4. O(log N) HNSW candidate management with `gb_trees`

## [1.0.0] - 2025-12-01

### Added

- Initial release
- RocksDB-backed vector storage with column families
- HNSW approximate nearest neighbor search
- 8-bit vector quantization with norm caching
- Pluggable embedding providers:
  - Local (Python sentence-transformers)
  - Ollama
  - OpenAI
- Provider chain for fallback
- Metadata filtering on search
- GitLab CI configuration
- HexDocs integration

### Features

- `barrel_vectordb:add/4` - Add document with text embedding
- `barrel_vectordb:add_vector/5` - Add document with pre-computed vector
- `barrel_vectordb:search/3` - Text-based semantic search
- `barrel_vectordb:search_vector/3` - Vector-based search
- `barrel_vectordb:get/2` - Retrieve document by ID
- `barrel_vectordb:update/4` - Update existing document
- `barrel_vectordb:upsert/4` - Insert or update document
- `barrel_vectordb:delete/2` - Delete document
- `barrel_vectordb:peek/2` - Sample random documents
- `barrel_vectordb:count/1` - Count total documents
